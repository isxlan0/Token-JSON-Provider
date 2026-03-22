package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"token-atlas/internal/auth"
	"token-atlas/internal/claim"
	"token-atlas/internal/config"
	dbstore "token-atlas/internal/database"
	"token-atlas/internal/httpapi"
	"token-atlas/internal/runtimecache"
)

const (
	httpShutdownTimeout = 15 * time.Second
	workerStopTimeout   = 10 * time.Second
)

type shutdownServer interface {
	Shutdown(ctx context.Context) error
	Close() error
}

type claimWorkerStopper interface {
	Stop(ctx context.Context) error
}

type closeFunc func() error

func (fn closeFunc) Close() error {
	return fn()
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	runtimeRoot, err := prepareRuntimeRoot()
	if err != nil {
		return fmt.Errorf("prepare runtime root: %w", err)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	store, err := dbstore.Open(cfg.Database.Path)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	storeClosed := false
	closeStore := func() error {
		if storeClosed {
			return nil
		}
		storeClosed = true
		return store.Close()
	}
	defer func() {
		if !storeClosed {
			if closeErr := closeStore(); closeErr != nil {
				logger.Error("close database", "error", closeErr)
			}
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := store.Init(ctx); err != nil {
		return fmt.Errorf("init database: %w", err)
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(middleware.Recover())
	httpapi.InstallErrorHandler(e)

	appCache := runtimecache.New(ctx, cfg.Cache, logger)

	authService := auth.NewService(cfg, store, nil, appCache)
	authService.RegisterRoutes(e)

	claimService := claim.NewService(cfg, store, authService, appCache, logger)
	claimService.RegisterRoutes(e)
	claimService.Start(ctx)

	staticDir := filepath.Join(runtimeRoot, "static")
	serveStaticPage := func(route string, filePath string) {
		e.GET(route, func(c echo.Context) error {
			c.Response().Header().Set(echo.HeaderCacheControl, "no-store")
			return c.File(filePath)
		})
	}

	serveStaticPage("/", filepath.Join(staticDir, "index.html"))
	serveStaticPage("/index.html", filepath.Join(staticDir, "index.html"))
	serveStaticPage("/admin", filepath.Join(staticDir, "admin.html"))
	serveStaticPage("/admin/", filepath.Join(staticDir, "admin.html"))
	serveStaticPage("/admin.html", filepath.Join(staticDir, "admin.html"))
	e.Static("/assets", staticDir)

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Server.Port)
	errCh := make(chan error, 1)

	go func() {
		logger.Info("starting go server", "addr", addr, "db_path", cfg.Database.Path, "runtime_root", runtimeRoot)
		errCh <- e.Start(addr)
	}()

	select {
	case <-ctx.Done():
		shutdownErr := shutdownRuntime(logger, e, claimService, closeFunc(closeStore), true)
		if shutdownErr != nil {
			return shutdownErr
		}

		logger.Info("server stopped", "reason", ctx.Err())
		return nil
	case err := <-errCh:
		cleanupErr := shutdownRuntime(logger, nil, claimService, closeFunc(closeStore), false)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.Join(fmt.Errorf("start server: %w", err), cleanupErr)
		}
		return cleanupErr
	}
}

func shutdownRuntime(logger *slog.Logger, server shutdownServer, workers claimWorkerStopper, store io.Closer, stopHTTP bool) error {
	if logger == nil {
		logger = slog.Default()
	}

	var errs []error
	if stopHTTP && server != nil {
		httpCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		shutdownErr := server.Shutdown(httpCtx)
		cancel()
		if shutdownErr != nil {
			errs = append(errs, fmt.Errorf("shutdown server: %w", shutdownErr))
			if closeErr := server.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
				errs = append(errs, fmt.Errorf("force close server: %w", closeErr))
			}
		}
	}

	if workers != nil {
		workerCtx, cancel := context.WithTimeout(context.Background(), workerStopTimeout)
		stopErr := workers.Stop(workerCtx)
		cancel()
		if stopErr != nil {
			errs = append(errs, fmt.Errorf("stop claim service: %w", stopErr))
		}
	}

	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			errs = append(errs, fmt.Errorf("close database: %w", closeErr))
		}
	}

	return errors.Join(errs...)
}
