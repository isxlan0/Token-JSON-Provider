package main

import (
	"context"
	"errors"
	"fmt"
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
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("close database", "error", closeErr)
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
		if startErr := e.Start(addr); startErr != nil && !errors.Is(startErr, http.ErrServerClosed) {
			errCh <- startErr
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := e.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown server: %w", err)
		}
		if err := claimService.Stop(shutdownCtx); err != nil {
			return fmt.Errorf("stop claim service: %w", err)
		}

		logger.Info("server stopped", "reason", ctx.Err())
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("start server: %w", err)
		}
		return nil
	}
}
