package claim

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStartupReconcileTokenFilesLogsInfoOnCancel(t *testing.T) {
	service, _ := newClaimTestService(t)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	handler := &memoryLogHandler{}
	service.logger = slog.New(handler)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	service.startupReconcileTokenFiles(ctx)

	if handler.containsLevel(slog.LevelError) {
		t.Fatalf("expected no error logs on cancelled reconcile, got %+v", handler.entries)
	}
	if !handler.containsMessage("startup reconcile token files cancelled") {
		t.Fatalf("expected cancel info log, got %+v", handler.entries)
	}
}

func TestReconcileTokenFilesReturnsCancelBeforeScanning(t *testing.T) {
	service, store := newClaimTestService(t)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	if err := service.ensureTokenDir(); err != nil {
		t.Fatalf("ensure token dir: %v", err)
	}
	if err := writeTestTokenFile(service.tokenDirPath(), "first.json", `{"account_id":"acct-1","access_token":"token-1","refresh_token":"refresh-1"}`); err != nil {
		t.Fatalf("write first token: %v", err)
	}
	if err := writeTestTokenFile(service.tokenDirPath(), "second.json", `{"account_id":"acct-2","access_token":"token-2","refresh_token":"refresh-2"}`); err != nil {
		t.Fatalf("write second token: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.reconcileTokenFiles(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}

	var count int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM tokens`).Scan(&count); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected cancelled reconcile to skip imports, got %d tokens", count)
	}
}

func TestEnqueueTokenImportWaitsForChannelCapacity(t *testing.T) {
	service, _ := newClaimTestService(t)
	service.tokenImportCh = make(chan tokenImportRequest, 1)
	service.tokenImportCh <- tokenImportRequest{fileName: "busy.json", reason: "existing"}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		service.enqueueTokenImport(ctx, "queued.json", "watch")
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("expected enqueue to block while channel is full")
	case <-time.After(100 * time.Millisecond):
	}

	<-service.tokenImportCh

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected enqueue to finish after channel capacity is available")
	}

	request := <-service.tokenImportCh
	if request.fileName != "queued.json" || request.reason != "watch" {
		t.Fatalf("unexpected queued request: %+v", request)
	}
}

func writeTestTokenFile(dir string, fileName string, content string) error {
	return os.WriteFile(filepath.Join(dir, fileName), []byte(content), 0o644)
}

type memoryLogHandler struct {
	mu      sync.Mutex
	entries []memoryLogEntry
}

type memoryLogEntry struct {
	level   slog.Level
	message string
}

func (h *memoryLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return true
}

func (h *memoryLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, memoryLogEntry{
		level:   record.Level,
		message: record.Message,
	})
	return nil
}

func (h *memoryLogHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *memoryLogHandler) WithGroup(_ string) slog.Handler {
	return h
}

func (h *memoryLogHandler) containsLevel(level slog.Level) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, entry := range h.entries {
		if entry.level == level {
			return true
		}
	}
	return false
}

func (h *memoryLogHandler) containsMessage(fragment string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, entry := range h.entries {
		if strings.Contains(entry.message, fragment) {
			return true
		}
	}
	return false
}
