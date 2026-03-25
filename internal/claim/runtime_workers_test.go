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

func TestStartupSyncTokenFileNamesEnqueuesMissingAndReactivatesInactive(t *testing.T) {
	service, store := newClaimTestService(t)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	if err := service.ensureTokenDir(); err != nil {
		t.Fatalf("ensure token dir: %v", err)
	}
	if err := writeTestTokenFile(service.tokenDirPath(), "present.json", `{"account_id":"acct-present","access_token":"token-present","refresh_token":"refresh-present"}`); err != nil {
		t.Fatalf("write present token: %v", err)
	}
	if err := writeTestTokenFile(service.tokenDirPath(), "reactivate.json", `{"account_id":"acct-reactivate","access_token":"token-reactivate","refresh_token":"refresh-reactivate"}`); err != nil {
		t.Fatalf("write reactivate token: %v", err)
	}
	if err := writeTestTokenFile(service.tokenDirPath(), "new.json", `{"account_id":"acct-new","access_token":"token-new","refresh_token":"refresh-new"}`); err != nil {
		t.Fatalf("write new token: %v", err)
	}

	insertTestToken(t, store, "present.json", `{"access_token":"token-present","refresh_token":"refresh-present","account_id":"acct-present"}`, 0, 1)
	reactivateID := insertTestToken(t, store, "reactivate.json", `{"access_token":"token-reactivate-old","refresh_token":"refresh-reactivate-old","account_id":"acct-reactivate-old"}`, 0, 1)
	missingID := insertTestToken(t, store, "missing.json", `{"access_token":"token-missing","refresh_token":"refresh-missing","account_id":"acct-missing"}`, 0, 1)

	if _, err := store.DB().Exec(`UPDATE tokens SET is_active = 0, is_available = 0 WHERE id = ?`, reactivateID); err != nil {
		t.Fatalf("deactivate reactivate token: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.setLifecycleContext(ctx)
	go service.tokenImportLoop(ctx)

	summary, err := service.startupSyncTokenFileNames(ctx)
	if err != nil {
		t.Fatalf("startup sync token file names: %v", err)
	}
	if summary["disk_count"] != 3 || summary["db_count"] != 3 {
		t.Fatalf("unexpected startup sync counts: %+v", summary)
	}
	if summary["disk_only"] != 1 || summary["reactivate_needed"] != 1 {
		t.Fatalf("unexpected startup sync import summary: %+v", summary)
	}
	if summary["db_only_active"] != 1 || summary["deactivated"] != 1 {
		t.Fatalf("unexpected startup sync deactivate summary: %+v", summary)
	}
	if summary["enqueued_imports"] != 2 {
		t.Fatalf("unexpected startup sync enqueue summary: %+v", summary)
	}

	waitForTestCondition(t, 3*time.Second, func() bool {
		var (
			newCount           int
			reactivateIsActive int
			missingIsActive    int
		)
		if err := store.DB().QueryRow(`SELECT COUNT(*) FROM tokens WHERE file_name = 'new.json'`).Scan(&newCount); err != nil {
			t.Fatalf("count new token: %v", err)
		}
		if err := store.DB().QueryRow(`SELECT is_active FROM tokens WHERE id = ?`, reactivateID).Scan(&reactivateIsActive); err != nil {
			t.Fatalf("load reactivate token state: %v", err)
		}
		if err := store.DB().QueryRow(`SELECT is_active FROM tokens WHERE id = ?`, missingID).Scan(&missingIsActive); err != nil {
			t.Fatalf("load missing token state: %v", err)
		}
		return newCount == 1 && reactivateIsActive == 1 && missingIsActive == 0
	})
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
