package claim

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
)

func TestServiceStartMarksReadyAfterInitialReconcile(t *testing.T) {
	service, store := newClaimTestService(t)
	if err := service.ensureTokenDir(); err != nil {
		t.Fatalf("ensure token dir: %v", err)
	}
	if err := writeTestTokenFile(service.tokenDirPath(), "startup-ready.json", `{"account_id":"acct-ready","access_token":"token-ready","refresh_token":"refresh-ready"}`); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)

	waitForTestCondition(t, 2*time.Second, service.isStartupReady)

	var count int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM tokens WHERE file_name = 'startup-ready.json'`).Scan(&count); err != nil {
		t.Fatalf("count imported tokens: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected startup reconcile to import one token, got %d", count)
	}

	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := service.Stop(stopCtx); err != nil {
		t.Fatalf("stop service: %v", err)
	}
}

func TestHealthReportsServiceUnavailableUntilStartupReady(t *testing.T) {
	service, _ := newClaimTestService(t)
	e := echo.New()

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()
	if err := service.getHealth(e.NewContext(request, recorder)); err != nil {
		t.Fatalf("get health before ready: %v", err)
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected service unavailable before ready, got %d", recorder.Code)
	}

	service.markStartupReady(map[string]int{"total": 0})

	request = httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder = httptest.NewRecorder()
	if err := service.getHealth(e.NewContext(request, recorder)); err != nil {
		t.Fatalf("get health after ready: %v", err)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected ok after ready, got %d", recorder.Code)
	}
}

func TestMarkTokenBannedRemovesFileAndRetriesOnFailure(t *testing.T) {
	service, store := newClaimTestService(t)
	if err := service.ensureTokenDir(); err != nil {
		t.Fatalf("ensure token dir: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)
	waitForTestCondition(t, 2*time.Second, service.isStartupReady)

	fileName := "banned-retry.json"
	filePath := service.tokenFileAbsolutePath(fileName)
	if err := os.WriteFile(filePath, []byte(`{"account_id":"acct-ban","access_token":"token-ban","refresh_token":"refresh-ban"}`), 0o644); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	result, err := store.DB().Exec(`
		INSERT INTO tokens (
			file_name,
			file_path,
			file_hash,
			encoding,
			content_json,
			account_id,
			access_token_hash,
			is_active,
			is_cleaned,
			is_enabled,
			is_banned,
			is_available,
			claim_count,
			max_claims,
			created_at_ts,
			updated_at_ts,
			last_seen_at_ts
		) VALUES (?, ?, ?, 'utf-8', ?, ?, ?, 1, 0, 1, 0, 1, 0, 1, 1000, 1000, 1000)
	`, fileName, service.tokenFileRelativePath(fileName), "file-hash-"+fileName, `{"account_id":"acct-ban","access_token":"token-ban","refresh_token":"refresh-ban"}`, "acct-ban", "hash-ban")
	if err != nil {
		t.Fatalf("insert token row: %v", err)
	}
	tokenID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted token id: %v", err)
	}

	var removeAttempts atomic.Int32
	service.removeTokenFile = func(path string) error {
		if removeAttempts.Add(1) == 1 {
			return errors.New("file busy")
		}
		return os.Remove(path)
	}

	if err := service.markTokenBanned(context.Background(), tokenID, "upstream_401"); err != nil {
		t.Fatalf("mark token banned: %v", err)
	}

	waitForTestCondition(t, 3*time.Second, func() bool {
		_, statErr := os.Stat(filePath)
		return os.IsNotExist(statErr) && removeAttempts.Load() >= 2
	})

	var (
		isBanned        int
		isEnabled       int
		isAvailable     int
		isActive        int
		lastProbeStatus string
	)
	if err := store.DB().QueryRow(`
		SELECT is_banned, is_enabled, is_available, is_active, last_probe_status
		FROM tokens
		WHERE id = ?
	`, tokenID).Scan(&isBanned, &isEnabled, &isAvailable, &isActive, &lastProbeStatus); err != nil {
		t.Fatalf("load banned token state: %v", err)
	}
	if isBanned != 1 || isEnabled != 0 || isAvailable != 0 || isActive != 0 {
		t.Fatalf("unexpected banned token state: banned=%d enabled=%d available=%d active=%d", isBanned, isEnabled, isAvailable, isActive)
	}
	if lastProbeStatus != "banned_401" {
		t.Fatalf("unexpected last probe status: %q", lastProbeStatus)
	}

	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := service.Stop(stopCtx); err != nil {
		t.Fatalf("stop service: %v", err)
	}
}

func waitForTestCondition(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
