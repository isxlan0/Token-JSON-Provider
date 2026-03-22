package claim

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadTokenFilePayloadTreatsTruncatedJSONAsRetryable(t *testing.T) {
	_, _, _, _, err := loadTokenFilePayload([]byte(`{"account_id":"acct-retry","access_token":"token-retry"`))
	if err == nil {
		t.Fatalf("expected truncated json to fail")
	}
	if !isRetriableTokenImportError(err) {
		t.Fatalf("expected retryable token import error, got %v", err)
	}
}

func TestTokenImportLoopRetriesUntilJSONWriteCompletes(t *testing.T) {
	service, store := newClaimTestService(t)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	if err := service.ensureTokenDir(); err != nil {
		t.Fatalf("ensure token dir: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go service.tokenImportLoop(ctx)

	fileName := "partial-watch.json"
	filePath := filepath.Join(service.tokenDirPath(), fileName)
	partial := `{"account_id":"acct-retry","access_token":"token-retry"`
	if err := os.WriteFile(filePath, []byte(partial), 0o644); err != nil {
		t.Fatalf("write partial token file: %v", err)
	}

	service.enqueueTokenImport(fileName, "watch")

	time.Sleep(125 * time.Millisecond)

	full := `{"account_id":"acct-retry","access_token":"token-retry","refresh_token":"refresh-retry"}`
	if err := os.WriteFile(filePath, []byte(full), 0o644); err != nil {
		t.Fatalf("write full token file: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		var (
			count     int
			accountID string
		)
		if err := store.DB().QueryRow(`SELECT COUNT(*), COALESCE(MIN(account_id), '') FROM tokens WHERE file_name = ?`, fileName).Scan(&count, &accountID); err != nil {
			t.Fatalf("query imported token: %v", err)
		}
		if count == 1 {
			if accountID != "acct-retry" {
				t.Fatalf("unexpected imported account id: %q", accountID)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("token file was not imported after retries")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
