package claim

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"token-atlas/internal/config"
	"token-atlas/internal/database"
	proberuntime "token-atlas/internal/probe"
)

func TestClaimTokensDirectGrant(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "1001", "tester")
	insertTestToken(t, store, "token-a.json", `{"access_token":"token-a","refresh_token":"refresh-a","account_id":"acct-a"}`, 0, 1)

	result, err := service.ClaimTokens(ctx, userID, nil, 1)
	if err != nil {
		t.Fatalf("claim tokens: %v", err)
	}

	if result.Queued {
		t.Fatalf("expected direct grant, got queued result")
	}
	if result.Granted != 1 || len(result.Items) != 1 {
		t.Fatalf("unexpected grant result: %+v", result)
	}
	if result.Items[0].FileName != "token-a.json" {
		t.Fatalf("unexpected claimed file name: %q", result.Items[0].FileName)
	}

	var (
		claimCount  int
		isAvailable int
	)
	if err := store.DB().QueryRow(`SELECT claim_count, is_available FROM tokens WHERE file_name = 'token-a.json'`).Scan(&claimCount, &isAvailable); err != nil {
		t.Fatalf("query claimed token state: %v", err)
	}
	if claimCount != 1 || isAvailable != 0 {
		t.Fatalf("unexpected token state after claim: claim_count=%d is_available=%d", claimCount, isAvailable)
	}
}

func TestQueuedClaimCanBeFulfilledByQueuePump(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "1002", "queued-user")

	result, err := service.ClaimTokens(ctx, userID, nil, 1)
	if err != nil {
		t.Fatalf("queue empty inventory claim: %v", err)
	}
	if !result.Queued {
		t.Fatalf("expected queued result, got %+v", result)
	}

	insertTestToken(t, store, "token-b.json", `{"access_token":"token-b","refresh_token":"refresh-b","account_id":"acct-b"}`, 0, 1)

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue: %v", err)
	}

	queueStatus, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get queue status: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected queue to be drained, got %+v", queueStatus)
	}

	claims, err := service.ListClaims(ctx, userID)
	if err != nil {
		t.Fatalf("list claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("expected one queued claim to be fulfilled, got %d", len(claims))
	}
	if claims[0].FileName != "token-b.json" {
		t.Fatalf("unexpected queued claim file: %q", claims[0].FileName)
	}
}

func newClaimTestService(t *testing.T) (*Service, *database.Store) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "claim-test.db")
	store, err := database.Open(dbPath)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init test database: %v", err)
	}

	cfg := config.Config{
		Inventory: config.InventoryConfig{
			Thresholds: config.ThresholdConfig{
				Healthy:  1,
				Warning:  1,
				Critical: 1,
			},
			Limits: config.InventoryLimitConfig{
				Healthy: config.InventoryStatusLimit{
					Hourly:    30,
					MaxClaims: 1,
				},
				Warning: config.InventoryStatusLimit{
					Hourly:    20,
					MaxClaims: 2,
				},
				Critical: config.InventoryStatusLimit{
					Hourly:    15,
					MaxClaims: 3,
				},
			},
			NonHealthyMaxClaimsScope: "all_unfinished",
		},
		APIKeys: config.APIKeyConfig{
			MaxPerUser:    5,
			RatePerMinute: 60,
		},
		LinuxDO: config.LinuxDOConfig{
			MinTrustLevel: 0,
		},
		Upload: config.UploadConfig{
			MaxFilesPerRequest: 10,
			MaxFileSizeBytes:   10 * 1024,
			MaxSuccessPerHour:  20,
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	service := NewService(cfg, store, nil, nil, logger)
	service.probe = alwaysOKProbe{}
	return service, store
}

func insertTestUser(t *testing.T, store *database.Store, linuxDOUserID string, username string) int64 {
	t.Helper()

	result, err := store.DB().Exec(`
		INSERT INTO users (
			linuxdo_user_id,
			linuxdo_username,
			linuxdo_name,
			trust_level,
			created_at_ts,
			last_login_at_ts
		) VALUES (?, ?, ?, 2, 1000, 1000)
	`, linuxDOUserID, username, username)
	if err != nil {
		t.Fatalf("insert test user: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted user id: %v", err)
	}
	return userID
}

func insertTestToken(t *testing.T, store *database.Store, fileName string, contentJSON string, claimCount int, maxClaims int) int64 {
	t.Helper()

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
		) VALUES (?, ?, ?, 'utf-8', ?, ?, ?, 1, 0, 1, 0, ?, ?, ?, 1000, 1000, 1000)
	`, fileName, "./token/"+fileName, "file-hash-"+fileName, contentJSON, "acct-"+fileName, "hash-"+fileName, boolToInt(claimCount < maxClaims), claimCount, maxClaims)
	if err != nil {
		t.Fatalf("insert test token: %v", err)
	}

	tokenID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted token id: %v", err)
	}
	return tokenID
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

type alwaysOKProbe struct{}

func (alwaysOKProbe) Start() {}

func (alwaysOKProbe) Stop() {}

func (alwaysOKProbe) Submit(map[string]any, float64) proberuntime.Result {
	return proberuntime.Result{Status: "ok"}
}
