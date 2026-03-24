package claim

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
	"token-atlas/internal/database"
)

func TestDashboardSummaryIncludesLeaderboardWindowAndRecentClaimsOmitRequestID(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4101", "alpha")
	insertTestToken(t, store, "alpha-1.json", `{"access_token":"alpha-1","refresh_token":"alpha-r-1","account_id":"acct-alpha-1"}`, 0, 1)
	insertTestToken(t, store, "alpha-2.json", `{"access_token":"alpha-2","refresh_token":"alpha-r-2","account_id":"acct-alpha-2"}`, 0, 1)

	if _, err := service.ClaimTokens(ctx, userID, nil, 1); err != nil {
		t.Fatalf("claim first token: %v", err)
	}
	if _, err := service.ClaimTokens(ctx, userID, nil, 1); err != nil {
		t.Fatalf("claim second token: %v", err)
	}

	payload, err := service.GetDashboardSummaryWithOptions(ctx, userID, dashboardSummaryOptions{
		Window:                 "7d",
		Bucket:                 "1h",
		LeaderboardWindow:      "24h",
		LeaderboardLimit:       10,
		RecentLimit:            10,
		ContributorLimit:       10,
		RecentContributorLimit: 10,
	})
	if err != nil {
		t.Fatalf("get dashboard summary: %v", err)
	}

	leaderboard, ok := payload["leaderboard"].(map[string]any)
	if !ok {
		t.Fatalf("leaderboard payload has unexpected type: %#v", payload["leaderboard"])
	}
	if got := leaderboard["window"]; got != 24*3600 {
		t.Fatalf("expected leaderboard window 86400, got %#v", got)
	}

	recent, ok := payload["recent"].(map[string]any)
	if !ok {
		t.Fatalf("recent payload has unexpected type: %#v", payload["recent"])
	}
	items, ok := recent["items"].([]map[string]any)
	if !ok {
		t.Fatalf("recent items has unexpected type: %#v", recent["items"])
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 recent claim groups, got %d", len(items))
	}
	if _, exists := items[0]["request_id"]; exists {
		t.Fatalf("recent claim item should not expose request_id: %#v", items[0])
	}
}

func TestAdminMePolicyOmitsNestedSystem(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4201", "admin-user")
	payload, err := service.GetAdminMe(ctx, &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "4201",
			Username:   "admin-user",
			Name:       "Admin User",
			TrustLevel: 4,
			IsAdmin:    true,
		},
		IsAdmin: true,
	})
	if err != nil {
		t.Fatalf("get admin me: %v", err)
	}

	policy, ok := payload["policy"].(map[string]any)
	if !ok {
		t.Fatalf("policy payload has unexpected type: %#v", payload["policy"])
	}
	if _, exists := policy["system"]; exists {
		t.Fatalf("admin/me policy should not contain nested system payload: %#v", policy)
	}
	if _, ok := payload["system"].(map[string]any); !ok {
		t.Fatalf("admin/me system payload missing: %#v", payload["system"])
	}
}

func TestSystemStatusUpdatedAtStableUntilGlobalRefresh(t *testing.T) {
	service, _ := newClaimTestService(t)
	ctx := context.Background()

	service.systemIndexMu.Lock()
	service.systemIndexUpdatedAt = "2000-01-01T00:00:00Z"
	service.systemIndexMu.Unlock()

	first, err := service.getSystemStatus(ctx)
	if err != nil {
		t.Fatalf("get first system status: %v", err)
	}
	second, err := service.getSystemStatus(ctx)
	if err != nil {
		t.Fatalf("get second system status: %v", err)
	}

	firstIndex := first["index"].(map[string]any)
	secondIndex := second["index"].(map[string]any)
	if firstIndex["updated_at"] != secondIndex["updated_at"] {
		t.Fatalf("updated_at should remain stable between reads: %v vs %v", firstIndex["updated_at"], secondIndex["updated_at"])
	}

	service.invalidateAllRuntimeCache(nil, true)

	third, err := service.getSystemStatus(ctx)
	if err != nil {
		t.Fatalf("get refreshed system status: %v", err)
	}
	thirdIndex := third["index"].(map[string]any)
	if thirdIndex["updated_at"] == firstIndex["updated_at"] {
		t.Fatalf("updated_at should change after global refresh: %v", thirdIndex["updated_at"])
	}
}

func TestListClaimFilesReturnsCorruptClaimDataError(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4301", "claim-user")
	tokenID := insertTestToken(t, store, "corrupt-claim.json", `{"access_token":"ok","refresh_token":"ok-r","account_id":"acct-ok"}`, 0, 1)
	insertStoredClaim(t, store, tokenID, userID, "{")

	_, err := service.ListClaimFiles(ctx, userID)
	if !errors.Is(err, errCorruptClaimData) {
		t.Fatalf("expected corrupt claim data error, got %v", err)
	}
}

func TestGetClaimedTokenForDownloadReturnsCorruptClaimDataError(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4302", "download-user")
	tokenID := insertTestToken(t, store, "corrupt-download.json", `{"access_token":"ok-2","refresh_token":"ok-r-2","account_id":"acct-ok-2"}`, 0, 1)
	insertStoredClaim(t, store, tokenID, userID, "{")

	_, err := service.GetClaimedTokenForDownload(ctx, tokenID, userID)
	if !errors.Is(err, errCorruptClaimData) {
		t.Fatalf("expected corrupt claim data error, got %v", err)
	}
}

func TestMapDatabaseBusyErrorReturnsRetryAfterHeader(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := mapDatabaseBusyError(c, errors.New("database is locked"))
	var httpErr *echo.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected echo HTTP error, got %T", err)
	}
	if httpErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", httpErr.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("expected Retry-After header 3, got %q", got)
	}
	if httpErr.Message != "数据库正忙，请稍后重试。" {
		t.Fatalf("unexpected db busy message: %#v", httpErr.Message)
	}
}

func TestAPIClaimRoutesUseQueuedRequestLifecycle(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4401", "api-queue-user")
	apiKeyID := insertTestAPIKey(t, store, userID, "api-queue-key")
	insertTestToken(t, store, "api-queued.json", `{"access_token":"api-queued","refresh_token":"api-queued-r","account_id":"acct-api-queued"}`, 0, 1)

	e := echo.New()
	postRequest := httptest.NewRequest(http.MethodPost, "/api/claim", strings.NewReader(`{"count":1}`))
	postRequest.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	postRecorder := httptest.NewRecorder()
	postContext := e.NewContext(postRequest, postRecorder)
	postContext.Set("auth.api_key_record", &auth.APIKeyRecord{UserID: userID, APIKeyID: apiKeyID})

	if err := service.claimByAPIKey(postContext); err != nil {
		t.Fatalf("post /api/claim: %v", err)
	}
	if postRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected post status: %d body=%s", postRecorder.Code, postRecorder.Body.String())
	}

	var accepted claimResult
	if err := json.Unmarshal(postRecorder.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("decode queued api claim result: %v", err)
	}
	if accepted.RequestID == "" {
		t.Fatalf("expected request id in queued api response: %+v", accepted)
	}
	if !accepted.Queued || accepted.QueueStatus != queueStatusQueuedWaiting {
		t.Fatalf("expected queued_waiting api ack, got %+v", accepted)
	}
	if accepted.Granted != 0 || len(accepted.Items) != 0 {
		t.Fatalf("api claim ack should not synchronously grant items: %+v", accepted)
	}

	getQueuedRequest := httptest.NewRequest(http.MethodGet, "/api/claims/"+accepted.RequestID, nil).WithContext(ctx)
	getQueuedRecorder := httptest.NewRecorder()
	getQueuedContext := e.NewContext(getQueuedRequest, getQueuedRecorder)
	getQueuedContext.SetParamNames("request_id")
	getQueuedContext.SetParamValues(accepted.RequestID)
	getQueuedContext.Set("auth.api_key_record", &auth.APIKeyRecord{UserID: userID, APIKeyID: apiKeyID})

	if err := service.getClaimRequestByAPIKey(getQueuedContext); err != nil {
		t.Fatalf("get queued api claim request: %v", err)
	}

	var queued claimResult
	if err := json.Unmarshal(getQueuedRecorder.Body.Bytes(), &queued); err != nil {
		t.Fatalf("decode queued api claim request: %v", err)
	}
	if !queued.Queued || queued.QueuePosition != 1 || queued.QueueRemaining != 1 {
		t.Fatalf("expected active queued request snapshot, got %+v", queued)
	}

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue: %v", err)
	}

	getDoneRequest := httptest.NewRequest(http.MethodGet, "/api/claims/"+accepted.RequestID, nil).WithContext(ctx)
	getDoneRecorder := httptest.NewRecorder()
	getDoneContext := e.NewContext(getDoneRequest, getDoneRecorder)
	getDoneContext.SetParamNames("request_id")
	getDoneContext.SetParamValues(accepted.RequestID)
	getDoneContext.Set("auth.api_key_record", &auth.APIKeyRecord{UserID: userID, APIKeyID: apiKeyID})

	if err := service.getClaimRequestByAPIKey(getDoneContext); err != nil {
		t.Fatalf("get completed api claim request: %v", err)
	}
	if getDoneRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected get status: %d body=%s", getDoneRecorder.Code, getDoneRecorder.Body.String())
	}

	var completed claimResult
	if err := json.Unmarshal(getDoneRecorder.Body.Bytes(), &completed); err != nil {
		t.Fatalf("decode completed api claim request: %v", err)
	}
	if completed.Queued {
		t.Fatalf("completed api request should not remain queued: %+v", completed)
	}
	if completed.QueueStatus != queueStatusSucceeded {
		t.Fatalf("expected succeeded api request status, got %+v", completed)
	}
	if completed.Granted != 1 || len(completed.Items) != 1 {
		t.Fatalf("expected one completed api claim item, got %+v", completed)
	}
	if !strings.Contains(completed.Items[0].DownloadURL, "/api/download/") {
		t.Fatalf("expected download url in completed api request payload, got %+v", completed.Items[0])
	}
}

func TestRunWithDatabaseBusyRetryRetriesBusyErrors(t *testing.T) {
	attempts := 0

	value, err := runWithDatabaseBusyRetry(context.Background(), func() (int, error) {
		attempts++
		if attempts < 3 {
			return 0, errors.New("database is locked (517)")
		}
		return 7, nil
	})
	if err != nil {
		t.Fatalf("runWithDatabaseBusyRetry returned error: %v", err)
	}
	if value != 7 {
		t.Fatalf("unexpected retry result: %d", value)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRunWithDatabaseBusyRetryStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	_, err := runWithDatabaseBusyRetry(ctx, func() (int, error) {
		return 0, errors.New("database is locked")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatalf("canceled retry should stop promptly")
	}
}

func TestCleanupExhaustedTokensIgnoresInactiveTokens(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	if err := os.MkdirAll("token", 0o755); err != nil {
		t.Fatalf("create token directory: %v", err)
	}

	activePath := filepath.Join("token", "cleanup-active.json")
	inactivePath := filepath.Join("token", "cleanup-inactive.json")
	if err := os.WriteFile(activePath, []byte(`{"ok":true}`), 0o644); err != nil {
		t.Fatalf("write active token file: %v", err)
	}
	if err := os.WriteFile(inactivePath, []byte(`{"ok":true}`), 0o644); err != nil {
		t.Fatalf("write inactive token file: %v", err)
	}

	insertCleanupToken(t, store, "cleanup-active.json", "token/cleanup-active.json", 1, 1, 1, 100)
	insertCleanupToken(t, store, "cleanup-inactive.json", "token/cleanup-inactive.json", 0, 1, 1, 50)

	result, err := service.CleanupExhaustedTokens(ctx, "files_only")
	if err != nil {
		t.Fatalf("cleanup exhausted tokens: %v", err)
	}

	if got := result["matched"]; got != 1 {
		t.Fatalf("expected only active exhausted token to match, got %#v", got)
	}
	if _, err := os.Stat(activePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected active exhausted token file to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(inactivePath); err != nil {
		t.Fatalf("inactive token file should remain, stat err=%v", err)
	}
}

func insertStoredClaim(t *testing.T, store *database.Store, tokenID int64, userID int64, contentJSON string) {
	t.Helper()

	if _, err := store.DB().Exec(`
		INSERT INTO token_claims (
			token_id,
			user_id,
			api_key_id,
			claimed_at_ts,
			is_hidden,
			claim_file_name,
			claim_file_path,
			claim_encoding,
			claim_content_json,
			request_id
		) VALUES (?, ?, NULL, 1000, 0, ?, ?, 'utf-8', ?, ?)
	`, tokenID, userID, "stored-claim.json", "./token/stored-claim.json", contentJSON, "req-stored"); err != nil {
		t.Fatalf("insert stored claim: %v", err)
	}
}

func insertCleanupToken(t *testing.T, store *database.Store, fileName string, filePath string, isActive int, claimCount int, maxClaims int, updatedAtTS int64) {
	t.Helper()

	if _, err := store.DB().Exec(`
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
		) VALUES (?, ?, ?, 'utf-8', ?, ?, ?, ?, 0, 1, 0, 0, ?, ?, 1000, ?, ?)
	`, fileName, filePath, "file-hash-"+fileName, `{"access_token":"`+fileName+`","refresh_token":"r-`+fileName+`","account_id":"acct-`+fileName+`"}`, "acct-"+fileName, "hash-"+fileName, isActive, claimCount, maxClaims, updatedAtTS, updatedAtTS); err != nil {
		t.Fatalf("insert cleanup token: %v", err)
	}
}
