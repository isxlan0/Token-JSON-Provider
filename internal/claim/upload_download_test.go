package claim

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"token-atlas/internal/auth"
	proberuntime "token-atlas/internal/probe"
)

func TestQueueUploadBatchAcceptsAndPreservesExtraFields(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	userID := insertTestUser(t, store, "2001", "uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "2001",
			Username:   "uploader",
			Name:       "Uploader",
			TrustLevel: 2,
		},
	}

	rawJSON := `{"account_id":"acct-upload","access_token":"token-upload","refresh_token":"refresh-upload","extra":{"region":"hk"}}`
	files := []uploadFileInput{{
		Name:          "upload.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(rawJSON)),
	}}

	result, err := service.QueueUploadBatch(context.Background(), requestContext, files)
	if err != nil {
		t.Fatalf("queue upload batch: %v", err)
	}

	summary := result["summary"].(map[string]int)
	if summary["queued"] != 1 {
		t.Fatalf("expected initial queued summary, got %+v", summary)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		payload := service.GetUploadResults(userID)
		currentSummary := payload["summary"].(map[string]int)
		if currentSummary["accepted"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("upload worker did not accept task in time: %+v", currentSummary)
		}
		time.Sleep(20 * time.Millisecond)
	}

	var contentJSON string
	if err := store.DB().QueryRow(`SELECT content_json FROM tokens WHERE account_id = 'acct-upload'`).Scan(&contentJSON); err != nil {
		t.Fatalf("query uploaded token: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(contentJSON), &payload); err != nil {
		t.Fatalf("decode stored upload content: %v", err)
	}
	if payload["account_id"] != "acct-upload" {
		t.Fatalf("unexpected stored account_id: %#v", payload["account_id"])
	}
	extra, ok := payload["extra"].(map[string]any)
	if !ok || extra["region"] != "hk" {
		t.Fatalf("expected extra field to be preserved, got %#v", payload["extra"])
	}
}

func TestDuplicateUploadSubmissionReceivesFinalStatusFromPendingTask(t *testing.T) {
	service, store := newClaimTestService(t)
	probe := newBlockingProbe()
	service.probe = probe

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	userID := insertTestUser(t, store, "2002", "duplicate-uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "2002",
			Username:   "duplicate-uploader",
			Name:       "Duplicate Uploader",
			TrustLevel: 2,
		},
	}

	rawJSON := `{"account_id":"acct-duplicate","access_token":"token-duplicate","refresh_token":"refresh-duplicate"}`
	files := []uploadFileInput{{
		Name:          "duplicate.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(rawJSON)),
	}}

	first, err := service.QueueUploadBatch(context.Background(), requestContext, files)
	if err != nil {
		t.Fatalf("queue first upload batch: %v", err)
	}
	if got := first["summary"].(map[string]int)["queued"]; got != 1 {
		t.Fatalf("expected first batch to queue 1 item, got %+v", first["summary"])
	}

	probe.waitUntilStarted(t)

	second, err := service.QueueUploadBatch(context.Background(), requestContext, files)
	if err != nil {
		t.Fatalf("queue duplicate upload batch: %v", err)
	}
	secondItems := second["items"].([]map[string]any)
	if got := secondItems[0]["status"]; got != "processing" && got != "queued" {
		t.Fatalf("expected duplicate batch to attach to pending task, got status=%#v item=%#v", got, secondItems[0])
	}

	probe.release()

	deadline := time.Now().Add(5 * time.Second)
	for {
		payload := service.GetUploadResults(userID)
		items := payload["items"].([]map[string]any)
		if len(items) == 1 && items[0]["status"] == "accepted" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("duplicate batch did not receive final accepted status: %#v", payload)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestClaimDownloadAccessSummaryHonorsHiddenAndOtherVisibleClaims(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userOneID := insertTestUser(t, store, "3001", "first")
	userTwoID := insertTestUser(t, store, "3002", "second")

	insertTestToken(t, store, "token-visible.json", `{"access_token":"visible","refresh_token":"visible-r","account_id":"acct-visible"}`, 0, 1)
	insertTestToken(t, store, "token-hidden.json", `{"access_token":"hidden","refresh_token":"hidden-r","account_id":"acct-hidden"}`, 0, 1)

	visibleResult, err := service.ClaimTokens(ctx, userOneID, nil, 1)
	if err != nil {
		t.Fatalf("claim visible token: %v", err)
	}
	hiddenResult, err := service.ClaimTokens(ctx, userOneID, nil, 1)
	if err != nil {
		t.Fatalf("claim hidden token: %v", err)
	}

	if err := service.HideClaims(ctx, userOneID, []int64{hiddenResult.Items[0].ClaimID}); err != nil {
		t.Fatalf("hide claim: %v", err)
	}

	hiddenAccess, err := service.GetClaimDownloadAccessSummary(ctx, hiddenResult.Items[0].TokenID, userOneID)
	if err != nil {
		t.Fatalf("get hidden access summary: %v", err)
	}
	if !hiddenAccess.UserHiddenClaim || hiddenAccess.OtherVisibleClaim {
		t.Fatalf("unexpected hidden access summary: %+v", hiddenAccess)
	}
	hiddenItem, err := service.GetClaimedTokenForDownload(ctx, hiddenResult.Items[0].TokenID, userOneID)
	if err != nil {
		t.Fatalf("get hidden claimed token: %v", err)
	}
	if hiddenItem != nil {
		t.Fatalf("expected hidden claimed token to be unavailable, got %+v", hiddenItem)
	}

	otherVisibleAccess, err := service.GetClaimDownloadAccessSummary(ctx, visibleResult.Items[0].TokenID, userTwoID)
	if err != nil {
		t.Fatalf("get other visible access summary: %v", err)
	}
	if otherVisibleAccess.UserHiddenClaim || !otherVisibleAccess.OtherVisibleClaim {
		t.Fatalf("unexpected other visible access summary: %+v", otherVisibleAccess)
	}
	otherVisibleItem, err := service.GetClaimedTokenForDownload(ctx, visibleResult.Items[0].TokenID, userTwoID)
	if err != nil {
		t.Fatalf("get other user claimed token: %v", err)
	}
	if otherVisibleItem != nil {
		t.Fatalf("expected other user's claimed token to be unavailable, got %+v", otherVisibleItem)
	}
}

func pushTempWorkingDir(t *testing.T) func() {
	t.Helper()

	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	tempDir := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatalf("create temp working directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir to temp working directory: %v", err)
	}

	return func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Fatalf("restore working directory: %v", err)
		}
	}
}

type blockingProbe struct {
	started   chan struct{}
	releaseCh chan struct{}
}

func newBlockingProbe() *blockingProbe {
	return &blockingProbe{
		started:   make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
}

func (p *blockingProbe) Start() {}

func (p *blockingProbe) Stop() {
	select {
	case <-p.releaseCh:
	default:
		close(p.releaseCh)
	}
}

func (p *blockingProbe) Submit(map[string]any, float64) proberuntime.Result {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-p.releaseCh
	return proberuntime.Result{Status: "ok"}
}

func (p *blockingProbe) waitUntilStarted(t *testing.T) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("probe did not start in time")
	}
}

func (p *blockingProbe) release() {
	select {
	case <-p.releaseCh:
	default:
		close(p.releaseCh)
	}
}
