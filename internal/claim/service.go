package claim

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
	"token-atlas/internal/config"
	"token-atlas/internal/database"
	proberuntime "token-atlas/internal/probe"
	"token-atlas/internal/runtimecache"
)

const (
	queuePumpInterval                = 500 * time.Millisecond
	queueAdvanceRowTimeout           = 1500 * time.Millisecond
	queueAdvanceRetryDelay           = 1 * time.Second
	queueAdvanceMaxProbeSubmits      = 4
	queueAdvanceMaxGrantsPerTick     = 4
	queueAdvanceMaxResolvedHeadTurns = 8
	queueNotifyBatchWindow           = 100 * time.Millisecond
	queueNotifyBatchMaxUsers         = 64
	directClaimSyncMaxCount          = queueAdvanceMaxGrantsPerTick
)

var errRetryAllocation = errors.New("retry allocation")

var dbWriteLocks sync.Map

type sqlQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type Service struct {
	cfg    config.Config
	store  *database.Store
	auth   *auth.Service
	logger *slog.Logger
	cache  *runtimecache.AppCache
	wakeCh chan struct{}

	// probe is kept as a test override; production paths use dedicated claim/upload probes.
	probe       claimProbe
	claimProbe  claimProbe
	uploadProbe claimProbe

	startOnce sync.Once
	advanceMu sync.Mutex
	workerWG  sync.WaitGroup

	lifecycleMu  sync.RWMutex
	lifecycleCtx context.Context

	systemIndexMu        sync.RWMutex
	systemIndexUpdatedAt string

	startupMu            sync.RWMutex
	startupReady         bool
	startupInProgress    bool
	startupAttempts      int
	startupLastError     string
	startupLastStartedAt string
	startupReadyAt       string
	startupLastSummary   map[string]int
	startupReadyCh       chan struct{}
	startupReadyOnce     sync.Once

	queueEvents           *queueStatusEventBroker
	adminQueueEvents      *queueStatusEventBroker
	claimEvents           *claimRealtimeEventBroker
	uploadEvents          *uploadResultsEventBroker
	adminQueueActivityMu  sync.Mutex
	adminQueueActivity    []adminQueueActivityEntry
	adminQueueActivitySeq int64
	hideClaimsCh          chan hideClaimsTask
	uploadTaskCh          chan uploadTask
	uploadMu              sync.Mutex
	uploadSnapshots       map[int64]uploadSnapshot
	uploadPending         map[string]*uploadPendingRecord
	uploadQueued          int
	queueNotifyCh         chan int64

	tokenImportCh       chan tokenImportRequest
	tokenImportDoneCh   chan tokenImportResult
	tokenImportMu       sync.Mutex
	tokenImportPending  map[string]int
	tokenInternalWrites map[string]time.Time

	tokenDeleteMu      sync.Mutex
	tokenDeletePending map[string]struct{}
	removeTokenFile    func(string) error
	promoteTokenFile   func(string, string) error
}

type claimProbe interface {
	Start()
	Stop()
	Submit(tokenContent map[string]any, waitTimeoutSec float64) proberuntime.Result
}

type inventoryPolicy struct {
	Status                   string
	Unclaimed                int
	Thresholds               map[string]int
	HourlyLimit              int
	MaxClaims                int
	NonHealthyMaxClaimsScope string
}

type inventoryRuntimeState struct {
	Status      string
	Total       int
	Available   int
	Unclaimed   int
	MaxClaims   int
	UpdatedAtTS int64
}

type claimStatsRuntimeState struct {
	ClaimedTotal  int
	ClaimedUnique int
	UpdatedAtTS   int64
}

type userClaimStatsRuntimeState struct {
	UserID                 int64
	ClaimedTotal           int
	ClaimedUnique          int
	ExclusiveClaimedUnique int
	UpdatedAtTS            int64
}

type quotaUsage struct {
	Used      int `json:"used"`
	Limit     int `json:"limit"`
	Remaining int `json:"remaining"`
}

type claimResponseItem struct {
	ClaimID     int64  `json:"claim_id"`
	TokenID     int64  `json:"token_id"`
	FileName    string `json:"file_name"`
	FilePath    string `json:"file_path"`
	Encoding    string `json:"encoding"`
	Content     any    `json:"content"`
	DownloadURL string `json:"download_url,omitempty"`
}

type claimResult struct {
	RequestID      string              `json:"request_id"`
	Items          []claimResponseItem `json:"items"`
	Requested      int                 `json:"requested"`
	Granted        int                 `json:"granted"`
	Queued         bool                `json:"queued"`
	QueueStatus    string              `json:"queue_status,omitempty"`
	QueueID        int64               `json:"queue_id,omitempty"`
	QueuePosition  int                 `json:"queue_position,omitempty"`
	QueueTotal     int                 `json:"queue_total,omitempty"`
	QueueRemaining int                 `json:"queue_remaining,omitempty"`
	BlockReason    string              `json:"block_reason,omitempty"`
	LastProgressAt string              `json:"last_progress_at,omitempty"`
	NextRetryAt    string              `json:"next_retry_at,omitempty"`
	Quota          quotaUsage          `json:"quota"`
	TerminalStatus string              `json:"-"`
	TerminalReason string              `json:"-"`
}

func claimResultTerminalStatus(result *claimResult) string {
	if result == nil {
		return claimStatusFailed
	}
	if status := strings.ToLower(strings.TrimSpace(result.TerminalStatus)); isClaimTerminalStatus(status) {
		return status
	}
	if result.Granted <= 0 {
		return claimStatusFailed
	}
	if result.Granted < result.Requested {
		return claimStatusPartial
	}
	return claimStatusSucceeded
}

func claimResultTerminalReason(result *claimResult) string {
	if result == nil {
		return ""
	}
	if reason := strings.TrimSpace(result.TerminalReason); reason != "" {
		return reason
	}
	if status := claimResultTerminalStatus(result); isClaimTerminalStatus(status) {
		return strings.TrimSpace(result.BlockReason)
	}
	return ""
}

type providerPayload struct {
	UserID   string `json:"user_id,omitempty"`
	Username string `json:"username,omitempty"`
	Name     string `json:"name,omitempty"`
}

type claimHistoryItem struct {
	ClaimID     int64            `json:"claim_id"`
	TokenID     int64            `json:"token_id"`
	RequestID   string           `json:"request_id"`
	FileName    string           `json:"file_name"`
	FilePath    string           `json:"file_path"`
	Encoding    string           `json:"encoding"`
	ClaimedAt   string           `json:"claimed_at"`
	Content     any              `json:"content"`
	Provider    *providerPayload `json:"provider,omitempty"`
	DownloadURL string           `json:"download_url,omitempty"`
}

type queueStatusPayload struct {
	Queued                bool   `json:"queued"`
	Status                string `json:"status,omitempty"`
	QueueID               int64  `json:"queue_id,omitempty"`
	Position              int    `json:"position,omitempty"`
	Ahead                 int    `json:"ahead,omitempty"`
	Requested             int    `json:"requested,omitempty"`
	Remaining             int    `json:"remaining,omitempty"`
	BlockReason           string `json:"block_reason,omitempty"`
	LastProgressAt        string `json:"last_progress_at,omitempty"`
	NextRetryAt           string `json:"next_retry_at,omitempty"`
	FrontBlocked          bool   `json:"front_blocked,omitempty"`
	FrontBlockReason      string `json:"front_block_reason,omitempty"`
	FrontNextRetryAt      string `json:"front_next_retry_at,omitempty"`
	EnqueuedAt            string `json:"enqueued_at,omitempty"`
	RequestID             string `json:"request_id,omitempty"`
	TotalQueued           int    `json:"total_queued"`
	AvailableTokens       int    `json:"available_tokens"`
	ClaimableNow          *int   `json:"claimable_now"`
	ClaimableNowState     string `json:"claimable_now_state,omitempty"`
	ClaimableNowUpdatedAt string `json:"claimable_now_updated_at,omitempty"`
	DataSource            string `json:"data_source,omitempty"`
	GeneratedAt           string `json:"generated_at,omitempty"`
	StaleAt               string `json:"stale_at,omitempty"`
	Degraded              bool   `json:"degraded,omitempty"`
	DegradedReason        string `json:"degraded_reason,omitempty"`
}

type claimableNowPayload struct {
	ClaimableNow          *int   `json:"claimable_now"`
	ClaimableNowState     string `json:"claimable_now_state,omitempty"`
	ClaimableNowUpdatedAt string `json:"claimable_now_updated_at,omitempty"`
	DataSource            string `json:"data_source,omitempty"`
	GeneratedAt           string `json:"generated_at,omitempty"`
	StaleAt               string `json:"stale_at,omitempty"`
	Degraded              bool   `json:"degraded,omitempty"`
	DegradedReason        string `json:"degraded_reason,omitempty"`
}

type storedClaimableNowSnapshot struct {
	ClaimableNow int    `json:"claimable_now"`
	GeneratedAt  string `json:"generated_at,omitempty"`
}

type userQueueEntry struct {
	ID                int64
	UserID            int64
	APIKeyID          sql.NullInt64
	Requested         int
	Remaining         int
	QueueRank         int
	EnqueuedAtTS      int64
	RequestID         string
	Status            string
	OriginSessionID   sql.NullString
	OriginTabID       sql.NullString
	BlockReason       sql.NullString
	NextRetryAtTS     sql.NullInt64
	LastProgressAtTS  sql.NullInt64
	TerminalAtTS      sql.NullInt64
	CancelReason      sql.NullString
	CancelledAtTS     sql.NullInt64
	CancelledByUserID sql.NullInt64
	LastErrorReason   sql.NullString
	LastErrorAtTS     sql.NullInt64
	FailureCount      int
}

type claimAllocation struct {
	ClaimID    int64
	TokenID    int64
	FileName   string
	FilePath   string
	Encoding   string
	Content    any
	FirstClaim bool
}

type profileUserPayload struct {
	ID         string `json:"id"`
	Username   string `json:"username"`
	Name       string `json:"name"`
	TrustLevel int64  `json:"trust_level"`
	IsAdmin    bool   `json:"is_admin"`
	IsBanned   bool   `json:"is_banned"`
}

type apiKeySummaryPayload struct {
	Limit  int `json:"limit"`
	Active int `json:"active"`
}

type profilePayload struct {
	User    profileUserPayload   `json:"user"`
	Quota   quotaUsage           `json:"quota"`
	Claims  map[string]int       `json:"claims"`
	APIKeys apiKeySummaryPayload `json:"api_keys"`
	Uploads map[string]int       `json:"uploads"`
}

type runtimeSnapshotPayload struct {
	User           *profileUserPayload             `json:"user,omitempty"`
	Quota          quotaUsage                      `json:"quota"`
	Claims         map[string]int                  `json:"claims"`
	APIKeys        map[string]apiKeySummaryPayload `json:"api_keys"`
	Uploads        map[string]int                  `json:"uploads,omitempty"`
	UploadResults  map[string]any                  `json:"upload_results"`
	Debug          debugSettingsPayload            `json:"debug"`
	DataSource     string                          `json:"data_source,omitempty"`
	GeneratedAt    string                          `json:"generated_at,omitempty"`
	StaleAt        string                          `json:"stale_at,omitempty"`
	Degraded       bool                            `json:"degraded,omitempty"`
	DegradedReason string                          `json:"degraded_reason,omitempty"`
}

func NewService(cfg config.Config, store *database.Store, authService *auth.Service, cache *runtimecache.AppCache, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}

	return &Service{
		cfg:                  cfg,
		store:                store,
		auth:                 authService,
		logger:               logger,
		cache:                cache,
		claimProbe:           proberuntime.New(cfg.Probe.DelaySec, cfg.Probe.TimeoutSec),
		uploadProbe:          proberuntime.New(cfg.Probe.DelaySec, cfg.Probe.TimeoutSec),
		wakeCh:               make(chan struct{}, 1),
		systemIndexUpdatedAt: isoformatNow(),
		queueEvents:          newQueueStatusEventBroker(),
		adminQueueEvents:     newQueueStatusEventBroker(),
		claimEvents:          newClaimRealtimeEventBroker(),
		uploadEvents:         newUploadResultsEventBroker(),
		hideClaimsCh:         make(chan hideClaimsTask, 256),
		uploadTaskCh:         make(chan uploadTask, 128),
		uploadSnapshots:      make(map[int64]uploadSnapshot),
		uploadPending:        make(map[string]*uploadPendingRecord),
		queueNotifyCh:        make(chan int64, 1024),
		startupInProgress:    true,
		startupReadyCh:       make(chan struct{}),
		tokenImportCh:        make(chan tokenImportRequest, 512),
		tokenImportDoneCh:    make(chan tokenImportResult, 512),
		tokenImportPending:   make(map[string]int),
		tokenInternalWrites:  make(map[string]time.Time),
		tokenDeletePending:   make(map[string]struct{}),
		removeTokenFile:      os.Remove,
		promoteTokenFile:     os.Rename,
	}
}

func (s *Service) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		s.setLifecycleContext(ctx)
		s.activeClaimProbe().Start()
		s.activeUploadProbe().Start()
		go func() {
			<-ctx.Done()
			s.activeClaimProbe().Stop()
			s.activeUploadProbe().Stop()
		}()

		ticker := time.NewTicker(queuePumpInterval)
		s.goWorker(func() {
			s.claimTrace("queue pump worker started", "interval_ms", durationMillis(queuePumpInterval))
			defer ticker.Stop()
			if !s.waitForStartupReady(ctx, "queue_pump") {
				s.claimTrace("queue pump worker stopped before startup ready", "reason", ctx.Err())
				return
			}
			s.safeAdvanceQueue(ctx)
			for {
				trigger := "ticker"
				select {
				case <-ctx.Done():
					s.claimTrace("queue pump worker stopped", "reason", ctx.Err())
					return
				case <-ticker.C:
				case <-s.wakeCh:
					trigger = "wake"
				}
				if trigger == "wake" {
					s.claimTraceWithDB("queue pump wake received")
				}
				s.safeAdvanceQueue(ctx)
			}
		})

		s.goWorker(func() { s.uploadWorkerLoop(ctx) })
		s.goWorker(func() { s.hideClaimsWorkerLoop(ctx) })
		s.goWorker(func() { s.tokenImportLoop(ctx) })
		s.goWorker(func() { s.awaitStartupReconcile(ctx) })
		s.goWorker(func() { s.tokenWatchLoop(ctx) })
		s.goWorker(func() { s.queueNotificationWorkerLoop(ctx) })
		s.goWorker(func() { s.warmReadCaches(ctx) })
	})
}

func (s *Service) goWorker(fn func()) {
	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		fn()
	}()
}

func (s *Service) Stop(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.workerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) safeAdvanceQueue(ctx context.Context) {
	if err := s.AdvanceQueue(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("advance claim queue", "error", err)
	}
}

func (s *Service) wakeQueuePump() {
	select {
	case s.wakeCh <- struct{}{}:
		s.claimTraceWithDB("queue pump wake signalled", "delivered", true)
	default:
		s.claimTraceWithDB("queue pump wake signalled", "delivered", false, "reason", "already_pending")
	}
}

func (s *Service) queueEnabled() bool {
	return s != nil && s.cfg.Server.QueueEnabled
}

func (s *Service) activeClaimProbe() claimProbe {
	if s != nil && s.probe != nil {
		return s.probe
	}
	if s != nil && s.claimProbe != nil {
		return s.claimProbe
	}
	return noopClaimProbe{}
}

func (s *Service) activeUploadProbe() claimProbe {
	if s != nil && s.probe != nil {
		return s.probe
	}
	if s != nil && s.uploadProbe != nil {
		return s.uploadProbe
	}
	return noopClaimProbe{}
}

type noopClaimProbe struct{}

func (noopClaimProbe) Start() {}

func (noopClaimProbe) Stop() {}

func (noopClaimProbe) Submit(map[string]any, float64) proberuntime.Result {
	return proberuntime.Result{Status: "non_401_error", Detail: "probe_unavailable"}
}

func (s *Service) ClaimTokens(ctx context.Context, userID int64, apiKeyID *int64, count int) (*claimResult, error) {
	return s.claimTokens(ctx, userID, apiKeyID, count, "", "")
}

func (s *Service) createQueuedClaimRequest(ctx context.Context, userID int64, apiKeyID *int64, count int, originSessionID string, originTabID string) (*claimResult, error) {
	requested := maxInt(1, count)
	now := time.Now().Unix()
	originSessionID = strings.TrimSpace(originSessionID)
	originTabID = strings.TrimSpace(originTabID)
	s.claimTraceWithDB(
		"create queued claim request started",
		"user_id", userID,
		"api_key_id", nullableInt64(apiKeyID),
		"requested_count", requested,
		"origin_session_id", originSessionID,
		"origin_tab_id", originTabID,
		"startup_ready", s.isStartupReady(),
	)

	type claimRequestBootstrap struct {
		Item      *userQueueEntry
		Terminal  *claimResult
		Used      int
		Limit     int
		Remaining int
	}

	initial, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (claimRequestBootstrap, error) {
		policy, err := s.ensureInventoryPolicyTx(ctx, tx, false)
		if err != nil {
			return claimRequestBootstrap{}, err
		}
		if requested > policy.HourlyLimit {
			requested = policy.HourlyLimit
		}
		if requested <= 0 {
			return claimRequestBootstrap{}, echo.NewHTTPError(http.StatusTooManyRequests, "您当前小时内的兑换额度已用完")
		}

		used, err := s.countUserClaimsSince(ctx, tx, userID, now-3600)
		if err != nil {
			return claimRequestBootstrap{}, err
		}

		queueState, assessErr := s.assessQueueAdvanceQueryer(ctx, tx, userQueueEntry{
			UserID:       userID,
			APIKeyID:     sqlNullInt64(apiKeyID),
			Requested:    requested,
			Remaining:    requested,
			Status:       queueStatusQueuedWaiting,
			EnqueuedAtTS: now,
		}, policy)
		if assessErr != nil {
			return claimRequestBootstrap{}, assessErr
		}
		s.claimTraceWithDB(
			"queued claim request assessed",
			"user_id", userID,
			"requested_count", requested,
			"used_quota", used,
			"hourly_limit", policy.HourlyLimit,
			"remaining_quota", maxInt(0, policy.HourlyLimit-used),
			"queue_status", queueState.Status,
			"block_reason", queueState.BlockReason,
			"next_retry_at_ts", queueState.NextRetryAtTS,
		)

		remainingQuota := maxInt(0, policy.HourlyLimit-used)
		if strings.TrimSpace(queueState.BlockReason) == queueBlockReasonHourlyQuotaExhausted {
			requestID, err := randomRequestID()
			if err != nil {
				return claimRequestBootstrap{}, fmt.Errorf("generate request id: %w", err)
			}
			return claimRequestBootstrap{
				Terminal: &claimResult{
					RequestID:      requestID,
					Items:          []claimResponseItem{},
					Requested:      requested,
					Granted:        0,
					Queued:         false,
					QueueStatus:    queueStatusPartial,
					QueueRemaining: requested,
					BlockReason:    queueBlockReasonHourlyQuotaExhausted,
					Quota: quotaUsage{
						Used:      used,
						Limit:     policy.HourlyLimit,
						Remaining: remainingQuota,
					},
					TerminalStatus: claimStatusPartial,
					TerminalReason: queueBlockReasonHourlyQuotaExhausted,
				},
				Used:      used,
				Limit:     policy.HourlyLimit,
				Remaining: remainingQuota,
			}, nil
		}

		queueItem, _, err := s.enqueueClaimTx(
			ctx,
			tx,
			userID,
			apiKeyID,
			requested,
			originSessionID,
			originTabID,
			queueState.Status,
			queueState.BlockReason,
			queueState.NextRetryAtTS,
		)
		if err != nil {
			return claimRequestBootstrap{}, err
		}

		return claimRequestBootstrap{
			Item:      queueItem,
			Used:      used,
			Limit:     policy.HourlyLimit,
			Remaining: maxInt(0, policy.HourlyLimit-used),
		}, nil
	})
	if err != nil {
		return nil, err
	}

	if initial.Terminal != nil {
		if err := s.recordTerminalClaimRequest(ctx, userID, apiKeyID, originSessionID, originTabID, initial.Terminal); err != nil {
			return nil, err
		}
		s.claimTraceWithDB(
			"create queued claim request completed",
			"user_id", userID,
			"result", claimTraceClaimResultSummary(initial.Terminal),
		)
		return initial.Terminal, nil
	}

	result := &claimResult{
		RequestID:      initial.Item.RequestID,
		Items:          []claimResponseItem{},
		Requested:      initial.Item.Requested,
		Granted:        maxInt(0, initial.Item.Requested-initial.Item.Remaining),
		Queued:         true,
		QueueStatus:    initial.Item.Status,
		QueueID:        initial.Item.ID,
		QueuePosition:  initial.Item.QueueRank,
		QueueTotal:     initial.Item.QueueRank,
		QueueRemaining: initial.Item.Remaining,
		BlockReason:    strings.TrimSpace(initial.Item.BlockReason.String),
		LastProgressAt: isoformatFromNullableTS(initial.Item.LastProgressAtTS),
		NextRetryAt:    isoformatFromNullableTS(initial.Item.NextRetryAtTS),
		Quota: quotaUsage{
			Used:      initial.Used,
			Limit:     initial.Limit,
			Remaining: initial.Remaining,
		},
	}

	s.afterQueueMutation(ctx, userID)
	s.wakeQueuePump()
	s.publishAdminQueueEnqueued(*initial.Item)
	s.claimTraceWithDB(
		"create queued claim request completed",
		"user_id", userID,
		"result", claimTraceClaimResultSummary(result),
	)
	return result, nil
}

func (s *Service) GetClaimRequest(ctx context.Context, userID int64, requestID string) (*claimResult, error) {
	entry, err := s.loadClaimRequestEntry(ctx, userID, requestID)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}

	items, err := s.listClaimItemsByRequestID(ctx, userID, entry.RequestID)
	if err != nil {
		return nil, err
	}
	quota, err := s.GetQuotaUsage(ctx, userID)
	if err != nil {
		return nil, err
	}

	queued := isQueueActiveStatus(entry.Status) && entry.Remaining > 0
	result := &claimResult{
		RequestID:      entry.RequestID,
		Items:          items,
		Requested:      maxInt(0, entry.Requested),
		Granted:        maxInt(0, entry.Requested-entry.Remaining),
		Queued:         queued,
		QueueStatus:    strings.ToLower(strings.TrimSpace(entry.Status)),
		QueueID:        entry.ID,
		QueueRemaining: maxInt(0, entry.Remaining),
		BlockReason:    strings.TrimSpace(entry.BlockReason.String),
		LastProgressAt: isoformatFromNullableTS(entry.LastProgressAtTS),
		NextRetryAt:    isoformatFromNullableTS(entry.NextRetryAtTS),
		Quota:          quota,
	}
	if queued {
		result.QueuePosition = entry.QueueRank
		queueTotal, err := s.getTotalQueued(ctx, s.store.DB())
		if err != nil {
			return nil, err
		}
		result.QueueTotal = maxInt(queueTotal, entry.QueueRank)
	} else {
		result.TerminalStatus = claimRealtimeStatusFromQueueEntry(*entry)
		result.TerminalReason = strings.TrimSpace(entry.CancelReason.String)
	}
	return result, nil
}

func (s *Service) claimTokens(ctx context.Context, userID int64, apiKeyID *int64, count int, originSessionID string, originTabID string) (*claimResult, error) {
	requested := maxInt(1, count)
	originalRequested := requested
	now := time.Now().Unix()
	originSessionID = strings.TrimSpace(originSessionID)
	originTabID = strings.TrimSpace(originTabID)

	type claimBootstrap struct {
		Target      int
		Used        int
		Limit       int
		Remaining   int
		APIKeyLimit int
		Queued      *claimResult
		QueuedItem  *userQueueEntry
	}

	initial, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (claimBootstrap, error) {
		policy, err := s.ensureInventoryPolicyTx(ctx, tx, false)
		if err != nil {
			return claimBootstrap{}, err
		}

		if requested > policy.HourlyLimit {
			requested = policy.HourlyLimit
		}

		used, err := s.countUserClaimsSince(ctx, tx, userID, now-3600)
		if err != nil {
			return claimBootstrap{}, err
		}

		remaining := maxInt(0, policy.HourlyLimit-used)
		if remaining <= 0 {
			return claimBootstrap{}, echo.NewHTTPError(http.StatusTooManyRequests, "您当前小时内的兑换额度已用完")
		}

		apiKeyLimit := 0
		if apiKeyID != nil {
			apiKeyLimit = s.cfg.APIKeys.RatePerMinute
			if apiKeyLimit > 0 {
				minuteUsed, err := s.countAPIKeyClaimsSince(ctx, tx, *apiKeyID, now-60)
				if err != nil {
					return claimBootstrap{}, err
				}
				remainingMinute := maxInt(0, apiKeyLimit-minuteUsed)
				if remainingMinute <= 0 {
					return claimBootstrap{}, echo.NewHTTPError(http.StatusTooManyRequests, "API key rate limit exceeded.")
				}
				requested = minInt(requested, remainingMinute)
			}
		}

		target := minInt(requested, remaining)
		if target <= 0 {
			return claimBootstrap{}, echo.NewHTTPError(http.StatusTooManyRequests, "您当前小时内的兑换额度已用完")
		}

		hasPending := false
		shouldPreferQueue := false
		enqueueForAsyncProcessing := func() (claimBootstrap, error) {
			queueState, assessErr := s.assessQueueAdvanceQueryer(ctx, tx, userQueueEntry{
				UserID:       userID,
				APIKeyID:     sqlNullInt64(apiKeyID),
				Requested:    target,
				Remaining:    target,
				Status:       queueStatusQueuedWaiting,
				EnqueuedAtTS: now,
			}, policy)
			if assessErr != nil {
				return claimBootstrap{}, assessErr
			}
			queueItem, _, err := s.enqueueClaimTx(ctx, tx, userID, apiKeyID, target, originSessionID, originTabID, queueState.Status, queueState.BlockReason, queueState.NextRetryAtTS)
			if err != nil {
				return claimBootstrap{}, err
			}

			return claimBootstrap{
				Used:        used,
				Limit:       policy.HourlyLimit,
				Remaining:   remaining,
				APIKeyLimit: apiKeyLimit,
				QueuedItem:  queueItem,
				Queued: &claimResult{
					RequestID:      queueItem.RequestID,
					Items:          []claimResponseItem{},
					Requested:      target,
					Granted:        0,
					Queued:         true,
					QueueStatus:    queueItem.Status,
					QueueID:        queueItem.ID,
					QueuePosition:  queueItem.QueueRank,
					QueueTotal:     queueItem.QueueRank,
					QueueRemaining: queueItem.Remaining,
					BlockReason:    strings.TrimSpace(queueItem.BlockReason.String),
					LastProgressAt: isoformatFromNullableTS(queueItem.LastProgressAtTS),
					NextRetryAt:    isoformatFromNullableTS(queueItem.NextRetryAtTS),
					Quota: quotaUsage{
						Used:      used,
						Limit:     policy.HourlyLimit,
						Remaining: remaining,
					},
				},
			}, nil
		}
		if s.queueEnabled() {
			shouldPreferQueue = originalRequested > directClaimSyncMaxCount
			var err error
			hasPending, err = s.hasPendingQueueTx(ctx, tx)
			if err != nil {
				return claimBootstrap{}, err
			}
		}
		if hasPending || shouldPreferQueue {
			return enqueueForAsyncProcessing()
		}

		return claimBootstrap{
			Target:      target,
			Used:        used,
			Limit:       policy.HourlyLimit,
			Remaining:   remaining,
			APIKeyLimit: apiKeyLimit,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	if initial.Queued != nil {
		s.afterQueueMutation(ctx, userID)
		s.wakeQueuePump()
		if initial.QueuedItem != nil {
			s.publishAdminQueueEnqueued(*initial.QueuedItem)
		}
		return initial.Queued, nil
	}

	requestID, err := randomRequestID()
	if err != nil {
		return nil, fmt.Errorf("generate request id: %w", err)
	}

	items := make([]claimResponseItem, 0, initial.Target)
	for len(items) < initial.Target {
		item, err := s.allocateClaimableToken(ctx, userID, apiKeyID, requestID, initial.Limit, initial.APIKeyLimit)
		if err != nil {
			return nil, err
		}
		if item == nil {
			break
		}

		items = append(items, claimResponseItem{
			ClaimID:  item.ClaimID,
			TokenID:  item.TokenID,
			FileName: item.FileName,
			FilePath: item.FilePath,
			Encoding: item.Encoding,
			Content:  item.Content,
		})
	}

	if len(items) == 0 {
		if !s.queueEnabled() {
			return nil, echo.NewHTTPError(http.StatusConflict, "排队机制已禁用，当前没有可直接领取的账号，请稍后重试")
		}
		policy, policyErr := s.getInventoryPolicy(ctx)
		if policyErr != nil {
			return nil, policyErr
		}
		queueState, assessErr := s.assessQueueAdvanceQueryer(ctx, s.store.DB(), userQueueEntry{
			UserID:       userID,
			APIKeyID:     sqlNullInt64(apiKeyID),
			Requested:    initial.Target,
			Remaining:    initial.Target,
			Status:       queueStatusQueuedWaiting,
			EnqueuedAtTS: now,
		}, policy)
		if assessErr != nil {
			return nil, assessErr
		}
		queued, err := s.enqueueClaim(ctx, userID, apiKeyID, initial.Target, initial.Used, initial.Limit, initial.Remaining, originSessionID, originTabID, queueState.Status, queueState.BlockReason, queueState.NextRetryAtTS)
		if err != nil {
			return nil, err
		}
		s.afterQueueMutation(ctx, userID)
		s.wakeQueuePump()
		return queued, nil
	}

	usedAfter := initial.Used + len(items)
	result := &claimResult{
		RequestID: requestID,
		Items:     items,
		Requested: initial.Target,
		Granted:   len(items),
		Queued:    false,
		Quota: quotaUsage{
			Used:      usedAfter,
			Limit:     initial.Limit,
			Remaining: maxInt(0, initial.Limit-usedAfter),
		},
	}
	if err := s.recordTerminalClaimRequest(ctx, userID, apiKeyID, originSessionID, originTabID, result); err != nil {
		return nil, err
	}
	s.invalidateUserQuotaCache(userID)
	s.invalidateUserClaimsCache(userID)
	s.invalidateInventoryCache()
	s.invalidateDashboardClaimCaches()
	s.invalidateAdminCache()
	s.scheduleQueueNotifications(userID)
	return result, nil
}

func (s *Service) AdvanceQueue(ctx context.Context) error {
	s.advanceMu.Lock()
	defer s.advanceMu.Unlock()

	policy, err := s.ensureInventoryPolicy(ctx, true)
	if err != nil {
		return err
	}
	if s.claimTraceEnabled() {
		s.claimTraceWithDB(
			"advance queue pass started",
			"hourly_limit", policy.HourlyLimit,
			"max_claims", policy.MaxClaims,
			"startup_ready", s.isStartupReady(),
		)
	}

	affectedUsers := make(map[int64]struct{}, queueAdvanceMaxResolvedHeadTurns+1)
	claimedUsers := make(map[int64]struct{}, queueAdvanceMaxResolvedHeadTurns+1)
	for resolvedHeads := 0; resolvedHeads < queueAdvanceMaxResolvedHeadTurns; resolvedHeads++ {
		row, err := s.loadQueueHead(ctx)
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		s.claimTraceWithDB("advance queue picked head", "resolved_heads", resolvedHeads, "queue_entry", claimTraceQueueEntrySummary(*row))
		result, err := s.advanceQueueRow(ctx, *row, policy)
		if err != nil {
			s.logger.Warn(
				"advance claim queue row failed",
				"queue_id",
				row.ID,
				"user_id",
				row.UserID,
				"request_id",
				row.RequestID,
				"error",
				err,
			)
			failureResult, failureErr := s.recordQueueFailure(ctx, *row, err.Error())
			if failureErr != nil {
				s.logger.Error(
					"record claim queue failure",
					"queue_id",
					row.ID,
					"user_id",
					row.UserID,
					"error",
					failureErr,
				)
				break
			}
			if failureResult.Changed {
				affectedUsers[row.UserID] = struct{}{}
				s.publishAdminQueueFailure(*row, err.Error(), failureResult.FailureCount, failureResult.Cancelled)
			}
			if failureResult.Cancelled {
				if publishErr := s.publishQueueTerminalState(ctx, *row, queueStatusFailed, queueCancelReasonFailureThreshold); publishErr != nil {
					s.logger.Warn(
						"publish queue failure terminal state",
						"queue_id",
						row.ID,
						"user_id",
						row.UserID,
						"request_id",
						row.RequestID,
						"error",
						publishErr,
					)
				}
				nextHead, headErr := s.loadQueueHead(ctx)
				if headErr != nil {
					return headErr
				}
				if nextHead != nil {
					affectedUsers[nextHead.UserID] = struct{}{}
				}
				continue
			}
			break
		}
		if result.Changed {
			affectedUsers[row.UserID] = struct{}{}
			if result.Claimed {
				claimedUsers[row.UserID] = struct{}{}
			}
			if result.Terminal {
				nextHead, headErr := s.loadQueueHead(ctx)
				if headErr != nil {
					return headErr
				}
				if nextHead != nil {
					affectedUsers[nextHead.UserID] = struct{}{}
				}
			}
		}
		if !result.Terminal {
			break
		}
	}

	if len(affectedUsers) > 0 {
		s.afterQueueMutation(ctx, mapKeysInt64(affectedUsers)...)
	}
	if len(claimedUsers) > 0 {
		for userID := range claimedUsers {
			s.invalidateUserQuotaCache(userID)
			s.invalidateUserClaimsCache(userID)
		}
		s.invalidateInventoryCache()
		s.invalidateDashboardClaimCaches()
	}
	if s.claimTraceEnabled() {
		s.claimTraceWithDB(
			"advance queue pass completed",
			"affected_users", len(affectedUsers),
			"claimed_users", len(claimedUsers),
		)
	}
	return nil
}

func (s *Service) GetQuotaUsage(ctx context.Context, userID int64) (quotaUsage, error) {
	return runtimecache.CacheJSON(s.cache, s.userQuotaCacheKey(userID), s.cfg.Cache.MeTTL, func() (quotaUsage, error) {
		policy, err := s.getInventoryPolicy(ctx)
		if err != nil {
			return quotaUsage{}, err
		}

		used, err := s.countUserClaimsSince(ctx, s.store.DB(), userID, time.Now().Unix()-3600)
		if err != nil {
			return quotaUsage{}, err
		}

		return quotaUsage{
			Used:      used,
			Limit:     policy.HourlyLimit,
			Remaining: maxInt(0, policy.HourlyLimit-used),
		}, nil
	})
}

func (s *Service) GetUserClaimTotals(ctx context.Context, userID int64) (map[string]int, error) {
	return runtimecache.CacheJSON(s.cache, s.userClaimSummaryCacheKey(userID), s.cfg.Cache.MeTTL, func() (map[string]int, error) {
		stats, exists, err := s.getUserClaimStatsRuntimeState(ctx, s.store.DB(), userID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return map[string]int{
				"total":  0,
				"unique": 0,
			}, nil
		}

		return map[string]int{
			"total":  stats.ClaimedTotal,
			"unique": stats.ClaimedUnique,
		}, nil
	})
}

func (s *Service) GetAPIKeySummary(ctx context.Context, userID int64) (apiKeySummaryPayload, error) {
	return runtimecache.CacheJSON(s.cache, s.userAPIKeySummaryCacheKey(userID), s.cfg.Cache.MeTTL, func() (apiKeySummaryPayload, error) {
		active, err := queryCount(ctx, s.store.DB(), `SELECT COUNT(*) FROM api_keys WHERE user_id = ? AND status = 'active'`, userID)
		if err != nil {
			return apiKeySummaryPayload{}, err
		}

		return apiKeySummaryPayload{
			Limit:  s.cfg.APIKeys.MaxPerUser,
			Active: active,
		}, nil
	})
}

func (s *Service) GetProfile(ctx context.Context, requestContext *auth.RequestContext) (profilePayload, error) {
	startedAt := time.Now()
	var (
		quotaDuration   time.Duration
		claimsDuration  time.Duration
		apiKeysDuration time.Duration
		quotaSQLCount   int64
		claimsSQLCount  int64
		apiKeysSQLCount int64
	)

	payload, cacheState, err := runtimecache.CacheJSONWithState(
		s.cache,
		s.userProfileCacheKey(requestContext.UserID, requestContext.IsAdmin),
		s.cfg.Cache.MeTTL,
		func() (profilePayload, error) {
			quotaStage := startObservedReadStage(ctx)
			quota, err := s.GetQuotaUsage(quotaStage.Context(), requestContext.UserID)
			quotaDuration = quotaStage.Duration()
			quotaSQLCount = quotaStage.SQLCount()
			if err != nil {
				return profilePayload{}, err
			}

			claimsStage := startObservedReadStage(ctx)
			claims, err := s.GetUserClaimTotals(claimsStage.Context(), requestContext.UserID)
			claimsDuration = claimsStage.Duration()
			claimsSQLCount = claimsStage.SQLCount()
			if err != nil {
				return profilePayload{}, err
			}

			apiKeysStage := startObservedReadStage(ctx)
			apiKeys, err := s.GetAPIKeySummary(apiKeysStage.Context(), requestContext.UserID)
			apiKeysDuration = apiKeysStage.Duration()
			apiKeysSQLCount = apiKeysStage.SQLCount()
			if err != nil {
				return profilePayload{}, err
			}

			return profilePayload{
				User: profileUserPayload{
					ID:         requestContext.User.ID,
					Username:   requestContext.User.Username,
					Name:       requestContext.User.Name,
					TrustLevel: requestContext.User.TrustLevel,
					IsAdmin:    requestContext.User.IsAdmin,
					IsBanned:   false,
				},
				Quota:   quota,
				Claims:  claims,
				APIKeys: apiKeys,
				Uploads: map[string]int{
					"max_files_per_request": s.cfg.Upload.MaxFilesPerRequest,
					"max_file_size_bytes":   s.cfg.Upload.MaxFileSizeBytes,
					"max_success_per_hour":  s.cfg.Upload.MaxSuccessPerHour,
					"min_trust_level":       s.cfg.LinuxDO.MinTrustLevel,
				},
			}, nil
		},
	)

	logArgs := []any{
		"user_id", requestContext.UserID,
		"cache_state", cacheStateLabel(cacheState),
		"total_ms", durationMillis(time.Since(startedAt)),
		"quota_ms", durationMillis(quotaDuration),
		"claims_ms", durationMillis(claimsDuration),
		"api_keys_ms", durationMillis(apiKeysDuration),
		"quota_sql_count", quotaSQLCount,
		"claims_sql_count", claimsSQLCount,
		"api_keys_sql_count", apiKeysSQLCount,
		"cumulative_sql_count", sumSQLCounts(quotaSQLCount, claimsSQLCount, apiKeysSQLCount),
	}
	logArgs = append(logArgs, s.dbStatsLogArgs()...)
	logArgs = append(logArgs, "error", err)
	s.logger.Info(
		"get profile",
		logArgs...,
	)
	return payload, err
}

func (s *Service) GetRuntimeSnapshot(ctx context.Context, userID int64) (runtimeSnapshotPayload, error) {
	startedAt := time.Now()
	var (
		quotaDuration         time.Duration
		claimsDuration        time.Duration
		apiKeysDuration       time.Duration
		uploadResultsDuration time.Duration
		quotaSQLCount         int64
		claimsSQLCount        int64
		apiKeysSQLCount       int64
		uploadResultsSQLCount int64
	)

	result, err := loadCachedReadResult(
		ctx,
		s,
		s.userRuntimeSnapshotCacheKey(userID),
		s.userRuntimeSnapshotStaleCacheKey(userID),
		s.cfg.Cache.MeTTL,
		func(loadCtx context.Context) (runtimeSnapshotPayload, error) {
			quotaStage := startObservedReadStage(loadCtx)
			quota, quotaErr := s.GetQuotaUsage(quotaStage.Context(), userID)
			quotaDuration = quotaStage.Duration()
			quotaSQLCount = quotaStage.SQLCount()
			if quotaErr != nil {
				return runtimeSnapshotPayload{}, quotaErr
			}

			claimsStage := startObservedReadStage(loadCtx)
			claims, claimsErr := s.GetUserClaimTotals(claimsStage.Context(), userID)
			claimsDuration = claimsStage.Duration()
			claimsSQLCount = claimsStage.SQLCount()
			if claimsErr != nil {
				return runtimeSnapshotPayload{}, claimsErr
			}

			apiKeysStage := startObservedReadStage(loadCtx)
			apiKeys, apiKeysErr := s.GetAPIKeySummary(apiKeysStage.Context(), userID)
			apiKeysDuration = apiKeysStage.Duration()
			apiKeysSQLCount = apiKeysStage.SQLCount()
			if apiKeysErr != nil {
				return runtimeSnapshotPayload{}, apiKeysErr
			}

			uploadResultsStage := startObservedReadStage(loadCtx)
			uploadResults := buildUploadResultsSummaryPayload(s.GetUploadResults(userID))
			uploadResultsDuration = uploadResultsStage.Duration()
			uploadResultsSQLCount = uploadResultsStage.SQLCount()

			generatedAt := isoformatNow()
			return runtimeSnapshotPayload{
				Quota:       quota,
				Claims:      claims,
				DataSource:  dataSourceLive,
				GeneratedAt: generatedAt,
				APIKeys: map[string]apiKeySummaryPayload{
					"summary": apiKeys,
				},
				Uploads: map[string]int{
					"max_files_per_request": s.cfg.Upload.MaxFilesPerRequest,
					"max_file_size_bytes":   s.cfg.Upload.MaxFileSizeBytes,
					"max_success_per_hour":  s.cfg.Upload.MaxSuccessPerHour,
					"min_trust_level":       s.cfg.LinuxDO.MinTrustLevel,
				},
				UploadResults: uploadResults,
			}, nil
		},
	)
	payload := result.Value
	if err == nil {
		payload.DataSource = result.DataSource
		if strings.TrimSpace(result.GeneratedAt) != "" {
			payload.GeneratedAt = result.GeneratedAt
		}
		payload.StaleAt = result.StaleAt
		payload.Degraded = result.Degraded
		payload.DegradedReason = result.DegradedReason
	}
	payload.Debug = s.debugSettingsPayload()

	logArgs := []any{
		"user_id", userID,
		"cache_state", cacheStateLabel(result.CacheState),
		"data_source", payload.DataSource,
		"total_ms", durationMillis(time.Since(startedAt)),
		"quota_ms", durationMillis(quotaDuration),
		"claims_ms", durationMillis(claimsDuration),
		"api_keys_ms", durationMillis(apiKeysDuration),
		"upload_results_ms", durationMillis(uploadResultsDuration),
		"quota_sql_count", quotaSQLCount,
		"claims_sql_count", claimsSQLCount,
		"api_keys_sql_count", apiKeysSQLCount,
		"upload_results_sql_count", uploadResultsSQLCount,
		"cumulative_sql_count", sumSQLCounts(quotaSQLCount, claimsSQLCount, apiKeysSQLCount, uploadResultsSQLCount),
	}
	logArgs = append(logArgs, s.dbStatsLogArgs()...)
	logArgs = append(logArgs, "error", err)
	s.logger.Info(
		"get runtime snapshot",
		logArgs...,
	)
	return payload, err
}

type queueStatusReadMetrics struct {
	EntryDuration      time.Duration
	QueueTotalDuration time.Duration
	InventoryDuration  time.Duration
	EntrySQLCount      int64
	QueueTotalSQLCount int64
	InventorySQLCount  int64
}

func (s *Service) GetQueueStatus(ctx context.Context, userID int64) (queueStatusPayload, error) {
	startedAt := time.Now()
	if payload, ok := s.getCachedQueueStatus(userID); ok {
		payload = withQueueStatusMetadata(payload, dataSourceCache, payload.GeneratedAt, "", "", false)
		payload = s.attachCachedClaimableNow(userID, payload)
		logArgs := []any{
			"user_id", userID,
			"cache_state", string(runtimecache.CacheStateMemoryHit),
			"data_source", payload.DataSource,
			"claimable_now_state", payload.ClaimableNowState,
			"total_ms", durationMillis(time.Since(startedAt)),
			"entry_ms", int64(0),
			"queue_total_ms", int64(0),
			"inventory_ms", int64(0),
			"claimable_ms", int64(0),
			"entry_sql_count", int64(0),
			"queue_total_sql_count", int64(0),
			"inventory_sql_count", int64(0),
			"claimable_sql_count", int64(0),
			"cumulative_sql_count", int64(0),
		}
		logArgs = append(logArgs, s.dbStatsLogArgs()...)
		s.logger.Info(
			"get queue status",
			logArgs...,
		)
		return payload, nil
	}

	var metrics queueStatusReadMetrics
	payload, err := s.loadQueueStatusSnapshotWithMetrics(ctx, userID, &metrics)
	if err != nil {
		if isReadDegradeError(err) {
			if stalePayload, ok := s.getCachedStaleQueueStatus(userID); ok {
				payload = withQueueStatusMetadata(
					stalePayload,
					dataSourceStale,
					stalePayload.GeneratedAt,
					staleTimestamp(stalePayload.GeneratedAt),
					degradedReasonForError(err),
					true,
				)
				payload = s.attachCachedClaimableNow(userID, payload)
				logArgs := []any{
					"user_id", userID,
					"cache_state", string(runtimecache.CacheStateMiss),
					"data_source", payload.DataSource,
					"claimable_now_state", payload.ClaimableNowState,
					"total_ms", durationMillis(time.Since(startedAt)),
					"entry_ms", durationMillis(metrics.EntryDuration),
					"queue_total_ms", durationMillis(metrics.QueueTotalDuration),
					"inventory_ms", durationMillis(metrics.InventoryDuration),
					"claimable_ms", int64(0),
					"entry_sql_count", metrics.EntrySQLCount,
					"queue_total_sql_count", metrics.QueueTotalSQLCount,
					"inventory_sql_count", metrics.InventorySQLCount,
					"claimable_sql_count", int64(0),
					"cumulative_sql_count", sumSQLCounts(metrics.EntrySQLCount, metrics.QueueTotalSQLCount, metrics.InventorySQLCount),
				}
				logArgs = append(logArgs, s.dbStatsLogArgs()...)
				logArgs = append(logArgs, "error", err)
				s.logger.Info(
					"get queue status",
					logArgs...,
				)
				return payload, nil
			}
		}

		logArgs := []any{
			"user_id", userID,
			"cache_state", string(runtimecache.CacheStateMiss),
			"data_source", dataSourceUnavailable,
			"claimable_now_state", dataSourceUnavailable,
			"total_ms", durationMillis(time.Since(startedAt)),
			"entry_ms", durationMillis(metrics.EntryDuration),
			"queue_total_ms", durationMillis(metrics.QueueTotalDuration),
			"inventory_ms", durationMillis(metrics.InventoryDuration),
			"claimable_ms", int64(0),
			"entry_sql_count", metrics.EntrySQLCount,
			"queue_total_sql_count", metrics.QueueTotalSQLCount,
			"inventory_sql_count", metrics.InventorySQLCount,
			"claimable_sql_count", int64(0),
			"cumulative_sql_count", sumSQLCounts(metrics.EntrySQLCount, metrics.QueueTotalSQLCount, metrics.InventorySQLCount),
		}
		logArgs = append(logArgs, s.dbStatsLogArgs()...)
		logArgs = append(logArgs, "error", err)
		s.logger.Info(
			"get queue status",
			logArgs...,
		)
		return queueStatusPayload{}, err
	}

	payload = s.setQueueStatusSnapshot(userID, payload)
	payload = withQueueStatusMetadata(payload, dataSourceLive, payload.GeneratedAt, "", "", false)
	payload = s.attachCachedClaimableNow(userID, payload)
	logArgs := []any{
		"user_id", userID,
		"cache_state", string(runtimecache.CacheStateMiss),
		"data_source", payload.DataSource,
		"claimable_now_state", payload.ClaimableNowState,
		"total_ms", durationMillis(time.Since(startedAt)),
		"entry_ms", durationMillis(metrics.EntryDuration),
		"queue_total_ms", durationMillis(metrics.QueueTotalDuration),
		"inventory_ms", durationMillis(metrics.InventoryDuration),
		"claimable_ms", int64(0),
		"entry_sql_count", metrics.EntrySQLCount,
		"queue_total_sql_count", metrics.QueueTotalSQLCount,
		"inventory_sql_count", metrics.InventorySQLCount,
		"claimable_sql_count", int64(0),
		"cumulative_sql_count", sumSQLCounts(metrics.EntrySQLCount, metrics.QueueTotalSQLCount, metrics.InventorySQLCount),
	}
	logArgs = append(logArgs, s.dbStatsLogArgs()...)
	s.logger.Info(
		"get queue status",
		logArgs...,
	)
	return payload, nil
}

func (s *Service) loadQueueStatusSnapshot(ctx context.Context, userID int64) (queueStatusPayload, error) {
	return s.loadQueueStatusSnapshotWithMetrics(ctx, userID, nil)
}

func (s *Service) loadQueueStatusSnapshotWithMetrics(
	ctx context.Context,
	userID int64,
	metrics *queueStatusReadMetrics,
) (queueStatusPayload, error) {
	entryStage := startObservedReadStage(ctx)
	entry, err := s.getUserQueueEntry(entryStage.Context(), userID)
	if metrics != nil {
		metrics.EntryDuration = entryStage.Duration()
		metrics.EntrySQLCount = entryStage.SQLCount()
	}
	if err != nil {
		return queueStatusPayload{}, err
	}

	queueTotalStage := startObservedReadStage(ctx)
	totalQueued, err := s.getTotalQueued(queueTotalStage.Context(), s.store.DB())
	if metrics != nil {
		metrics.QueueTotalDuration = queueTotalStage.Duration()
		metrics.QueueTotalSQLCount = queueTotalStage.SQLCount()
	}
	if err != nil {
		return queueStatusPayload{}, err
	}

	var queueHead *userQueueEntry
	if entry != nil && entry.QueueRank > 1 {
		headStage := startObservedReadStage(ctx)
		queueHead, err = s.loadQueueHead(headStage.Context())
		if metrics != nil {
			metrics.EntryDuration += headStage.Duration()
			metrics.EntrySQLCount += headStage.SQLCount()
		}
		if err != nil {
			return queueStatusPayload{}, err
		}
	}

	inventoryStage := startObservedReadStage(ctx)
	runtimeState, runtimeExists, err := s.getInventoryRuntimeState(inventoryStage.Context(), s.store.DB())
	if metrics != nil {
		metrics.InventoryDuration = inventoryStage.Duration()
		metrics.InventorySQLCount = inventoryStage.SQLCount()
	}
	if err != nil {
		return queueStatusPayload{}, err
	}

	availableTokens := 0
	if runtimeExists {
		availableTokens = runtimeState.Available
	} else {
		inventoryFallbackStage := startObservedReadStage(ctx)
		snapshot, snapshotErr := s.getInventorySnapshot(inventoryFallbackStage.Context(), s.store.DB())
		if metrics != nil {
			metrics.InventoryDuration += inventoryFallbackStage.Duration()
			metrics.InventorySQLCount += inventoryFallbackStage.SQLCount()
		}
		if snapshotErr != nil {
			return queueStatusPayload{}, snapshotErr
		}
		availableTokens = snapshot["available"]
	}

	return buildQueueStatusPayload(entry, totalQueued, availableTokens, queueHead), nil
}

func deriveFrontQueueBlock(queueHead *userQueueEntry, follower *userQueueEntry) (bool, string, string) {
	if follower == nil || follower.QueueRank <= 1 || queueHead == nil || queueHead.ID <= 0 || queueHead.ID == follower.ID {
		return false, "", ""
	}

	blockReason := strings.TrimSpace(queueHead.BlockReason.String)
	if strings.EqualFold(strings.TrimSpace(queueHead.Status), queueStatusQueuedBlocked) {
		if blockReason == "" {
			blockReason = queueBlockReasonAllocationStalled
		}
		return true, blockReason, isoformatFromNullableTS(queueHead.NextRetryAtTS)
	}

	if queueHead.NextRetryAtTS.Valid && queueHead.NextRetryAtTS.Int64 > time.Now().Unix() {
		if blockReason == "" {
			blockReason = queueBlockReasonAllocationStalled
		}
		return true, blockReason, isoformatFromNullableTS(queueHead.NextRetryAtTS)
	}

	return false, "", ""
}

func buildQueueStatusPayload(entry *userQueueEntry, totalQueued int, availableTokens int, queueHead *userQueueEntry) queueStatusPayload {
	generatedAt := isoformatNow()
	if entry == nil {
		return withQueueStatusMetadata(queueStatusPayload{
			Queued:          false,
			TotalQueued:     totalQueued,
			AvailableTokens: availableTokens,
			ClaimableNow:    nil,
		}, dataSourceLive, generatedAt, "", "", false)
	}

	payload := queueStatusPayload{
		Queued:          true,
		Status:          entry.Status,
		QueueID:         entry.ID,
		Position:        entry.QueueRank,
		Ahead:           maxInt(0, entry.QueueRank-1),
		Requested:       entry.Requested,
		Remaining:       entry.Remaining,
		BlockReason:     strings.TrimSpace(entry.BlockReason.String),
		LastProgressAt:  isoformatFromNullableTS(entry.LastProgressAtTS),
		NextRetryAt:     isoformatFromNullableTS(entry.NextRetryAtTS),
		EnqueuedAt:      isoformatFromTS(entry.EnqueuedAtTS),
		RequestID:       entry.RequestID,
		TotalQueued:     totalQueued,
		AvailableTokens: availableTokens,
		ClaimableNow:    nil,
	}
	if frontBlocked, frontReason, frontNextRetryAt := deriveFrontQueueBlock(queueHead, entry); frontBlocked {
		payload.FrontBlocked = true
		payload.FrontBlockReason = frontReason
		payload.FrontNextRetryAt = frontNextRetryAt
	}
	return withQueueStatusMetadata(payload, dataSourceLive, generatedAt, "", "", false)
}

func (s *Service) attachCachedClaimableNow(userID int64, payload queueStatusPayload) queueStatusPayload {
	if snapshot, ok := s.getCachedClaimableNow(userID); ok {
		return applyClaimableNowSnapshot(payload, snapshot, dataSourceCache)
	}
	if snapshot, ok := s.getCachedStaleClaimableNow(userID); ok {
		payload = applyClaimableNowSnapshot(payload, snapshot, dataSourceStale)
		if strings.TrimSpace(payload.StaleAt) == "" {
			payload.StaleAt = staleTimestamp(snapshot.GeneratedAt)
		}
		if strings.TrimSpace(payload.DegradedReason) == "" {
			payload.DegradedReason = degradedReasonClaimableDeferred
		}
		payload.Degraded = true
		return payload
	}
	payload.ClaimableNow = nil
	payload.ClaimableNowState = dataSourceDeferred
	payload.ClaimableNowUpdatedAt = ""
	if strings.TrimSpace(payload.DegradedReason) == "" {
		payload.DegradedReason = degradedReasonClaimableDeferred
	}
	payload.Degraded = true
	return payload
}

func (s *Service) GetClaimableNow(ctx context.Context, userID int64) (claimableNowPayload, error) {
	startedAt := time.Now()
	stage := startObservedReadStage(ctx)
	snapshot, cacheState, err := runtimecache.CacheJSONWithState(
		s.cache,
		s.userClaimableNowCacheKey(userID),
		s.claimableNowSnapshotTTL(),
		func() (storedClaimableNowSnapshot, error) {
			count, countErr := s.countClaimableTokensForUser(stage.Context(), s.store.DB(), userID)
			if countErr != nil {
				return storedClaimableNowSnapshot{}, countErr
			}
			return storedClaimableNowSnapshot{
				ClaimableNow: count,
				GeneratedAt:  isoformatNow(),
			}, nil
		},
	)
	if err != nil {
		if staleSnapshot, ok := s.getCachedStaleClaimableNow(userID); ok && isReadDegradeError(err) {
			payload := claimableNowPayloadFromSnapshot(
				staleSnapshot,
				dataSourceStale,
				staleTimestamp(staleSnapshot.GeneratedAt),
				degradedReasonForError(err),
				true,
			)
			s.logger.Info(
				"get claimable now",
				"user_id", userID,
				"cache_state", cacheStateLabel(cacheState),
				"data_source", payload.DataSource,
				"total_ms", durationMillis(time.Since(startedAt)),
				"count_ms", durationMillis(stage.Duration()),
				"count_sql_count", stage.SQLCount(),
				"cumulative_sql_count", stage.SQLCount(),
				"error", err,
			)
			return payload, nil
		}

		payload := unavailableClaimableNowPayload(degradedReasonForError(err))
		s.logger.Info(
			"get claimable now",
			"user_id", userID,
			"cache_state", cacheStateLabel(cacheState),
			"data_source", payload.DataSource,
			"total_ms", durationMillis(time.Since(startedAt)),
			"count_ms", durationMillis(stage.Duration()),
			"count_sql_count", stage.SQLCount(),
			"cumulative_sql_count", stage.SQLCount(),
			"error", err,
		)
		return claimableNowPayload{}, err
	}

	if cacheState == runtimecache.CacheStateMiss {
		snapshot = s.setClaimableNowSnapshot(userID, snapshot)
	}

	dataSource := dataSourceLive
	if cacheState == runtimecache.CacheStateMemoryHit || cacheState == runtimecache.CacheStateFlightShared {
		dataSource = dataSourceCache
	}
	payload := claimableNowPayloadFromSnapshot(snapshot, dataSource, "", "", false)
	s.logger.Info(
		"get claimable now",
		"user_id", userID,
		"cache_state", cacheStateLabel(cacheState),
		"data_source", payload.DataSource,
		"total_ms", durationMillis(time.Since(startedAt)),
		"count_ms", durationMillis(stage.Duration()),
		"count_sql_count", stage.SQLCount(),
		"cumulative_sql_count", stage.SQLCount(),
	)
	return payload, nil
}

func (s *Service) ListClaims(ctx context.Context, userID int64) ([]claimHistoryItem, error) {
	return runtimecache.CacheJSON(s.cache, s.claimsListCacheKey(userID), s.cfg.Cache.ClaimsTTL, func() ([]claimHistoryItem, error) {
		rows, err := s.store.DB().QueryContext(ctx, `
			SELECT token_claims.id,
			       token_claims.claimed_at_ts,
			       token_claims.request_id,
			       token_claims.token_id,
			       COALESCE(token_claims.claim_file_name, tokens.file_name) AS file_name,
			       COALESCE(token_claims.claim_file_path, tokens.file_path) AS file_path,
			       COALESCE(token_claims.claim_encoding, tokens.encoding) AS encoding,
			       COALESCE(token_claims.claim_content_json, tokens.content_json) AS content_json,
			       token_claims.provider_user_id,
			       token_claims.provider_username,
			       token_claims.provider_name
			FROM token_claims
			LEFT JOIN tokens ON tokens.id = token_claims.token_id
			WHERE token_claims.user_id = ? AND token_claims.is_hidden = 0
			ORDER BY token_claims.claimed_at_ts DESC, token_claims.id DESC
		`, userID)
		if err != nil {
			return nil, fmt.Errorf("list claims: %w", err)
		}
		defer rows.Close()

		items := make([]claimHistoryItem, 0)
		for rows.Next() {
			var (
				item             claimHistoryItem
				claimedAtTS      int64
				contentJSON      string
				providerUserID   sql.NullString
				providerUsername sql.NullString
				providerName     sql.NullString
			)
			if err := rows.Scan(
				&item.ClaimID,
				&claimedAtTS,
				&item.RequestID,
				&item.TokenID,
				&item.FileName,
				&item.FilePath,
				&item.Encoding,
				&contentJSON,
				&providerUserID,
				&providerUsername,
				&providerName,
			); err != nil {
				return nil, fmt.Errorf("scan claim row: %w", err)
			}

			content, err := decodeJSONContent(contentJSON)
			if err != nil {
				return nil, err
			}

			item.ClaimedAt = isoformatFromTS(claimedAtTS)
			item.Content = content
			if providerUserID.Valid || providerUsername.Valid || providerName.Valid {
				item.Provider = &providerPayload{
					UserID:   providerUserID.String,
					Username: providerUsername.String,
					Name:     firstNonEmpty(providerName.String, providerUsername.String),
				}
			}
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate claims: %w", err)
		}

		return items, nil
	})
}

func (s *Service) HideClaims(ctx context.Context, userID int64, claimIDs []int64) error {
	if len(claimIDs) == 0 {
		return nil
	}

	placeholders := make([]string, 0, len(claimIDs))
	args := make([]any, 0, len(claimIDs)+1)
	args = append(args, userID)
	for _, claimID := range claimIDs {
		placeholders = append(placeholders, "?")
		args = append(args, claimID)
	}

	query := fmt.Sprintf(`
		UPDATE token_claims
		SET is_hidden = 1
		WHERE user_id = ? AND id IN (%s)
	`, strings.Join(placeholders, ","))

	if _, err := execWriteContext(ctx, s.store.DB(), query, args...); err != nil {
		return fmt.Errorf("hide claims: %w", err)
	}

	return nil
}

func (s *Service) GetDashboardSummary(ctx context.Context, userID int64, window string, bucket string) (map[string]any, error) {
	return s.GetDashboardSummaryWithOptions(ctx, userID, dashboardSummaryOptions{
		Window:                 window,
		Bucket:                 bucket,
		LeaderboardWindow:      "24h",
		LeaderboardLimit:       10,
		RecentLimit:            10,
		ContributorLimit:       10,
		RecentContributorLimit: 10,
	})
}

func (s *Service) loadSystemStatus(ctx context.Context) (map[string]any, error) {
	snapshot, runtimeState, runtimeExists, err := s.loadInventorySnapshotForRead(ctx, s.store.DB())
	if err != nil {
		return nil, err
	}

	policy := s.buildInventoryPolicy(snapshot)
	if runtimeExists {
		policy = s.buildInventoryPolicyForStatus(snapshot, runtimeState.Status)
	}
	totalQueued, err := s.getTotalQueued(ctx, s.store.DB())
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"inventory": snapshot,
		"queue": map[string]any{
			"total": totalQueued,
		},
		"health": map[string]any{
			"status":       policy.Status,
			"hourly_limit": policy.HourlyLimit,
			"max_claims":   policy.MaxClaims,
			"thresholds":   policy.Thresholds,
		},
		"index": map[string]any{
			"updated_at": s.systemIndexTimestamp(),
		},
	}, nil
}

func (s *Service) getSystemStatusObserved(ctx context.Context) (cachedReadResult[map[string]any], error) {
	return loadCachedReadResult(
		ctx,
		s,
		s.dashboardCacheKey("dashboard-system", nil),
		s.dashboardStaleCacheKey("dashboard-system", nil),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) (map[string]any, error) {
			return s.loadSystemStatus(loadCtx)
		},
	)
}

func (s *Service) getSystemStatus(ctx context.Context) (map[string]any, error) {
	result, err := s.getSystemStatusObserved(ctx)
	return result.Value, err
}

func (s *Service) systemIndexTimestamp() string {
	s.systemIndexMu.RLock()
	defer s.systemIndexMu.RUnlock()
	return s.systemIndexUpdatedAt
}

func (s *Service) touchSystemIndexTimestamp() {
	s.systemIndexMu.Lock()
	s.systemIndexUpdatedAt = isoformatNow()
	s.systemIndexMu.Unlock()
}

func (s *Service) loadDashboardStats(ctx context.Context, userID int64) (map[string]int, error) {
	snapshot, _, _, err := s.loadInventorySnapshotForRead(ctx, s.store.DB())
	if err != nil {
		return nil, err
	}

	runtimeStats, runtimeExists, err := s.getClaimStatsRuntimeState(ctx, s.store.DB())
	if err != nil {
		return nil, err
	}
	if !runtimeExists {
		return nil, fmt.Errorf("claim stats runtime unavailable")
	}

	userStats, _, err := s.getUserClaimStatsRuntimeState(ctx, s.store.DB(), userID)
	if err != nil {
		return nil, err
	}

	othersClaimedTotal := maxInt(0, runtimeStats.ClaimedTotal-userStats.ClaimedTotal)
	othersClaimedUnique := maxInt(0, runtimeStats.ClaimedUnique-userStats.ExclusiveClaimedUnique)

	return map[string]int{
		"total_tokens":          snapshot["total"],
		"available_tokens":      snapshot["available"],
		"claimed_total":         runtimeStats.ClaimedTotal,
		"claimed_unique":        runtimeStats.ClaimedUnique,
		"others_claimed_total":  othersClaimedTotal,
		"others_claimed_unique": othersClaimedUnique,
	}, nil
}

func (s *Service) getDashboardStatsObserved(ctx context.Context, userID int64) (cachedReadResult[map[string]int], error) {
	return loadCachedReadResult(
		ctx,
		s,
		s.dashboardCacheKey("dashboard-stats", &userID),
		s.dashboardStaleCacheKey("dashboard-stats", &userID),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) (map[string]int, error) {
			return s.loadDashboardStats(loadCtx, userID)
		},
	)
}

func (s *Service) getDashboardStats(ctx context.Context, userID int64) (map[string]int, error) {
	result, err := s.getDashboardStatsObserved(ctx, userID)
	return result.Value, err
}

func (s *Service) enqueueClaim(ctx context.Context, userID int64, apiKeyID *int64, target int, used int, limit int, remaining int, originSessionID string, originTabID string, queueStatus string, blockReason string, nextRetryAtTS int64) (*claimResult, error) {
	type queueInsertResult struct {
		Item *userQueueEntry
	}

	inserted, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (queueInsertResult, error) {
		item, _, err := s.enqueueClaimTx(ctx, tx, userID, apiKeyID, target, originSessionID, originTabID, queueStatus, blockReason, nextRetryAtTS)
		if err != nil {
			return queueInsertResult{}, err
		}
		return queueInsertResult{Item: item}, nil
	})
	if err != nil {
		return nil, err
	}

	s.publishAdminQueueEnqueued(*inserted.Item)
	return &claimResult{
		RequestID:      inserted.Item.RequestID,
		Items:          []claimResponseItem{},
		Requested:      target,
		Granted:        0,
		Queued:         true,
		QueueStatus:    inserted.Item.Status,
		QueueID:        inserted.Item.ID,
		QueuePosition:  inserted.Item.QueueRank,
		QueueTotal:     inserted.Item.QueueRank,
		QueueRemaining: inserted.Item.Remaining,
		BlockReason:    strings.TrimSpace(inserted.Item.BlockReason.String),
		LastProgressAt: isoformatFromNullableTS(inserted.Item.LastProgressAtTS),
		NextRetryAt:    isoformatFromNullableTS(inserted.Item.NextRetryAtTS),
		Quota: quotaUsage{
			Used:      used,
			Limit:     limit,
			Remaining: remaining,
		},
	}, nil
}

func (s *Service) enqueueClaimTx(ctx context.Context, tx *sql.Tx, userID int64, apiKeyID *int64, requested int, originSessionID string, originTabID string, queueStatus string, blockReason string, nextRetryAtTS int64) (*userQueueEntry, bool, error) {
	existing, err := s.getUserQueueEntryTx(ctx, tx, userID)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		return existing, true, nil
	}

	totalQueued, err := s.getTotalQueued(ctx, tx)
	if err != nil {
		return nil, false, err
	}

	requestID, err := randomRequestID()
	if err != nil {
		return nil, false, fmt.Errorf("generate queue request id: %w", err)
	}

	now := time.Now().Unix()
	var apiKeyArg any
	if apiKeyID != nil {
		apiKeyArg = *apiKeyID
	}
	var originSessionArg any
	if trimmed := strings.TrimSpace(originSessionID); trimmed != "" {
		originSessionArg = trimmed
	}
	var originTabArg any
	if trimmed := strings.TrimSpace(originTabID); trimmed != "" {
		originTabArg = trimmed
	}
	normalizedQueueStatus := strings.ToLower(strings.TrimSpace(queueStatus))
	if !isQueueActiveStatus(normalizedQueueStatus) {
		normalizedQueueStatus = queueStatusQueuedWaiting
	}
	var blockReasonArg any
	if trimmed := strings.TrimSpace(blockReason); trimmed != "" {
		blockReasonArg = trimmed
	}
	var nextRetryArg any
	if nextRetryAtTS > 0 {
		nextRetryArg = nextRetryAtTS
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO claim_queue (
			user_id, api_key_id, requested, remaining, queue_rank, enqueued_at_ts, request_id, status, origin_session_id, origin_tab_id, block_reason, next_retry_at_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, userID, apiKeyArg, requested, requested, totalQueued+1, now, requestID, normalizedQueueStatus, originSessionArg, originTabArg, blockReasonArg, nextRetryArg)
	if err != nil {
		return nil, false, fmt.Errorf("insert claim queue row: %w", err)
	}

	queueID, err := result.LastInsertId()
	if err != nil {
		return nil, false, fmt.Errorf("read queue row id: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO queue_runtime (id, total_queued, updated_at_ts)
		VALUES (1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			total_queued = excluded.total_queued,
			updated_at_ts = excluded.updated_at_ts
	`, totalQueued+1, now); err != nil {
		return nil, false, fmt.Errorf("update queue runtime: %w", err)
	}

	return &userQueueEntry{
		ID:           queueID,
		UserID:       userID,
		Requested:    requested,
		Remaining:    requested,
		QueueRank:    totalQueued + 1,
		EnqueuedAtTS: now,
		RequestID:    requestID,
		Status:       normalizedQueueStatus,
		OriginSessionID: sql.NullString{
			String: strings.TrimSpace(originSessionID),
			Valid:  strings.TrimSpace(originSessionID) != "",
		},
		OriginTabID: sql.NullString{
			String: strings.TrimSpace(originTabID),
			Valid:  strings.TrimSpace(originTabID) != "",
		},
		BlockReason: sql.NullString{
			String: strings.TrimSpace(blockReason),
			Valid:  strings.TrimSpace(blockReason) != "",
		},
		NextRetryAtTS: sql.NullInt64{
			Int64: nextRetryAtTS,
			Valid: nextRetryAtTS > 0,
		},
	}, false, nil
}

func (s *Service) recordTerminalClaimRequest(ctx context.Context, userID int64, apiKeyID *int64, originSessionID string, originTabID string, result *claimResult) error {
	if userID <= 0 || result == nil || strings.TrimSpace(result.RequestID) == "" {
		return nil
	}

	requested := maxInt(0, result.Requested)
	granted := maxInt(0, result.Granted)
	remaining := maxInt(0, requested-granted)
	status := claimResultTerminalStatus(result)
	reasonCode := claimResultTerminalReason(result)

	nowTS := time.Now().Unix()
	var cancelledAtArg any
	switch status {
	case queueStatusCancelled, queueStatusExpired, queueStatusFailed:
		cancelledAtArg = nowTS
	}
	var apiKeyArg any
	if apiKeyID != nil {
		apiKeyArg = *apiKeyID
	}
	var originSessionArg any
	if trimmed := strings.TrimSpace(originSessionID); trimmed != "" {
		originSessionArg = trimmed
	}
	var originTabArg any
	if trimmed := strings.TrimSpace(originTabID); trimmed != "" {
		originTabArg = trimmed
	}

	_, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (int64, error) {
		row := tx.QueryRowContext(ctx, `
			SELECT id
			FROM claim_queue
			WHERE user_id = ? AND request_id = ?
			ORDER BY id DESC
			LIMIT 1
		`, userID, result.RequestID)

		var existingID int64
		switch scanErr := row.Scan(&existingID); scanErr {
		case nil:
			if _, err := tx.ExecContext(ctx, `
				UPDATE claim_queue
				SET api_key_id = ?,
				    requested = ?,
				    remaining = ?,
				    queue_rank = 0,
				    status = ?,
				    origin_session_id = ?,
				    origin_tab_id = ?,
				    block_reason = NULL,
				    next_retry_at_ts = NULL,
				    last_progress_at_ts = ?,
				    terminal_at_ts = ?,
				    cancel_reason = ?,
				    cancelled_at_ts = ?,
				    cancelled_by_user_id = NULL,
				    last_error_reason = NULL,
				    last_error_at_ts = NULL,
				    failure_count = 0
				WHERE id = ?
			`, apiKeyArg, requested, remaining, status, originSessionArg, originTabArg, nowTS, nowTS, nullIfEmpty(reasonCode), cancelledAtArg, existingID); err != nil {
				return 0, fmt.Errorf("update terminal claim request %s: %w", result.RequestID, err)
			}
			return existingID, nil
		case sql.ErrNoRows:
		default:
			return 0, fmt.Errorf("load claim request row %s: %w", result.RequestID, scanErr)
		}

		record, err := tx.ExecContext(ctx, `
			INSERT INTO claim_queue (
				user_id,
				api_key_id,
				requested,
				remaining,
				queue_rank,
				enqueued_at_ts,
				request_id,
				status,
				origin_session_id,
				origin_tab_id,
				block_reason,
				next_retry_at_ts,
				last_progress_at_ts,
				terminal_at_ts,
				cancel_reason,
				cancelled_at_ts
			) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)
		`, userID, apiKeyArg, requested, remaining, nowTS, result.RequestID, status, originSessionArg, originTabArg, nowTS, nowTS, nullIfEmpty(reasonCode), cancelledAtArg)
		if err != nil {
			return 0, fmt.Errorf("insert terminal claim request %s: %w", result.RequestID, err)
		}
		insertedID, err := record.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("read terminal claim request id %s: %w", result.RequestID, err)
		}
		return insertedID, nil
	})
	return err
}

func (s *Service) consumeQueueGrant(ctx context.Context, queueID int64, granted int) (bool, error) {
	if granted <= 0 {
		return false, nil
	}

	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (bool, error) {
		row := tx.QueryRowContext(ctx, `
			SELECT remaining
			FROM claim_queue
			WHERE id = ? AND status IN ('queued', 'queued_waiting', 'queued_blocked')
		`, queueID)

		var remaining int
		if err := row.Scan(&remaining); err != nil {
			if err == sql.ErrNoRows {
				return false, nil
			}
			return false, fmt.Errorf("load queue row %d: %w", queueID, err)
		}

		remainingAfter := maxInt(0, remaining-granted)
		nowTS := time.Now().Unix()
		if remainingAfter <= 0 {
			if _, err := tx.ExecContext(ctx, `
				UPDATE claim_queue
				SET remaining = 0,
				    queue_rank = 0,
				    status = ?,
				    block_reason = NULL,
				    next_retry_at_ts = NULL,
				    last_progress_at_ts = ?,
				    terminal_at_ts = ?,
				    cancel_reason = NULL,
				    cancelled_at_ts = NULL,
				    cancelled_by_user_id = NULL,
				    last_error_reason = NULL,
				    last_error_at_ts = NULL,
				    failure_count = 0
				WHERE id = ?
			`, queueStatusSucceeded, nowTS, nowTS, queueID); err != nil {
				return false, fmt.Errorf("complete queue row %d: %w", queueID, err)
			}
		} else {
			if _, err := tx.ExecContext(ctx, `
				UPDATE claim_queue
				SET remaining = ?,
				    status = ?,
				    block_reason = NULL,
				    next_retry_at_ts = NULL,
				    last_progress_at_ts = ?,
				    terminal_at_ts = NULL,
				    cancel_reason = NULL,
				    cancelled_at_ts = NULL,
				    cancelled_by_user_id = NULL,
				    last_error_reason = NULL,
				    last_error_at_ts = NULL,
				    failure_count = 0
				WHERE id = ?
			`, remainingAfter, queueStatusQueuedWaiting, nowTS, queueID); err != nil {
				return false, fmt.Errorf("update queue remaining: %w", err)
			}
		}
		if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
			return false, err
		}

		return remainingAfter <= 0, nil
	})
}

func (s *Service) allocateClaimableToken(ctx context.Context, userID int64, apiKeyID *int64, requestID string, hourlyLimit int, apiKeyLimit int) (*claimAllocation, error) {
	for attempt := 0; attempt < 16; attempt++ {
		item, retry, err := s.allocateClaimableTokenOnce(ctx, userID, apiKeyID, requestID, hourlyLimit, apiKeyLimit, 0)
		if err != nil {
			return nil, err
		}
		if item != nil {
			return item, nil
		}
		if !retry {
			return nil, nil
		}
	}
	return nil, nil
}

func (s *Service) allocateClaimableTokenForQueue(ctx context.Context, userID int64, apiKeyID *int64, requestID string, hourlyLimit int, apiKeyLimit int, budget time.Duration) (*claimAllocation, error) {
	item, _, err := s.allocateClaimableTokenOnce(ctx, userID, apiKeyID, requestID, hourlyLimit, apiKeyLimit, budget)
	return item, err
}

func (s *Service) allocateClaimableTokenOnce(ctx context.Context, userID int64, apiKeyID *int64, requestID string, hourlyLimit int, apiKeyLimit int, budget time.Duration) (*claimAllocation, bool, error) {
	candidate, err := s.reserveClaimableTokenForUser(ctx, userID)
	if err != nil {
		return nil, false, err
	}
	if candidate == nil {
		return nil, false, nil
	}

	probeResult := s.submitProbeWithBudget(s.activeClaimProbe(), candidate.Content, s.cfg.Probe.TimeoutSec+s.cfg.Probe.DelaySec+5.0, budget)
	if probeResult.IsBanned() {
		if err := s.markTokenBanned(ctx, candidate.TokenID, "upstream_401"); err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	if probeResult.Status != "ok" {
		statusText := probeResult.Status
		clearLock := false
		if probeResult.Detail == "queue_tick_timeout" {
			statusText = probeResult.Detail
			clearLock = true
		}
		if err := s.recordProbeStatus(ctx, candidate.TokenID, statusText, clearLock); err != nil {
			return nil, false, err
		}
		if probeResult.Detail == "queue_tick_timeout" {
			return nil, false, nil
		}
	}

	item, err := s.finalizeClaimReservedToken(ctx, candidate.TokenID, userID, apiKeyID, requestID, hourlyLimit, apiKeyLimit)
	if err != nil {
		return nil, false, err
	}
	return item, item == nil, nil
}

func (s *Service) submitProbeWithBudget(probe claimProbe, tokenContent map[string]any, waitTimeoutSec float64, budget time.Duration) proberuntime.Result {
	if probe == nil {
		return proberuntime.Result{Status: "non_401_error", Detail: "probe_unavailable"}
	}
	if budget <= 0 {
		return probe.Submit(tokenContent, waitTimeoutSec)
	}

	resultCh := make(chan proberuntime.Result, 1)
	go func() {
		resultCh <- probe.Submit(tokenContent, waitTimeoutSec)
	}()

	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case result := <-resultCh:
		return result
	case <-timer.C:
		return proberuntime.Result{Status: "non_401_error", Detail: "queue_tick_timeout"}
	}
}

func (s *Service) ensureInventoryPolicy(ctx context.Context, refreshExistingScope bool) (inventoryPolicy, error) {
	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (inventoryPolicy, error) {
		return s.ensureInventoryPolicyTx(ctx, tx, refreshExistingScope)
	})
}

func (s *Service) ensureInventoryPolicyTx(ctx context.Context, tx *sql.Tx, refreshExistingScope bool) (inventoryPolicy, error) {
	snapshot, err := s.getInventorySnapshot(ctx, tx)
	if err != nil {
		return inventoryPolicy{}, err
	}

	policy := s.buildInventoryPolicy(snapshot)
	runtimeState, runtimeExists, err := s.getInventoryRuntimeState(ctx, tx)
	if err != nil {
		return inventoryPolicy{}, err
	}

	healthyMaxClaims := s.cfg.Inventory.Limits.Healthy.MaxClaims
	applyNonHealthyScope := func(now int64) error {
		if policy.MaxClaims <= healthyMaxClaims {
			return nil
		}

		if policy.NonHealthyMaxClaimsScope == "all_unfinished" {
			if _, err := tx.ExecContext(ctx, `
				UPDATE tokens
				SET max_claims = ?,
					is_available = CASE WHEN claim_count < ? THEN 1 ELSE 0 END,
					updated_at_ts = ?
				WHERE is_active = 1
				  AND is_enabled = 1
				  AND claim_count < ?
				  AND max_claims < ?
			`, policy.MaxClaims, policy.MaxClaims, now, policy.MaxClaims, policy.MaxClaims); err != nil {
				return fmt.Errorf("apply non healthy max claims policy: %w", err)
			}
			return nil
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE tokens
			SET max_claims = ?,
				is_available = CASE WHEN claim_count < ? THEN 1 ELSE 0 END,
				updated_at_ts = ?
			WHERE is_active = 1
			  AND is_enabled = 1
			  AND claim_count = 0
			  AND max_claims < ?
		`, policy.MaxClaims, policy.MaxClaims, now, policy.MaxClaims); err != nil {
			return fmt.Errorf("apply non healthy unclaimed policy: %w", err)
		}
		return nil
	}

	now := time.Now().Unix()
	tokensChanged := false
	if !runtimeExists || runtimeState.Status != policy.Status || runtimeState.MaxClaims != policy.MaxClaims {
		if _, err := tx.ExecContext(ctx, `
			UPDATE tokens
			SET max_claims = ?,
				is_available = CASE WHEN claim_count < ? THEN 1 ELSE 0 END,
				updated_at_ts = ?
			WHERE is_active = 1 AND is_enabled = 1
		`, healthyMaxClaims, healthyMaxClaims, now); err != nil {
			return inventoryPolicy{}, fmt.Errorf("reset token max claims policy: %w", err)
		}

		if err := applyNonHealthyScope(now); err != nil {
			return inventoryPolicy{}, err
		}
		tokensChanged = true
	} else if refreshExistingScope {
		if err := applyNonHealthyScope(time.Now().Unix()); err != nil {
			return inventoryPolicy{}, err
		}
		tokensChanged = true
	}

	finalSnapshot := snapshot
	if tokensChanged {
		finalSnapshot, err = s.getInventorySnapshot(ctx, tx)
		if err != nil {
			return inventoryPolicy{}, err
		}
	}

	if !runtimeExists ||
		runtimeState.Status != policy.Status ||
		runtimeState.MaxClaims != policy.MaxClaims ||
		runtimeState.Total != finalSnapshot["total"] ||
		runtimeState.Available != finalSnapshot["available"] ||
		runtimeState.Unclaimed != finalSnapshot["unclaimed"] {
		if err := s.upsertInventoryRuntimeState(ctx, tx, finalSnapshot, policy.Status, policy.MaxClaims, now); err != nil {
			return inventoryPolicy{}, err
		}
	}

	return policy, nil
}

func (s *Service) refreshInventoryRuntimeTx(ctx context.Context, tx *sql.Tx) error {
	snapshot, err := s.getInventorySnapshot(ctx, tx)
	if err != nil {
		return err
	}

	policy := s.buildInventoryPolicy(snapshot)
	if err := s.upsertInventoryRuntimeState(ctx, tx, snapshot, policy.Status, policy.MaxClaims, time.Now().Unix()); err != nil {
		return err
	}
	return nil
}

func (s *Service) buildInventoryPolicy(snapshot map[string]int) inventoryPolicy {
	return s.buildInventoryPolicyForStatus(snapshot, "")
}

func (s *Service) buildInventoryPolicyForStatus(snapshot map[string]int, status string) inventoryPolicy {
	resolvedStatus := status
	switch resolvedStatus {
	case "healthy", "warning", "critical":
	default:
		resolvedStatus = s.resolveInventoryStatus(snapshot["unclaimed"])
	}

	hourlyLimit := s.cfg.Inventory.Limits.Healthy.Hourly
	maxClaims := s.cfg.Inventory.Limits.Healthy.MaxClaims
	switch resolvedStatus {
	case "warning":
		hourlyLimit = s.cfg.Inventory.Limits.Warning.Hourly
		maxClaims = s.cfg.Inventory.Limits.Warning.MaxClaims
	case "critical":
		hourlyLimit = s.cfg.Inventory.Limits.Critical.Hourly
		maxClaims = s.cfg.Inventory.Limits.Critical.MaxClaims
	}

	return inventoryPolicy{
		Status:    resolvedStatus,
		Unclaimed: snapshot["unclaimed"],
		Thresholds: map[string]int{
			"healthy":  s.cfg.Inventory.Thresholds.Healthy,
			"warning":  s.cfg.Inventory.Thresholds.Warning,
			"critical": s.cfg.Inventory.Thresholds.Critical,
		},
		HourlyLimit:              hourlyLimit,
		MaxClaims:                maxClaims,
		NonHealthyMaxClaimsScope: s.cfg.Inventory.NonHealthyMaxClaimsScope,
	}
}

func (s *Service) getInventoryPolicy(ctx context.Context) (inventoryPolicy, error) {
	snapshot, runtimeState, runtimeExists, err := s.loadInventorySnapshotForRead(ctx, s.store.DB())
	if err != nil {
		return inventoryPolicy{}, err
	}
	policy := s.buildInventoryPolicy(snapshot)
	if runtimeExists {
		policy = s.buildInventoryPolicyForStatus(snapshot, runtimeState.Status)
	}
	return policy, nil
}

func (s *Service) resolveInventoryStatus(unclaimed int) string {
	if unclaimed < s.cfg.Inventory.Thresholds.Critical {
		return "critical"
	}
	if unclaimed < s.cfg.Inventory.Thresholds.Warning {
		return "warning"
	}
	return "healthy"
}

func (s *Service) getInventorySnapshot(ctx context.Context, queryer sqlQueryer) (map[string]int, error) {
	total, err := queryCount(ctx, queryer, `
		SELECT COUNT(*)
		FROM tokens
		WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0
	`)
	if err != nil {
		return nil, err
	}

	available, err := queryCount(ctx, queryer, `
		SELECT COALESCE(SUM(
			CASE
				WHEN claim_count < max_claims THEN max_claims - claim_count
				ELSE 0
			END
		), 0)
		FROM tokens
		WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0
	`)
	if err != nil {
		return nil, err
	}

	unclaimed, err := queryCount(ctx, queryer, `
		SELECT COUNT(*)
		FROM tokens
		WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0 AND claim_count = 0
	`)
	if err != nil {
		return nil, err
	}

	return map[string]int{
		"total":     total,
		"available": available,
		"unclaimed": unclaimed,
	}, nil
}

func (s *Service) loadInventorySnapshotForRead(ctx context.Context, queryer sqlQueryer) (map[string]int, inventoryRuntimeState, bool, error) {
	runtimeState, runtimeExists, err := s.getInventoryRuntimeState(ctx, queryer)
	if err != nil {
		return nil, inventoryRuntimeState{}, false, err
	}
	if runtimeExists {
		return map[string]int{
			"total":     runtimeState.Total,
			"available": runtimeState.Available,
			"unclaimed": runtimeState.Unclaimed,
		}, runtimeState, true, nil
	}

	snapshot, err := s.getInventorySnapshot(ctx, queryer)
	if err != nil {
		return nil, inventoryRuntimeState{}, false, err
	}
	return snapshot, inventoryRuntimeState{}, false, nil
}

func (s *Service) upsertInventoryRuntimeState(ctx context.Context, queryer sqlQueryer, snapshot map[string]int, status string, maxClaims int, updatedAtTS int64) error {
	if _, err := execContextCounted(ctx, queryer, `
		INSERT INTO inventory_runtime (
			id,
			status,
			total_tokens,
			available_tokens,
			unclaimed_tokens,
			max_claims,
			updated_at_ts
		)
		VALUES (1, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = excluded.status,
			total_tokens = excluded.total_tokens,
			available_tokens = excluded.available_tokens,
			unclaimed_tokens = excluded.unclaimed_tokens,
			max_claims = excluded.max_claims,
			updated_at_ts = excluded.updated_at_ts
	`, status, snapshot["total"], snapshot["available"], snapshot["unclaimed"], maxClaims, updatedAtTS); err != nil {
		return fmt.Errorf("upsert inventory runtime: %w", err)
	}
	return nil
}

func (s *Service) getInventoryRuntimeState(ctx context.Context, queryer sqlQueryer) (inventoryRuntimeState, bool, error) {
	row := queryRowContextCounted(ctx, queryer, `
		SELECT status,
		       total_tokens,
		       available_tokens,
		       unclaimed_tokens,
		       max_claims,
		       updated_at_ts
		FROM inventory_runtime
		WHERE id = 1
	`)

	var state inventoryRuntimeState
	if err := row.Scan(
		&state.Status,
		&state.Total,
		&state.Available,
		&state.Unclaimed,
		&state.MaxClaims,
		&state.UpdatedAtTS,
	); err != nil {
		if err == sql.ErrNoRows {
			return inventoryRuntimeState{}, false, nil
		}
		return inventoryRuntimeState{}, false, fmt.Errorf("load inventory runtime: %w", err)
	}

	return state, true, nil
}

func (s *Service) getClaimStatsRuntimeState(ctx context.Context, queryer sqlQueryer) (claimStatsRuntimeState, bool, error) {
	row := queryRowContextCounted(ctx, queryer, `
		SELECT claimed_total,
		       claimed_unique,
		       updated_at_ts
		FROM claim_stats_runtime
		WHERE id = 1
	`)

	var state claimStatsRuntimeState
	if err := row.Scan(
		&state.ClaimedTotal,
		&state.ClaimedUnique,
		&state.UpdatedAtTS,
	); err != nil {
		if err == sql.ErrNoRows {
			return claimStatsRuntimeState{}, false, nil
		}
		return claimStatsRuntimeState{}, false, fmt.Errorf("load claim stats runtime: %w", err)
	}

	return state, true, nil
}

func (s *Service) getUserClaimStatsRuntimeState(ctx context.Context, queryer sqlQueryer, userID int64) (userClaimStatsRuntimeState, bool, error) {
	row := queryRowContextCounted(ctx, queryer, `
		SELECT user_id,
		       claimed_total,
		       claimed_unique,
		       exclusive_claimed_unique,
		       updated_at_ts
		FROM user_claim_stats_runtime
		WHERE user_id = ?
	`, userID)

	var state userClaimStatsRuntimeState
	if err := row.Scan(
		&state.UserID,
		&state.ClaimedTotal,
		&state.ClaimedUnique,
		&state.ExclusiveClaimedUnique,
		&state.UpdatedAtTS,
	); err != nil {
		if err == sql.ErrNoRows {
			return userClaimStatsRuntimeState{}, false, nil
		}
		return userClaimStatsRuntimeState{}, false, fmt.Errorf("load user claim stats runtime for user %d: %w", userID, err)
	}

	return state, true, nil
}

func (s *Service) tokenClaimStatsBeforeInsert(ctx context.Context, queryer sqlQueryer, tokenID int64) (int, int64, error) {
	rows, err := queryContextCounted(ctx, queryer, `
		SELECT user_id
		FROM user_token_claims
		WHERE token_id = ?
		ORDER BY id ASC
		LIMIT 2
	`, tokenID)
	if err != nil {
		return 0, 0, fmt.Errorf("load token claim stats for token %d: %w", tokenID, err)
	}
	defer rows.Close()

	count := 0
	firstUserID := int64(0)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return 0, 0, fmt.Errorf("scan token claim stats for token %d: %w", tokenID, err)
		}
		count++
		if count == 1 {
			firstUserID = userID
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterate token claim stats for token %d: %w", tokenID, err)
	}

	return count, firstUserID, nil
}

func (s *Service) applyClaimStatsRuntimeTx(ctx context.Context, tx *sql.Tx, userID int64, existingClaimerCount int, previousExclusiveUserID int64) error {
	if userID <= 0 {
		return nil
	}

	uniqueClaimDelta := 0
	exclusiveClaimDelta := 0
	if existingClaimerCount == 0 {
		uniqueClaimDelta = 1
		exclusiveClaimDelta = 1
	}

	now := time.Now().Unix()
	if _, err := execContextCounted(ctx, tx, `
		INSERT INTO claim_stats_runtime (id, claimed_total, claimed_unique, updated_at_ts)
		VALUES (1, 1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			claimed_total = claim_stats_runtime.claimed_total + 1,
			claimed_unique = claim_stats_runtime.claimed_unique + excluded.claimed_unique,
			updated_at_ts = excluded.updated_at_ts
	`, uniqueClaimDelta, now); err != nil {
		return fmt.Errorf("upsert claim stats runtime: %w", err)
	}

	if _, err := execContextCounted(ctx, tx, `
		INSERT INTO user_claim_stats_runtime (
			user_id,
			claimed_total,
			claimed_unique,
			exclusive_claimed_unique,
			updated_at_ts
		)
		VALUES (?, 1, 1, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET
			claimed_total = user_claim_stats_runtime.claimed_total + 1,
			claimed_unique = user_claim_stats_runtime.claimed_unique + 1,
			exclusive_claimed_unique = user_claim_stats_runtime.exclusive_claimed_unique + excluded.exclusive_claimed_unique,
			updated_at_ts = excluded.updated_at_ts
	`, userID, exclusiveClaimDelta, now); err != nil {
		return fmt.Errorf("upsert user claim stats runtime for user %d: %w", userID, err)
	}

	if existingClaimerCount == 1 && previousExclusiveUserID > 0 && previousExclusiveUserID != userID {
		if _, err := execContextCounted(ctx, tx, `
			UPDATE user_claim_stats_runtime
			SET exclusive_claimed_unique = CASE
					WHEN exclusive_claimed_unique > 0 THEN exclusive_claimed_unique - 1
					ELSE 0
				END,
			    updated_at_ts = ?
			WHERE user_id = ?
		`, now, previousExclusiveUserID); err != nil {
			return fmt.Errorf("decrement exclusive claim stats runtime for user %d: %w", previousExclusiveUserID, err)
		}
	}

	return nil
}

func (s *Service) hasPendingQueueTx(ctx context.Context, tx *sql.Tx) (bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM claim_queue
		WHERE status IN ('queued', 'queued_waiting', 'queued_blocked') AND remaining > 0
		LIMIT 1
	`)

	var marker int
	if err := row.Scan(&marker); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("check pending queue: %w", err)
	}

	return true, nil
}

func (s *Service) listQueuedEntries(ctx context.Context) ([]userQueueEntry, error) {
	rows, err := s.store.DB().QueryContext(ctx, `
		SELECT id,
		       user_id,
		       api_key_id,
		       requested,
		       remaining,
		       0,
		       enqueued_at_ts,
		       request_id,
		       status,
		       origin_session_id,
		       origin_tab_id,
		       block_reason,
		       next_retry_at_ts,
		       last_progress_at_ts,
		       terminal_at_ts,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE status IN ('queued', 'queued_waiting', 'queued_blocked') AND remaining > 0
		ORDER BY enqueued_at_ts ASC, id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list queued entries: %w", err)
	}
	defer rows.Close()

	items := make([]userQueueEntry, 0)
	for rows.Next() {
		var item userQueueEntry
		if err := rows.Scan(
			&item.ID,
			&item.UserID,
			&item.APIKeyID,
			&item.Requested,
			&item.Remaining,
			&item.QueueRank,
			&item.EnqueuedAtTS,
			&item.RequestID,
			&item.Status,
			&item.OriginSessionID,
			&item.OriginTabID,
			&item.BlockReason,
			&item.NextRetryAtTS,
			&item.LastProgressAtTS,
			&item.TerminalAtTS,
			&item.CancelReason,
			&item.CancelledAtTS,
			&item.CancelledByUserID,
			&item.LastErrorReason,
			&item.LastErrorAtTS,
			&item.FailureCount,
		); err != nil {
			return nil, fmt.Errorf("scan queued entry: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued entries: %w", err)
	}
	assignSequentialQueueRanks(items)

	return items, nil
}

func (s *Service) loadQueueHead(ctx context.Context) (*userQueueEntry, error) {
	row := queryRowContextCounted(ctx, s.store.DB(), `
		SELECT id,
		       user_id,
		       api_key_id,
		       requested,
		       remaining,
		       1,
		       enqueued_at_ts,
		       request_id,
		       status,
		       origin_session_id,
		       origin_tab_id,
		       block_reason,
		       next_retry_at_ts,
		       last_progress_at_ts,
		       terminal_at_ts,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE status IN ('queued', 'queued_waiting', 'queued_blocked') AND remaining > 0
		ORDER BY enqueued_at_ts ASC, id ASC
		LIMIT 1
	`)
	var item userQueueEntry
	if err := row.Scan(
		&item.ID,
		&item.UserID,
		&item.APIKeyID,
		&item.Requested,
		&item.Remaining,
		&item.QueueRank,
		&item.EnqueuedAtTS,
		&item.RequestID,
		&item.Status,
		&item.OriginSessionID,
		&item.OriginTabID,
		&item.BlockReason,
		&item.NextRetryAtTS,
		&item.LastProgressAtTS,
		&item.TerminalAtTS,
		&item.CancelReason,
		&item.CancelledAtTS,
		&item.CancelledByUserID,
		&item.LastErrorReason,
		&item.LastErrorAtTS,
		&item.FailureCount,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load queue head: %w", err)
	}
	return &item, nil
}

func (s *Service) getUserQueueEntry(ctx context.Context, userID int64) (*userQueueEntry, error) {
	return s.getUserQueueEntryFrom(ctx, s.store.DB(), userID)
}

func (s *Service) getUserQueueEntryTx(ctx context.Context, tx *sql.Tx, userID int64) (*userQueueEntry, error) {
	return s.getUserQueueEntryFrom(ctx, tx, userID)
}

func (s *Service) loadClaimRequestEntry(ctx context.Context, userID int64, requestID string) (*userQueueEntry, error) {
	return s.loadClaimRequestEntryFrom(ctx, s.store.DB(), userID, requestID)
}

func (s *Service) loadClaimRequestEntryFrom(ctx context.Context, queryer sqlQueryer, userID int64, requestID string) (*userQueueEntry, error) {
	trimmedRequestID := strings.TrimSpace(requestID)
	if userID <= 0 || trimmedRequestID == "" {
		return nil, nil
	}

	row := queryRowContextCounted(ctx, queryer, `
		SELECT id,
		       user_id,
		       api_key_id,
		       requested,
		       remaining,
		       0,
		       enqueued_at_ts,
		       request_id,
		       status,
		       origin_session_id,
		       origin_tab_id,
		       block_reason,
		       next_retry_at_ts,
		       last_progress_at_ts,
		       terminal_at_ts,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE user_id = ? AND request_id = ?
		ORDER BY id DESC
		LIMIT 1
	`, userID, trimmedRequestID)

	var item userQueueEntry
	if err := row.Scan(
		&item.ID,
		&item.UserID,
		&item.APIKeyID,
		&item.Requested,
		&item.Remaining,
		&item.QueueRank,
		&item.EnqueuedAtTS,
		&item.RequestID,
		&item.Status,
		&item.OriginSessionID,
		&item.OriginTabID,
		&item.BlockReason,
		&item.NextRetryAtTS,
		&item.LastProgressAtTS,
		&item.TerminalAtTS,
		&item.CancelReason,
		&item.CancelledAtTS,
		&item.CancelledByUserID,
		&item.LastErrorReason,
		&item.LastErrorAtTS,
		&item.FailureCount,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load claim request %q for user %d: %w", trimmedRequestID, userID, err)
	}
	if isQueueActiveStatus(item.Status) && item.Remaining > 0 {
		position, err := s.lookupQueuePosition(ctx, queryer, item)
		if err != nil {
			return nil, err
		}
		item.QueueRank = position
	}
	return &item, nil
}

func (s *Service) getUserQueueEntryFrom(ctx context.Context, queryer sqlQueryer, userID int64) (*userQueueEntry, error) {
	row := queryRowContextCounted(ctx, queryer, `
		SELECT id,
		       user_id,
		       api_key_id,
		       requested,
		       remaining,
		       0,
		       enqueued_at_ts,
		       request_id,
		       status,
		       origin_session_id,
		       origin_tab_id,
		       block_reason,
		       next_retry_at_ts,
		       last_progress_at_ts,
		       terminal_at_ts,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE user_id = ? AND status IN ('queued', 'queued_waiting', 'queued_blocked') AND remaining > 0
		ORDER BY enqueued_at_ts ASC, id ASC
		LIMIT 1
	`, userID)

	var item userQueueEntry
	if err := row.Scan(
		&item.ID,
		&item.UserID,
		&item.APIKeyID,
		&item.Requested,
		&item.Remaining,
		&item.QueueRank,
		&item.EnqueuedAtTS,
		&item.RequestID,
		&item.Status,
		&item.OriginSessionID,
		&item.OriginTabID,
		&item.BlockReason,
		&item.NextRetryAtTS,
		&item.LastProgressAtTS,
		&item.TerminalAtTS,
		&item.CancelReason,
		&item.CancelledAtTS,
		&item.CancelledByUserID,
		&item.LastErrorReason,
		&item.LastErrorAtTS,
		&item.FailureCount,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load queue entry for user %d: %w", userID, err)
	}
	position, err := s.lookupQueuePosition(ctx, queryer, item)
	if err != nil {
		return nil, err
	}
	item.QueueRank = position

	return &item, nil
}

func (s *Service) getTotalQueued(ctx context.Context, queryer sqlQueryer) (int, error) {
	row := queryRowContextCounted(ctx, queryer, `
		SELECT total_queued
		FROM queue_runtime
		WHERE id = 1
	`)

	var totalQueued int
	if err := row.Scan(&totalQueued); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("load queue runtime: %w", err)
	}
	return totalQueued, nil
}

func (s *Service) remainingHourlyQuota(ctx context.Context, userID int64, limit int) (int, error) {
	return s.remainingHourlyQuotaQueryer(ctx, s.store.DB(), userID, limit)
}

func (s *Service) remainingMinuteQuota(ctx context.Context, apiKeyID int64, limit int) (int, error) {
	return s.remainingMinuteQuotaQueryer(ctx, s.store.DB(), apiKeyID, limit)
}

func (s *Service) remainingHourlyQuotaQueryer(ctx context.Context, queryer sqlQueryer, userID int64, limit int) (int, error) {
	used, err := s.countUserClaimsSince(ctx, queryer, userID, time.Now().Unix()-3600)
	if err != nil {
		return 0, err
	}
	return maxInt(0, limit-used), nil
}

func (s *Service) remainingMinuteQuotaQueryer(ctx context.Context, queryer sqlQueryer, apiKeyID int64, limit int) (int, error) {
	used, err := s.countAPIKeyClaimsSince(ctx, queryer, apiKeyID, time.Now().Unix()-60)
	if err != nil {
		return 0, err
	}
	return maxInt(0, limit-used), nil
}

func (s *Service) countUserClaimsSince(ctx context.Context, queryer sqlQueryer, userID int64, cutoff int64) (int, error) {
	return queryCount(ctx, queryer, `
		SELECT COUNT(*)
		FROM token_claims
		WHERE user_id = ? AND claimed_at_ts >= ?
	`, userID, cutoff)
}

func (s *Service) countAPIKeyClaimsSince(ctx context.Context, queryer sqlQueryer, apiKeyID int64, cutoff int64) (int, error) {
	return queryCount(ctx, queryer, `
		SELECT COUNT(*)
		FROM token_claims
		WHERE api_key_id = ? AND claimed_at_ts >= ?
	`, apiKeyID, cutoff)
}

func (s *Service) countClaimableTokensForUser(ctx context.Context, queryer sqlQueryer, userID int64) (int, error) {
	return queryCount(ctx, queryer, `
		SELECT COUNT(*)
		FROM tokens
		WHERE is_active = 1
		  AND is_enabled = 1
		  AND is_banned = 0
		  AND is_available = 1
		  AND claim_count < max_claims
		  AND (probe_lock_until_ts IS NULL OR probe_lock_until_ts < ?)
		  AND NOT EXISTS (
			  SELECT 1
			  FROM user_token_claims
			  WHERE user_token_claims.user_id = ?
				AND user_token_claims.token_id = tokens.id
		  )
	`, time.Now().Unix(), userID)
}

func (s *Service) countPotentialClaimableTokensForUser(ctx context.Context, queryer sqlQueryer, userID int64) (int, error) {
	return queryCount(ctx, queryer, `
		SELECT COUNT(*)
		FROM tokens
		WHERE is_active = 1
		  AND is_enabled = 1
		  AND is_banned = 0
		  AND is_available = 1
		  AND claim_count < max_claims
		  AND NOT EXISTS (
			  SELECT 1
			  FROM user_token_claims
			  WHERE user_token_claims.user_id = ?
				AND user_token_claims.token_id = tokens.id
		  )
	`, userID)
}

func (s *Service) countPotentialClaimableTokens(ctx context.Context, queryer sqlQueryer) (int, error) {
	return queryCount(ctx, queryer, `
		SELECT COUNT(*)
		FROM tokens
		WHERE is_active = 1
		  AND is_enabled = 1
		  AND is_banned = 0
		  AND is_available = 1
		  AND claim_count < max_claims
	`)
}

func sqlNullInt64(value *int64) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *value, Valid: true}
}

func isoformatFromNullableTS(value sql.NullInt64) string {
	if !value.Valid || value.Int64 <= 0 {
		return ""
	}
	return isoformatFromTS(value.Int64)
}

func dbWriteLock(db *sql.DB) *sync.Mutex {
	if db == nil {
		return &sync.Mutex{}
	}
	if existing, ok := dbWriteLocks.Load(db); ok {
		return existing.(*sync.Mutex)
	}
	created := &sync.Mutex{}
	actual, _ := dbWriteLocks.LoadOrStore(db, created)
	return actual.(*sync.Mutex)
}

func withSerializedDBWrite[T any](db *sql.DB, fn func() (T, error)) (T, error) {
	lock := dbWriteLock(db)
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

func execWriteContext(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	return runWithDatabaseBusyRetry(ctx, func() (sql.Result, error) {
		return withSerializedDBWrite(db, func() (sql.Result, error) {
			return db.ExecContext(ctx, query, args...)
		})
	})
}

func withTx[T any](ctx context.Context, db *sql.DB, fn func(*sql.Tx) (T, error)) (T, error) {
	return runWithDatabaseBusyRetry(ctx, func() (T, error) {
		return withSerializedDBWrite(db, func() (T, error) {
			var zero T

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return zero, fmt.Errorf("begin transaction: %w", err)
			}
			defer func() {
				_ = tx.Rollback()
			}()

			value, err := fn(tx)
			if err != nil {
				return zero, err
			}

			if err := tx.Commit(); err != nil {
				return zero, fmt.Errorf("commit transaction: %w", err)
			}

			return value, nil
		})
	})
}

func queryCount(ctx context.Context, queryer sqlQueryer, query string, args ...any) (int, error) {
	var count int
	if err := queryRowContextCounted(ctx, queryer, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("query count: %w", err)
	}
	return count, nil
}

func decodeJSONContent(raw string) (any, error) {
	var payload any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("decode json content: %w", err)
	}
	return payload, nil
}

func buildDefaultUploadResultsPayload() map[string]any {
	return map[string]any{
		"batch_id":     nil,
		"created_at":   nil,
		"summary":      buildDefaultUploadResultsSummary(),
		"items":        []map[string]any{},
		"history":      []map[string]any{},
		"queue_status": nil,
	}
}

func buildDefaultUploadResultsSummary() map[string]int {
	return map[string]int{
		"total":      0,
		"accepted":   0,
		"duplicates": 0,
		"invalid":    0,
		"rejected":   0,
		"db_busy":    0,
		"queued":     0,
		"processing": 0,
	}
}

func randomRequestID() (string, error) {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func parseWindowSeconds(value string, fallback int, maxSeconds int) int {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return fallback
	}

	for _, unit := range []struct {
		suffix string
		scale  int
	}{
		{"d", 24 * 3600},
		{"h", 3600},
		{"m", 60},
		{"s", 1},
	} {
		if !strings.HasSuffix(trimmed, unit.suffix) {
			continue
		}

		numberPart := strings.TrimSpace(strings.TrimSuffix(trimmed, unit.suffix))
		parsed, err := strconv.Atoi(numberPart)
		if err != nil || parsed <= 0 {
			return fallback
		}

		result := parsed * unit.scale
		if maxSeconds > 0 && result > maxSeconds {
			return maxSeconds
		}
		return result
	}

	parsed, err := strconv.Atoi(trimmed)
	if err != nil || parsed <= 0 {
		return fallback
	}
	if maxSeconds > 0 && parsed > maxSeconds {
		return maxSeconds
	}
	return parsed
}

func parseBucketSeconds(value string, fallback int) int {
	return parseWindowSeconds(value, fallback, 7*24*3600)
}

func isUniqueConstraintError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint failed") || strings.Contains(message, "constraint failed")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Service) buildDownloadURL(c echo.Context, tokenID int64) string {
	path := "/api/download/" + strconv.FormatInt(tokenID, 10)
	if baseURL := strings.TrimRight(strings.TrimSpace(s.cfg.Server.ProviderBaseURL), "/"); baseURL != "" {
		return baseURL + path
	}

	forwardedProto := strings.TrimSpace(strings.SplitN(c.Request().Header.Get("X-Forwarded-Proto"), ",", 2)[0])
	forwardedHost := strings.TrimSpace(strings.SplitN(c.Request().Header.Get("X-Forwarded-Host"), ",", 2)[0])
	if forwardedProto != "" && forwardedHost != "" {
		return forwardedProto + "://" + forwardedHost + path
	}

	scheme := strings.TrimSpace(c.Scheme())
	host := strings.TrimSpace(c.Request().Host)
	if scheme != "" && host != "" {
		return scheme + "://" + host + path
	}
	return path
}

func isoformatNow() string {
	return time.Now().In(time.Local).Format(time.RFC3339)
}

func isoformatFromTS(value int64) string {
	return time.Unix(value, 0).In(time.Local).Format(time.RFC3339)
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
