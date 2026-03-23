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

const queuePumpInterval = 2 * time.Second

var errRetryAllocation = errors.New("retry allocation")

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
	probe  claimProbe
	wakeCh chan struct{}

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

	queueEvents     *queueStatusEventBroker
	claimEvents     *claimRealtimeEventBroker
	uploadEvents    *uploadResultsEventBroker
	hideClaimsCh    chan hideClaimsTask
	uploadTaskCh    chan uploadTask
	uploadMu        sync.Mutex
	uploadSnapshots map[int64]uploadSnapshot
	uploadPending   map[string]*uploadPendingRecord
	uploadQueued    int

	tokenImportCh       chan tokenImportRequest
	tokenImportDoneCh   chan tokenImportResult
	tokenImportMu       sync.Mutex
	tokenImportPending  map[string]int
	tokenInternalWrites map[string]time.Time

	tokenDeleteMu      sync.Mutex
	tokenDeletePending map[string]struct{}
	removeTokenFile    func(string) error
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
	QueueID        int64               `json:"queue_id,omitempty"`
	QueuePosition  int                 `json:"queue_position,omitempty"`
	QueueRemaining int                 `json:"queue_remaining,omitempty"`
	Quota          quotaUsage          `json:"quota"`
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
	Queued          bool   `json:"queued"`
	QueueID         int64  `json:"queue_id,omitempty"`
	Position        int    `json:"position,omitempty"`
	Ahead           int    `json:"ahead,omitempty"`
	Requested       int    `json:"requested,omitempty"`
	Remaining       int    `json:"remaining,omitempty"`
	EnqueuedAt      string `json:"enqueued_at,omitempty"`
	RequestID       string `json:"request_id,omitempty"`
	TotalQueued     int    `json:"total_queued"`
	AvailableTokens int    `json:"available_tokens"`
	ClaimableNow    int    `json:"claimable_now"`
	Degraded        bool   `json:"degraded,omitempty"`
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
	User          *profileUserPayload             `json:"user,omitempty"`
	Quota         quotaUsage                      `json:"quota"`
	Claims        map[string]int                  `json:"claims"`
	APIKeys       map[string]apiKeySummaryPayload `json:"api_keys"`
	Uploads       map[string]int                  `json:"uploads,omitempty"`
	UploadResults map[string]any                  `json:"upload_results"`
	Degraded      bool                            `json:"degraded,omitempty"`
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
		probe:                proberuntime.New(cfg.Probe.DelaySec, cfg.Probe.TimeoutSec),
		wakeCh:               make(chan struct{}, 1),
		systemIndexUpdatedAt: isoformatNow(),
		queueEvents:          newQueueStatusEventBroker(),
		claimEvents:          newClaimRealtimeEventBroker(),
		uploadEvents:         newUploadResultsEventBroker(),
		hideClaimsCh:         make(chan hideClaimsTask, 256),
		uploadTaskCh:         make(chan uploadTask, 128),
		uploadSnapshots:      make(map[int64]uploadSnapshot),
		uploadPending:        make(map[string]*uploadPendingRecord),
		startupInProgress:    true,
		startupReadyCh:       make(chan struct{}),
		tokenImportCh:        make(chan tokenImportRequest, 512),
		tokenImportDoneCh:    make(chan tokenImportResult, 512),
		tokenImportPending:   make(map[string]int),
		tokenInternalWrites:  make(map[string]time.Time),
		tokenDeletePending:   make(map[string]struct{}),
		removeTokenFile:      os.Remove,
	}
}

func (s *Service) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		s.setLifecycleContext(ctx)
		s.probe.Start()
		go func() {
			<-ctx.Done()
			s.probe.Stop()
		}()

		ticker := time.NewTicker(queuePumpInterval)
		s.goWorker(func() {
			defer ticker.Stop()
			if !s.waitForStartupReady(ctx) {
				return
			}
			s.safeAdvanceQueue(ctx)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				case <-s.wakeCh:
				}
				s.safeAdvanceQueue(ctx)
			}
		})

		s.goWorker(func() { s.uploadWorkerLoop(ctx) })
		s.goWorker(func() { s.hideClaimsWorkerLoop(ctx) })
		s.goWorker(func() { s.tokenImportLoop(ctx) })
		s.goWorker(func() { s.tokenWatchLoop(ctx) })
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
	default:
	}
}

func (s *Service) ClaimTokens(ctx context.Context, userID int64, apiKeyID *int64, count int) (*claimResult, error) {
	requested := maxInt(1, count)
	now := time.Now().Unix()

	type claimBootstrap struct {
		Target      int
		Used        int
		Limit       int
		Remaining   int
		APIKeyLimit int
		Queued      *claimResult
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

		hasPending, err := s.hasPendingQueueTx(ctx, tx)
		if err != nil {
			return claimBootstrap{}, err
		}
		if hasPending {
			queueItem, _, err := s.enqueueClaimTx(ctx, tx, userID, apiKeyID, target)
			if err != nil {
				return claimBootstrap{}, err
			}

			return claimBootstrap{
				Used:        used,
				Limit:       policy.HourlyLimit,
				Remaining:   remaining,
				APIKeyLimit: apiKeyLimit,
				Queued: &claimResult{
					RequestID:      queueItem.RequestID,
					Items:          []claimResponseItem{},
					Requested:      target,
					Granted:        0,
					Queued:         true,
					QueueID:        queueItem.ID,
					QueuePosition:  queueItem.QueueRank,
					QueueRemaining: queueItem.Remaining,
					Quota: quotaUsage{
						Used:      used,
						Limit:     policy.HourlyLimit,
						Remaining: remaining,
					},
				},
			}, nil
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
		queued, err := s.enqueueClaim(ctx, userID, apiKeyID, initial.Target, initial.Used, initial.Limit, initial.Remaining)
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
	s.invalidateUserQuotaCache(userID)
	s.invalidateUserClaimsCache(userID)
	s.invalidateInventoryCache()
	s.invalidateDashboardClaimCaches()
	s.invalidateAdminCache()
	s.notifyQueueUsers(ctx, userID)
	return result, nil
}

func (s *Service) AdvanceQueue(ctx context.Context) error {
	s.advanceMu.Lock()
	defer s.advanceMu.Unlock()

	policy, err := s.ensureInventoryPolicy(ctx, true)
	if err != nil {
		return err
	}

	queueRows, err := s.listQueuedEntries(ctx)
	if err != nil {
		return err
	}
	if len(queueRows) == 0 {
		return nil
	}

	affectedUsers := make(map[int64]struct{}, len(queueRows))
	claimedUsers := make(map[int64]struct{}, len(queueRows))
	for _, row := range queueRows {
		result, err := s.advanceQueueRow(ctx, row, policy)
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
			failureResult, failureErr := s.recordQueueFailure(ctx, row, err.Error())
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
				continue
			}
			if failureResult.Changed {
				affectedUsers[row.UserID] = struct{}{}
			}
			if failureResult.Cancelled {
				if publishErr := s.publishQueueTerminalState(ctx, row, claimStatusCancelled, queueCancelReasonFailureThreshold); publishErr != nil {
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
			}
			continue
		}
		if result.Changed {
			affectedUsers[row.UserID] = struct{}{}
			if result.Claimed {
				claimedUsers[row.UserID] = struct{}{}
			}
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
		total, err := queryCount(ctx, s.store.DB(), `SELECT COUNT(*) FROM token_claims WHERE user_id = ?`, userID)
		if err != nil {
			return nil, err
		}

		unique, err := queryCount(ctx, s.store.DB(), `SELECT COUNT(DISTINCT token_id) FROM token_claims WHERE user_id = ?`, userID)
		if err != nil {
			return nil, err
		}

		return map[string]int{
			"total":  total,
			"unique": unique,
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
	observedCtx, _ := withSQLMetrics(ctx)
	var (
		quotaDuration   time.Duration
		claimsDuration  time.Duration
		apiKeysDuration time.Duration
		loaderExecuted  bool
	)

	payload, err := runtimecache.CacheJSON(
		s.cache,
		s.userProfileCacheKey(requestContext.UserID, requestContext.IsAdmin),
		s.cfg.Cache.MeTTL,
		func() (profilePayload, error) {
			loaderExecuted = true

			stageStartedAt := time.Now()
			quota, err := s.GetQuotaUsage(observedCtx, requestContext.UserID)
			quotaDuration = time.Since(stageStartedAt)
			if err != nil {
				return profilePayload{}, err
			}

			stageStartedAt = time.Now()
			claims, err := s.GetUserClaimTotals(observedCtx, requestContext.UserID)
			claimsDuration = time.Since(stageStartedAt)
			if err != nil {
				return profilePayload{}, err
			}

			stageStartedAt = time.Now()
			apiKeys, err := s.GetAPIKeySummary(observedCtx, requestContext.UserID)
			apiKeysDuration = time.Since(stageStartedAt)
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

	s.logger.Info(
		"get profile",
		"user_id", requestContext.UserID,
		"cache_hit", !loaderExecuted,
		"total_ms", durationMillis(time.Since(startedAt)),
		"quota_ms", durationMillis(quotaDuration),
		"claims_ms", durationMillis(claimsDuration),
		"api_keys_ms", durationMillis(apiKeysDuration),
		"sql_count", sqlCount(observedCtx),
		"error", err,
	)
	return payload, err
}

func (s *Service) GetRuntimeSnapshot(ctx context.Context, userID int64) (runtimeSnapshotPayload, error) {
	startedAt := time.Now()
	observedCtx, _ := withSQLMetrics(ctx)
	var (
		quotaDuration         time.Duration
		claimsDuration        time.Duration
		apiKeysDuration       time.Duration
		uploadResultsDuration time.Duration
		loaderExecuted        bool
	)

	payload, err := runtimecache.CacheJSON(
		s.cache,
		s.userRuntimeSnapshotCacheKey(userID),
		s.cfg.Cache.MeTTL,
		func() (runtimeSnapshotPayload, error) {
			loaderExecuted = true

			stageStartedAt := time.Now()
			quota, err := s.GetQuotaUsage(observedCtx, userID)
			quotaDuration = time.Since(stageStartedAt)
			if err != nil {
				return runtimeSnapshotPayload{}, err
			}

			stageStartedAt = time.Now()
			claims, err := s.GetUserClaimTotals(observedCtx, userID)
			claimsDuration = time.Since(stageStartedAt)
			if err != nil {
				return runtimeSnapshotPayload{}, err
			}

			stageStartedAt = time.Now()
			apiKeys, err := s.GetAPIKeySummary(observedCtx, userID)
			apiKeysDuration = time.Since(stageStartedAt)
			if err != nil {
				return runtimeSnapshotPayload{}, err
			}

			stageStartedAt = time.Now()
			uploadResults := buildUploadResultsSummaryPayload(s.GetUploadResults(userID))
			uploadResultsDuration = time.Since(stageStartedAt)

			return runtimeSnapshotPayload{
				Quota:  quota,
				Claims: claims,
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

	s.logger.Info(
		"get runtime snapshot",
		"user_id", userID,
		"cache_hit", !loaderExecuted,
		"total_ms", durationMillis(time.Since(startedAt)),
		"quota_ms", durationMillis(quotaDuration),
		"claims_ms", durationMillis(claimsDuration),
		"api_keys_ms", durationMillis(apiKeysDuration),
		"upload_results_ms", durationMillis(uploadResultsDuration),
		"sql_count", sqlCount(observedCtx),
		"error", err,
	)
	return payload, err
}

func (s *Service) GetQueueStatus(ctx context.Context, userID int64) (queueStatusPayload, error) {
	startedAt := time.Now()
	observedCtx, _ := withSQLMetrics(ctx)
	if payload, ok := s.getCachedQueueStatus(userID); ok {
		s.logger.Info(
			"get queue status",
			"user_id", userID,
			"cache_hit", true,
			"total_ms", durationMillis(time.Since(startedAt)),
			"entry_ms", int64(0),
			"queue_total_ms", int64(0),
			"inventory_ms", int64(0),
			"claimable_ms", int64(0),
			"sql_count", sqlCount(observedCtx),
		)
		return payload, nil
	}

	var (
		entryDuration      time.Duration
		queueTotalDuration time.Duration
		inventoryDuration  time.Duration
		claimableDuration  time.Duration
	)

	payload, err := s.loadQueueStatusSnapshotWithTimings(
		observedCtx,
		userID,
		&entryDuration,
		&queueTotalDuration,
		&inventoryDuration,
		&claimableDuration,
	)
	if err != nil {
		s.logger.Info(
			"get queue status",
			"user_id", userID,
			"cache_hit", false,
			"total_ms", durationMillis(time.Since(startedAt)),
			"entry_ms", durationMillis(entryDuration),
			"queue_total_ms", durationMillis(queueTotalDuration),
			"inventory_ms", durationMillis(inventoryDuration),
			"claimable_ms", durationMillis(claimableDuration),
			"sql_count", sqlCount(observedCtx),
			"error", err,
		)
		return queueStatusPayload{}, err
	}

	payload = s.setQueueStatusSnapshot(userID, payload)
	s.logger.Info(
		"get queue status",
		"user_id", userID,
		"cache_hit", false,
		"total_ms", durationMillis(time.Since(startedAt)),
		"entry_ms", durationMillis(entryDuration),
		"queue_total_ms", durationMillis(queueTotalDuration),
		"inventory_ms", durationMillis(inventoryDuration),
		"claimable_ms", durationMillis(claimableDuration),
		"sql_count", sqlCount(observedCtx),
	)
	return payload, nil
}

func (s *Service) loadQueueStatusSnapshot(ctx context.Context, userID int64) (queueStatusPayload, error) {
	return s.loadQueueStatusSnapshotWithTimings(ctx, userID, nil, nil, nil, nil)
}

func (s *Service) loadQueueStatusSnapshotWithTimings(
	ctx context.Context,
	userID int64,
	entryDuration *time.Duration,
	queueTotalDuration *time.Duration,
	inventoryDuration *time.Duration,
	claimableDuration *time.Duration,
) (queueStatusPayload, error) {
	stageStartedAt := time.Now()
	entry, err := s.getUserQueueEntry(ctx, userID)
	if entryDuration != nil {
		*entryDuration = time.Since(stageStartedAt)
	}
	if err != nil {
		return queueStatusPayload{}, err
	}

	stageStartedAt = time.Now()
	totalQueued, err := s.getTotalQueued(ctx, s.store.DB())
	if queueTotalDuration != nil {
		*queueTotalDuration = time.Since(stageStartedAt)
	}
	if err != nil {
		return queueStatusPayload{}, err
	}

	stageStartedAt = time.Now()
	runtimeState, runtimeExists, err := s.getInventoryRuntimeState(ctx, s.store.DB())
	if inventoryDuration != nil {
		*inventoryDuration = time.Since(stageStartedAt)
	}
	if err != nil {
		return queueStatusPayload{}, err
	}

	availableTokens := 0
	if runtimeExists {
		availableTokens = runtimeState.Available
	} else {
		stageStartedAt = time.Now()
		snapshot, snapshotErr := s.getInventorySnapshot(ctx, s.store.DB())
		if inventoryDuration != nil {
			*inventoryDuration += time.Since(stageStartedAt)
		}
		if snapshotErr != nil {
			return queueStatusPayload{}, snapshotErr
		}
		availableTokens = snapshot["available"]
	}

	stageStartedAt = time.Now()
	claimableNow, err := s.countClaimableTokensForUser(ctx, s.store.DB(), userID)
	if claimableDuration != nil {
		*claimableDuration = time.Since(stageStartedAt)
	}
	if err != nil {
		return queueStatusPayload{}, err
	}

	return buildQueueStatusPayload(entry, totalQueued, availableTokens, claimableNow), nil
}

func buildQueueStatusPayload(entry *userQueueEntry, totalQueued int, availableTokens int, claimableNow int) queueStatusPayload {
	if entry == nil {
		return queueStatusPayload{
			Queued:          false,
			TotalQueued:     totalQueued,
			AvailableTokens: availableTokens,
			ClaimableNow:    claimableNow,
		}
	}

	return queueStatusPayload{
		Queued:          true,
		QueueID:         entry.ID,
		Position:        entry.QueueRank,
		Ahead:           maxInt(0, entry.QueueRank-1),
		Requested:       entry.Requested,
		Remaining:       entry.Remaining,
		EnqueuedAt:      isoformatFromTS(entry.EnqueuedAtTS),
		RequestID:       entry.RequestID,
		TotalQueued:     totalQueued,
		AvailableTokens: availableTokens,
		ClaimableNow:    claimableNow,
	}
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

	if _, err := s.store.DB().ExecContext(ctx, query, args...); err != nil {
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

func (s *Service) getSystemStatus(ctx context.Context) (map[string]any, error) {
	return runtimecache.CacheJSON(s.cache, s.dashboardCacheKey("dashboard-system", nil), s.cfg.Cache.DashboardTTL, func() (map[string]any, error) {
		snapshot, runtimeState, runtimeExists, err := s.loadInventorySnapshotForRead(ctx, s.store.DB())
		if err != nil {
			return nil, err
		}

		policy := s.buildInventoryPolicy(snapshot)
		if runtimeExists {
			policy.Status = runtimeState.Status
			policy.MaxClaims = runtimeState.MaxClaims
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
	})
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

func (s *Service) getDashboardStats(ctx context.Context, userID int64) (map[string]int, error) {
	return runtimecache.CacheJSON(s.cache, s.dashboardCacheKey("dashboard-stats", &userID), s.cfg.Cache.DashboardTTL, func() (map[string]int, error) {
		snapshot, _, _, err := s.loadInventorySnapshotForRead(ctx, s.store.DB())
		if err != nil {
			return nil, err
		}

		claimedTotal, err := queryCount(ctx, s.store.DB(), `SELECT COUNT(*) FROM token_claims`)
		if err != nil {
			return nil, err
		}

		othersClaimedTotal, err := queryCount(ctx, s.store.DB(), `SELECT COUNT(*) FROM token_claims WHERE user_id != ?`, userID)
		if err != nil {
			return nil, err
		}

		othersClaimedUnique, err := queryCount(ctx, s.store.DB(), `SELECT COUNT(DISTINCT token_id) FROM token_claims WHERE user_id != ?`, userID)
		if err != nil {
			return nil, err
		}

		return map[string]int{
			"total_tokens":          snapshot["total"],
			"available_tokens":      snapshot["available"],
			"claimed_total":         claimedTotal,
			"claimed_unique":        maxInt(0, snapshot["total"]-snapshot["unclaimed"]),
			"others_claimed_total":  othersClaimedTotal,
			"others_claimed_unique": othersClaimedUnique,
		}, nil
	})
}

func (s *Service) enqueueClaim(ctx context.Context, userID int64, apiKeyID *int64, target int, used int, limit int, remaining int) (*claimResult, error) {
	type queueInsertResult struct {
		Item *userQueueEntry
	}

	inserted, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (queueInsertResult, error) {
		item, _, err := s.enqueueClaimTx(ctx, tx, userID, apiKeyID, target)
		if err != nil {
			return queueInsertResult{}, err
		}
		return queueInsertResult{Item: item}, nil
	})
	if err != nil {
		return nil, err
	}

	return &claimResult{
		RequestID:      inserted.Item.RequestID,
		Items:          []claimResponseItem{},
		Requested:      target,
		Granted:        0,
		Queued:         true,
		QueueID:        inserted.Item.ID,
		QueuePosition:  inserted.Item.QueueRank,
		QueueRemaining: inserted.Item.Remaining,
		Quota: quotaUsage{
			Used:      used,
			Limit:     limit,
			Remaining: remaining,
		},
	}, nil
}

func (s *Service) enqueueClaimTx(ctx context.Context, tx *sql.Tx, userID int64, apiKeyID *int64, requested int) (*userQueueEntry, bool, error) {
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

	result, err := tx.ExecContext(ctx, `
		INSERT INTO claim_queue (
			user_id, api_key_id, requested, remaining, queue_rank, enqueued_at_ts, request_id, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
	`, userID, apiKeyArg, requested, requested, totalQueued+1, now, requestID)
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
	}, false, nil
}

func (s *Service) consumeQueueGrant(ctx context.Context, queueID int64, granted int) (bool, error) {
	if granted <= 0 {
		return false, nil
	}

	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (bool, error) {
		row := tx.QueryRowContext(ctx, `
			SELECT remaining
			FROM claim_queue
			WHERE id = ? AND status = 'queued'
		`, queueID)

		var remaining int
		if err := row.Scan(&remaining); err != nil {
			if err == sql.ErrNoRows {
				return false, nil
			}
			return false, fmt.Errorf("load queue row %d: %w", queueID, err)
		}

		remainingAfter := maxInt(0, remaining-granted)
		if remainingAfter <= 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM claim_queue WHERE id = ?`, queueID); err != nil {
				return false, fmt.Errorf("delete queue row %d: %w", queueID, err)
			}
			if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
				return false, err
			}
		} else {
			if _, err := tx.ExecContext(ctx, `
				UPDATE claim_queue
				SET remaining = ?,
				    status = 'queued',
				    cancel_reason = NULL,
				    cancelled_at_ts = NULL,
				    cancelled_by_user_id = NULL,
				    last_error_reason = NULL,
				    last_error_at_ts = NULL,
				    failure_count = 0
				WHERE id = ?
			`, remainingAfter, queueID); err != nil {
				return false, fmt.Errorf("update queue remaining: %w", err)
			}
			if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
				return false, err
			}
		}

		return remainingAfter <= 0, nil
	})
}

func (s *Service) allocateClaimableToken(ctx context.Context, userID int64, apiKeyID *int64, requestID string, hourlyLimit int, apiKeyLimit int) (*claimAllocation, error) {
	waitTimeoutSec := s.cfg.Probe.TimeoutSec + s.cfg.Probe.DelaySec + 5.0
	for attempt := 0; attempt < 16; attempt++ {
		candidate, err := s.reserveClaimableTokenForUser(ctx, userID)
		if err != nil {
			return nil, err
		}
		if candidate == nil {
			return nil, nil
		}

		probeResult := s.probe.Submit(candidate.Content, waitTimeoutSec)
		if probeResult.IsBanned() {
			if err := s.markTokenBanned(ctx, candidate.TokenID, "upstream_401"); err != nil {
				return nil, err
			}
			continue
		}
		if probeResult.Status != "ok" {
			if err := s.recordProbeStatus(ctx, candidate.TokenID, probeResult.Status, false); err != nil {
				return nil, err
			}
		}

		item, err := s.finalizeClaimReservedToken(ctx, candidate.TokenID, userID, apiKeyID, requestID, hourlyLimit, apiKeyLimit)
		if err != nil {
			return nil, err
		}
		if item != nil {
			return item, nil
		}
	}
	return nil, nil
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
	status := s.resolveInventoryStatus(snapshot["unclaimed"])
	hourlyLimit := s.cfg.Inventory.Limits.Healthy.Hourly
	maxClaims := s.cfg.Inventory.Limits.Healthy.MaxClaims
	switch status {
	case "warning":
		hourlyLimit = s.cfg.Inventory.Limits.Warning.Hourly
		maxClaims = s.cfg.Inventory.Limits.Warning.MaxClaims
	case "critical":
		hourlyLimit = s.cfg.Inventory.Limits.Critical.Hourly
		maxClaims = s.cfg.Inventory.Limits.Critical.MaxClaims
	}

	return inventoryPolicy{
		Status:    status,
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
		policy.Status = runtimeState.Status
		policy.MaxClaims = runtimeState.MaxClaims
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

func (s *Service) hasPendingQueueTx(ctx context.Context, tx *sql.Tx) (bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM claim_queue
		WHERE status = 'queued' AND remaining > 0
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
		       queue_rank,
		       enqueued_at_ts,
		       request_id,
		       status,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE status = 'queued' AND remaining > 0
		ORDER BY queue_rank ASC, id ASC
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

	return items, nil
}

func (s *Service) getUserQueueEntry(ctx context.Context, userID int64) (*userQueueEntry, error) {
	return s.getUserQueueEntryFrom(ctx, s.store.DB(), userID)
}

func (s *Service) getUserQueueEntryTx(ctx context.Context, tx *sql.Tx, userID int64) (*userQueueEntry, error) {
	return s.getUserQueueEntryFrom(ctx, tx, userID)
}

func (s *Service) getUserQueueEntryFrom(ctx context.Context, queryer sqlQueryer, userID int64) (*userQueueEntry, error) {
	row := queryRowContextCounted(ctx, queryer, `
		SELECT id,
		       user_id,
		       api_key_id,
		       requested,
		       remaining,
		       queue_rank,
		       enqueued_at_ts,
		       request_id,
		       status,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE user_id = ? AND status = 'queued' AND remaining > 0
		ORDER BY queue_rank ASC, id ASC
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
	used, err := s.countUserClaimsSince(ctx, s.store.DB(), userID, time.Now().Unix()-3600)
	if err != nil {
		return 0, err
	}
	return maxInt(0, limit-used), nil
}

func (s *Service) remainingMinuteQuota(ctx context.Context, apiKeyID int64, limit int) (int, error) {
	used, err := s.countAPIKeyClaimsSince(ctx, s.store.DB(), apiKeyID, time.Now().Unix()-60)
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

func withTx[T any](ctx context.Context, db *sql.DB, fn func(*sql.Tx) (T, error)) (T, error) {
	return runWithDatabaseBusyRetry(ctx, func() (T, error) {
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
