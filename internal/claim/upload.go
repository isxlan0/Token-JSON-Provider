package claim

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
)

var (
	errUploadDuplicate   = errors.New("upload duplicate")
	errUploadRateLimited = errors.New("upload rate limited")
)

type uploadFileInput struct {
	Name          string `json:"name"`
	ContentBase64 string `json:"content_base64"`
}

type uploadTask struct {
	BatchID          string
	UserID           int64
	ProviderUsername string
	ProviderName     string
	HourlyLimit      int
	RequestIndex     int
	FileName         string
	ContentJSON      string
	AccountID        string
	AccessTokenHash  string
}

type uploadPendingRecord struct {
	BatchID         string
	RequestIndex    int
	FileName        string
	AccountID       string
	AccessTokenHash string
	Status          string
	Reason          string
	Stage           string
	StageLabel      string
	Detail          string
	UpdatedAt       string
	QueuePosition   int
	QueueTotal      int
	Events          []map[string]any
	Watchers        []uploadWatcher
}

type uploadSnapshot struct {
	BatchID     string
	CreatedAt   string
	Items       []map[string]any
	History     []map[string]any
	QueueStatus map[string]any
}

type uploadWatcher struct {
	UserID       int64
	BatchID      string
	RequestIndex int
}

type uploadStateSpec struct {
	Status        string
	Reason        string
	Stage         string
	StageLabel    string
	Detail        string
	AccountID     string
	QueuePosition int
	QueueTotal    int
	RecordEvent   bool
}

func (s *Service) GetUploadResults(userID int64) map[string]any {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()

	snapshot, ok := s.uploadSnapshots[userID]
	if !ok {
		if s.cache != nil {
			var cached map[string]any
			if s.cache.GetJSON(s.userUploadResultsCacheKey(userID), &cached) {
				return cached
			}
		}
		return buildDefaultUploadResultsPayload()
	}
	return snapshot.payload()
}

func (s *Service) QueueUploadBatch(ctx context.Context, requestContext *auth.RequestContext, files []uploadFileInput) (map[string]any, error) {
	if requestContext == nil {
		return nil, echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}
	if requestContext.User.TrustLevel < int64(s.cfg.LinuxDO.MinTrustLevel) {
		return nil, echo.NewHTTPError(http.StatusForbidden, "当前账号信任等级不足，暂不能上传账号")
	}

	maxFiles := s.cfg.Upload.MaxFilesPerRequest
	if len(files) == 0 {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "请至少上传一个 JSON 文件。")
	}
	if len(files) > maxFiles {
		return nil, echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("单次最多上传 %d 个文件。", maxFiles))
	}

	uploadedCount, err := s.countRecentSuccessfulUploads(ctx, requestContext.UserID)
	if err != nil {
		return nil, err
	}
	if remaining := maxInt(0, s.cfg.Upload.MaxSuccessPerHour-uploadedCount); remaining <= 0 {
		return nil, echo.NewHTTPError(http.StatusTooManyRequests, "当前小时上传额度不足")
	}

	batchID, err := randomRequestID()
	if err != nil {
		return nil, fmt.Errorf("generate upload batch id: %w", err)
	}
	createdAt := isoformatNow()

	results := make([]map[string]any, 0, len(files))
	tasks := make([]uploadTask, 0, len(files))
	for index, input := range files {
		fileName := filepath.Base(strings.TrimSpace(input.Name))
		if fileName == "." || fileName == string(filepath.Separator) || fileName == "" {
			fileName = "upload.json"
		}

		normalized, contentJSON, err := normalizeUploadedFile(input, s.cfg.Upload.MaxFileSizeBytes)
		if err != nil {
			status := "invalid_file"
			if invalid, ok := err.(invalidUploadFileError); ok {
				status = invalid.uploadStatus()
			}
			results = append(results, newUploadResultItem(index, fileName, "", buildUploadStatePatch(uploadStateSpec{
				Status:      status,
				Reason:      err.Error(),
				Stage:       status,
				StageLabel:  "文件校验失败",
				Detail:      err.Error(),
				RecordEvent: true,
			})))
			continue
		}

		task := uploadTask{
			BatchID:          batchID,
			UserID:           requestContext.UserID,
			ProviderUsername: requestContext.User.Username,
			ProviderName:     firstNonEmpty(requestContext.User.Name, requestContext.User.Username),
			HourlyLimit:      s.cfg.Upload.MaxSuccessPerHour,
			RequestIndex:     index,
			FileName:         fileName,
			ContentJSON:      contentJSON,
			AccountID:        normalizedUploadString(normalized, "account_id"),
			AccessTokenHash:  hashTokenValue(normalizedUploadString(normalized, "access_token")),
		}

		existing, position := s.reserveUploadTask(task)
		if existing != nil {
			results = append(results, existing.snapshotItem(index, fileName, normalizedUploadString(normalized, "account_id")))
			continue
		}

		results = append(results, newUploadResultItem(index, fileName, normalizedUploadString(normalized, "account_id"), buildUploadStatePatch(uploadStateSpec{
			Status:        "queued",
			Reason:        "已进入上传队列。",
			Stage:         "queued",
			StageLabel:    "等待开始",
			Detail:        formatUploadQueueDetail(position, position),
			AccountID:     normalizedUploadString(normalized, "account_id"),
			QueuePosition: position,
			QueueTotal:    position,
			RecordEvent:   true,
		})))
		tasks = append(tasks, task)
	}
	results = s.refreshPendingUploadResultItems(results)

	payload := uploadSnapshot{
		BatchID:   batchID,
		CreatedAt: createdAt,
		Items:     cloneAnyMapSlice(results),
	}
	s.setUploadSnapshot(requestContext.UserID, payload)

	for _, task := range tasks {
		select {
		case s.uploadTaskCh <- task:
		case <-ctx.Done():
			return s.GetUploadResults(requestContext.UserID), ctx.Err()
		}
	}

	return payload.payload(), nil
}

func (s *Service) uploadWorkerLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case task := <-s.uploadTaskCh:
			if ctx.Err() != nil {
				return
			}
			s.markUploadTaskProcessing(task)
			s.processUploadTask(ctx, task)
		}
	}
}

func (s *Service) processUploadTask(ctx context.Context, task uploadTask) {
	resultItem := buildUploadStatePatch(uploadStateSpec{
		Status:      "probe_failed",
		Reason:      "后台处理失败，请稍后重试",
		Stage:       "processing_failed",
		StageLabel:  "处理失败",
		Detail:      "后台处理异常，本次文件尚未成功入库。",
		AccountID:   task.AccountID,
		RecordEvent: true,
	})

	conflict, err := s.getUploadedTokenConflict(ctx, task.AccountID, task.AccessTokenHash)
	switch {
	case err != nil && isDatabaseBusyError(err):
		resultItem = buildUploadStatePatch(uploadStateSpec{
			Status:      "db_busy",
			Reason:      "数据库正忙，本文件尚未处理，请稍后重试。",
			Stage:       "db_busy",
			StageLabel:  "数据库繁忙",
			Detail:      "数据库仍在处理其他任务，本次文件暂未入库。",
			AccountID:   task.AccountID,
			RecordEvent: true,
		})
	case err != nil:
		s.logger.Error("check upload conflict", "error", err, "account_id", task.AccountID)
	case conflict != nil:
		resultItem = buildUploadStatePatch(uploadStateSpec{
			Status:      "duplicate",
			Reason:      "账号已存在于历史库存中",
			Stage:       "duplicate",
			StageLabel:  "库存已存在",
			Detail:      "该账号已在历史库存中，跳过重复入库。",
			AccountID:   task.AccountID,
			RecordEvent: true,
		})
	default:
		var tokenContent map[string]any
		if err := json.Unmarshal([]byte(task.ContentJSON), &tokenContent); err != nil {
			s.logger.Error("decode upload task content", "error", err, "account_id", task.AccountID)
			break
		}

		s.pushUploadTaskProgress(task, buildUploadStatePatch(uploadStateSpec{
			Status:      "processing",
			Reason:      "正在测试账号可用性。",
			Stage:       "probing",
			StageLabel:  "开始测试",
			Detail:      "正在调用探活接口验证账号状态，请稍候。",
			AccountID:   task.AccountID,
			RecordEvent: true,
		}))

		probeResult := s.probe.Submit(tokenContent, s.cfg.Probe.TimeoutSec+s.cfg.Probe.DelaySec+5.0)
		switch {
		case probeResult.IsBanned():
			resultItem = buildUploadStatePatch(uploadStateSpec{
				Status:      "banned_401",
				Reason:      "账号已失效或被上游封禁",
				Stage:       "probe_rejected",
				StageLabel:  "测试返回 401",
				Detail:      formatUploadProbeFailureDetail(probeResult.HTTPStatus, probeResult.Detail, "账号已失效或被上游封禁。"),
				AccountID:   task.AccountID,
				RecordEvent: true,
			})
		case probeResult.Detail == "probe_queue_timeout":
			resultItem = buildUploadStatePatch(uploadStateSpec{
				Status:      "probe_timeout",
				Reason:      "账号探活超时，请稍后重试",
				Stage:       "probe_timeout",
				StageLabel:  "测试超时",
				Detail:      "探活任务等待超时，本次文件未入库。",
				AccountID:   task.AccountID,
				RecordEvent: true,
			})
		case probeResult.Status != "ok":
			reason := "账号探活失败"
			if probeResult.HTTPStatus != nil {
				reason = fmt.Sprintf("账号探活请求异常（HTTP %d）", *probeResult.HTTPStatus)
			} else if strings.TrimSpace(probeResult.Detail) != "" {
				reason = fmt.Sprintf("账号探活请求异常（%s）", probeResult.Detail)
			}
			resultItem = buildUploadStatePatch(uploadStateSpec{
				Status:      "probe_failed",
				Reason:      reason,
				Stage:       "probe_failed",
				StageLabel:  "测试失败",
				Detail:      formatUploadProbeFailureDetail(probeResult.HTTPStatus, probeResult.Detail, reason),
				AccountID:   task.AccountID,
				RecordEvent: true,
			})
		default:
			s.pushUploadTaskProgress(task, buildUploadStatePatch(uploadStateSpec{
				Status:      "processing",
				Reason:      "账号测试通过，准备入库。",
				Stage:       "probe_ok",
				StageLabel:  "测试通过",
				Detail:      "探活返回正常，准备写入库存。",
				AccountID:   task.AccountID,
				RecordEvent: true,
			}))

			uploadedAtTS := time.Now().Unix()
			targetName, nameErr := buildUploadedFileName(task.AccountID, uploadedAtTS)
			if nameErr != nil {
				s.logger.Error("build upload filename", "error", nameErr, "account_id", task.AccountID)
				break
			}

			s.pushUploadTaskProgress(task, buildUploadStatePatch(uploadStateSpec{
				Status:      "processing",
				Reason:      "正在写入库存，请稍候。",
				Stage:       "storing",
				StageLabel:  "写入库存",
				Detail:      "测试已通过，正在落盘并登记数据库。",
				AccountID:   task.AccountID,
				RecordEvent: true,
			}))

			s.markInternalTokenWrite(targetName)
			absolutePath, relativePath, writeErr := s.writeUploadedTokenFile(targetName, task.ContentJSON)
			if writeErr != nil {
				s.logger.Error("write uploaded token file", "error", writeErr, "file_name", targetName)
				break
			}

			created, createErr := s.createUploadedToken(ctx, uploadedTokenCreateParams{
				FileName:         targetName,
				FilePath:         relativePath,
				ContentJSON:      task.ContentJSON,
				ProviderUserID:   task.UserID,
				ProviderUsername: task.ProviderUsername,
				ProviderName:     task.ProviderName,
				UploadedAtTS:     uploadedAtTS,
				HourlyLimit:      task.HourlyLimit,
			})
			if createErr != nil {
				_ = os.Remove(absolutePath)
				switch {
				case errors.Is(createErr, errUploadDuplicate):
					resultItem = buildUploadStatePatch(uploadStateSpec{
						Status:      "duplicate",
						Reason:      "账号已存在于历史库存中",
						Stage:       "duplicate",
						StageLabel:  "库存已存在",
						Detail:      "账号在写入数据库时发现重复，未重复入库。",
						AccountID:   task.AccountID,
						RecordEvent: true,
					})
				case errors.Is(createErr, errUploadRateLimited):
					resultItem = buildUploadStatePatch(uploadStateSpec{
						Status:      "rate_limited",
						Reason:      "当前小时上传额度不足",
						Stage:       "rate_limited",
						StageLabel:  "额度不足",
						Detail:      "测试已通过，但当前小时上传额度已用尽。",
						AccountID:   task.AccountID,
						RecordEvent: true,
					})
				case isDatabaseBusyError(createErr):
					resultItem = buildUploadStatePatch(uploadStateSpec{
						Status:      "db_busy",
						Reason:      "数据库正忙，本文件尚未处理，请稍后重试。",
						Stage:       "db_busy",
						StageLabel:  "数据库繁忙",
						Detail:      "文件已生成，但数据库暂时繁忙，未完成入库登记。",
						AccountID:   task.AccountID,
						RecordEvent: true,
					})
				default:
					s.logger.Error("create uploaded token", "error", createErr, "file_name", targetName)
				}
			} else {
				resultItem = buildUploadStatePatch(uploadStateSpec{
					Status:      "accepted",
					Reason:      "校验通过，已加入可领取库存",
					Stage:       "accepted",
					StageLabel:  "入库成功",
					Detail:      fmt.Sprintf("测试通过并完成入库，库存文件 %s 已可供后续领取。", created.FileName),
					AccountID:   created.AccountID,
					RecordEvent: true,
				})
				resultItem["token_id"] = created.TokenID
				userIDCopy := task.UserID
				s.invalidateUserCache(task.UserID)
				s.invalidateDashboardCache(&userIDCopy)
				s.invalidateAdminCache()
				s.notifyQueueUsers(ctx, task.UserID)
				s.wakeQueuePump()
				s.primeUserReadCaches(ctx, task.UserID)
			}
		}
	}

	s.completeUploadTask(task, resultItem)
}

func (s *Service) reserveUploadTask(task uploadTask) (*uploadPendingRecord, int) {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()

	watcher := uploadWatcher{
		UserID:       task.UserID,
		BatchID:      task.BatchID,
		RequestIndex: task.RequestIndex,
	}

	existing := s.findPendingUploadTaskLocked(task.AccountID, task.AccessTokenHash)
	if existing != nil {
		s.registerUploadWatcherLocked(existing, watcher)
		return clonePendingUploadRecord(existing), 0
	}

	s.uploadQueued++
	pending := &uploadPendingRecord{
		BatchID:         task.BatchID,
		RequestIndex:    task.RequestIndex,
		FileName:        task.FileName,
		AccountID:       task.AccountID,
		AccessTokenHash: task.AccessTokenHash,
		Watchers:        []uploadWatcher{watcher},
	}
	s.applyUploadPendingPatchLocked(pending, buildUploadStatePatch(uploadStateSpec{
		Status:        "queued",
		Reason:        "已进入上传队列。",
		Stage:         "queued",
		StageLabel:    "等待开始",
		Detail:        formatUploadQueueDetail(s.uploadQueued, s.uploadQueued),
		AccountID:     task.AccountID,
		QueuePosition: s.uploadQueued,
		QueueTotal:    s.uploadQueued,
		RecordEvent:   true,
	}))
	s.setPendingUploadTaskLocked(pending)
	s.syncQueuedUploadStateLocked()
	return nil, pending.QueuePosition
}

func (s *Service) markUploadTaskProcessing(task uploadTask) {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()

	pending := s.findPendingUploadTaskLocked(task.AccountID, task.AccessTokenHash)
	patch := buildUploadStatePatch(uploadStateSpec{
		Status:      "processing",
		Reason:      "已轮到当前文件，准备开始测试。",
		Stage:       "processing",
		StageLabel:  "开始处理",
		Detail:      "已从上传队列取出，马上开始探活测试。",
		AccountID:   task.AccountID,
		RecordEvent: true,
	})
	if pending != nil && pending.Status == "queued" {
		s.uploadQueued = maxInt(0, s.uploadQueued-1)
		s.applyUploadPendingPatchLocked(pending, patch)
		s.syncQueuedUploadStateLocked()
		s.updateUploadWatchersLocked(pending, patch)
		return
	}
	s.updateUploadSnapshotItemLocked(task.UserID, task.BatchID, task.RequestIndex, patch)
}

func (s *Service) pushUploadTaskProgress(task uploadTask, patch map[string]any) {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()

	pending := s.findPendingUploadTaskLocked(task.AccountID, task.AccessTokenHash)
	if pending != nil {
		s.applyUploadPendingPatchLocked(pending, patch)
		s.updateUploadWatchersLocked(pending, patch)
		return
	}
	s.updateUploadSnapshotItemLocked(task.UserID, task.BatchID, task.RequestIndex, patch)
}

func (s *Service) completeUploadTask(task uploadTask, patch map[string]any) {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()

	pending := s.findPendingUploadTaskLocked(task.AccountID, task.AccessTokenHash)
	if pending != nil {
		s.applyUploadPendingPatchLocked(pending, patch)
		s.updateUploadWatchersLocked(pending, patch)
	} else {
		s.updateUploadSnapshotItemLocked(task.UserID, task.BatchID, task.RequestIndex, patch)
	}
	s.clearPendingUploadTaskLocked(task.AccountID, task.AccessTokenHash)
}

func (s *Service) setUploadSnapshot(userID int64, snapshot uploadSnapshot) {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()
	s.uploadSnapshots[userID] = cloneUploadSnapshot(snapshot)
	s.persistUploadSnapshotLocked(userID)
}

func (s *Service) updateUploadSnapshotItemLocked(userID int64, batchID string, requestIndex int, patch map[string]any) {
	snapshot, ok := s.uploadSnapshots[userID]
	if !ok || snapshot.BatchID != batchID {
		return
	}

	nextItems := make([]map[string]any, 0, len(snapshot.Items))
	for _, item := range snapshot.Items {
		updated := cloneAnyMap(item)
		if intFromAny(updated["request_index"]) == requestIndex {
			updated = applyUploadItemPatch(updated, patch)
		}
		nextItems = append(nextItems, updated)
	}

	snapshot.Items = nextItems
	s.uploadSnapshots[userID] = snapshot
	s.persistUploadSnapshotLocked(userID)
}

func (s *Service) findPendingUploadTaskLocked(accountID string, accessTokenHash string) *uploadPendingRecord {
	for _, key := range pendingUploadKeys(accountID, accessTokenHash) {
		if item, ok := s.uploadPending[key]; ok {
			return item
		}
	}
	return nil
}

func (s *Service) setPendingUploadTaskLocked(item *uploadPendingRecord) {
	for _, key := range pendingUploadKeys(item.AccountID, item.AccessTokenHash) {
		s.uploadPending[key] = item
	}
}

func (s *Service) registerUploadWatcherLocked(item *uploadPendingRecord, watcher uploadWatcher) {
	for _, existing := range item.Watchers {
		if existing.UserID == watcher.UserID && existing.BatchID == watcher.BatchID && existing.RequestIndex == watcher.RequestIndex {
			return
		}
	}
	item.Watchers = append(item.Watchers, watcher)
}

func (s *Service) updateUploadWatchersLocked(item *uploadPendingRecord, patch map[string]any) {
	for _, watcher := range item.Watchers {
		s.updateUploadSnapshotItemLocked(watcher.UserID, watcher.BatchID, watcher.RequestIndex, patch)
	}
}

func (s *Service) applyUploadPendingPatchLocked(item *uploadPendingRecord, patch map[string]any) {
	if item == nil {
		return
	}

	if status, ok := patch["status"].(string); ok {
		item.Status = status
	}
	if reason, ok := patch["reason"].(string); ok {
		item.Reason = reason
	}
	if stage, ok := patch["stage"].(string); ok {
		item.Stage = stage
	}
	if stageLabel, ok := patch["stage_label"].(string); ok {
		item.StageLabel = stageLabel
	}
	if detail, ok := patch["detail"].(string); ok {
		item.Detail = detail
	}
	if updatedAt, ok := patch["updated_at"].(string); ok {
		item.UpdatedAt = updatedAt
	}
	if queuePosition, ok := patch["queue_position"]; ok {
		item.QueuePosition = maxInt(0, intFromAny(queuePosition))
	}
	if queueTotal, ok := patch["queue_total"]; ok {
		item.QueueTotal = maxInt(0, intFromAny(queueTotal))
	}
	item.Events = appendUploadItemEvent(item.Events, patch["event"])
}

func (s *Service) syncQueuedUploadStateLocked() {
	queuedItems := s.uniqueQueuedUploadRecordsLocked()
	total := len(queuedItems)
	for index, item := range queuedItems {
		position := index + 1
		item.QueuePosition = position
		item.QueueTotal = total
		patch := buildUploadStatePatch(uploadStateSpec{
			Status:        "queued",
			Reason:        "已进入上传队列。",
			Stage:         "queued",
			StageLabel:    "等待开始",
			Detail:        formatUploadQueueDetail(position, total),
			AccountID:     item.AccountID,
			QueuePosition: position,
			QueueTotal:    total,
			RecordEvent:   false,
		})
		s.applyUploadPendingPatchLocked(item, patch)
		s.updateUploadWatchersLocked(item, patch)
	}
}

func (s *Service) uniqueQueuedUploadRecordsLocked() []*uploadPendingRecord {
	seen := make(map[*uploadPendingRecord]struct{})
	items := make([]*uploadPendingRecord, 0, len(s.uploadPending))
	for _, item := range s.uploadPending {
		if item == nil || item.Status != "queued" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		items = append(items, item)
	}
	sort.SliceStable(items, func(left int, right int) bool {
		if items[left].QueuePosition == items[right].QueuePosition {
			if items[left].BatchID == items[right].BatchID {
				return items[left].RequestIndex < items[right].RequestIndex
			}
			return items[left].BatchID < items[right].BatchID
		}
		return items[left].QueuePosition < items[right].QueuePosition
	})
	return items
}

func (s *Service) clearPendingUploadTaskLocked(accountID string, accessTokenHash string) {
	for _, key := range pendingUploadKeys(accountID, accessTokenHash) {
		delete(s.uploadPending, key)
	}
}

func (s *Service) countRecentSuccessfulUploads(ctx context.Context, userID int64) (int, error) {
	return runWithDatabaseBusyRetry(ctx, func() (int, error) {
		return queryCount(ctx, s.store.DB(), `
			SELECT COUNT(*)
			FROM tokens
			WHERE provider_user_id = ?
			  AND uploaded_at_ts IS NOT NULL
			  AND uploaded_at_ts >= ?
		`, strconvFormatInt(userID), time.Now().Unix()-3600)
	})
}

func (s *Service) getUploadedTokenConflict(ctx context.Context, accountID string, accessTokenHash string) (map[string]any, error) {
	return runWithDatabaseBusyRetry(ctx, func() (map[string]any, error) {
		row := s.store.DB().QueryRowContext(ctx, `
			SELECT id, account_id, access_token_hash, file_name
			FROM tokens
			WHERE account_id = ? OR access_token_hash = ?
			ORDER BY id ASC
			LIMIT 1
		`, accountID, accessTokenHash)

		var (
			id              int64
			conflictAccount sql.NullString
			conflictHash    sql.NullString
			fileName        string
		)
		if err := row.Scan(&id, &conflictAccount, &conflictHash, &fileName); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, fmt.Errorf("query upload conflict: %w", err)
		}

		return map[string]any{
			"id":                id,
			"account_id":        conflictAccount.String,
			"access_token_hash": conflictHash.String,
			"file_name":         fileName,
		}, nil
	})
}

type uploadedTokenCreateParams struct {
	FileName         string
	FilePath         string
	ContentJSON      string
	ProviderUserID   int64
	ProviderUsername string
	ProviderName     string
	UploadedAtTS     int64
	HourlyLimit      int
}

type createdUploadedToken struct {
	TokenID   int64
	AccountID string
	FileName  string
}

func (s *Service) createUploadedToken(ctx context.Context, params uploadedTokenCreateParams) (createdUploadedToken, error) {
	normalized, err := normalizeUploadContentJSON(params.ContentJSON)
	if err != nil {
		return createdUploadedToken{}, fmt.Errorf("normalize uploaded token content: %w", err)
	}

	fileHashBytes := sha256.Sum256([]byte(params.ContentJSON))
	fileHash := hex.EncodeToString(fileHashBytes[:])
	accountID := normalizedUploadString(normalized, "account_id")
	accessTokenHash := hashTokenValue(normalizedUploadString(normalized, "access_token"))
	healthyMaxClaims := s.cfg.Inventory.Limits.Healthy.MaxClaims

	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (createdUploadedToken, error) {
		uploadedCount, err := queryCount(ctx, tx, `
			SELECT COUNT(*)
			FROM tokens
			WHERE provider_user_id = ?
			  AND uploaded_at_ts IS NOT NULL
			  AND uploaded_at_ts >= ?
		`, strconvFormatInt(params.ProviderUserID), params.UploadedAtTS-3600)
		if err != nil {
			return createdUploadedToken{}, err
		}
		if uploadedCount >= params.HourlyLimit {
			return createdUploadedToken{}, errUploadRateLimited
		}

		conflict, err := s.getUploadedTokenConflictTx(ctx, tx, accountID, accessTokenHash)
		if err != nil {
			return createdUploadedToken{}, err
		}
		if conflict {
			return createdUploadedToken{}, errUploadDuplicate
		}

		result, err := tx.ExecContext(ctx, `
			INSERT INTO tokens (
				file_name,
				file_path,
				file_hash,
				encoding,
				content_json,
				account_id,
				access_token_hash,
				provider_user_id,
				provider_username,
				provider_name,
				uploaded_at_ts,
				is_active,
				is_cleaned,
				is_enabled,
				is_banned,
				is_available,
				claim_count,
				max_claims,
				created_at_ts,
				cleaned_at_ts,
				updated_at_ts,
				last_seen_at_ts
			) VALUES (?, ?, ?, 'utf-8', ?, ?, ?, ?, ?, ?, ?, 1, 0, 1, 0, 1, 0, ?, ?, NULL, ?, ?)
		`, params.FileName, params.FilePath, fileHash, params.ContentJSON, accountID, accessTokenHash, strconvFormatInt(params.ProviderUserID), params.ProviderUsername, params.ProviderName, params.UploadedAtTS, healthyMaxClaims, params.UploadedAtTS, params.UploadedAtTS, params.UploadedAtTS)
		if err != nil {
			if isUniqueConstraintError(err) {
				return createdUploadedToken{}, errUploadDuplicate
			}
			return createdUploadedToken{}, fmt.Errorf("insert uploaded token: %w", err)
		}

		tokenID, err := result.LastInsertId()
		if err != nil {
			return createdUploadedToken{}, fmt.Errorf("read uploaded token id: %w", err)
		}

		if _, err := s.ensureInventoryPolicyTx(ctx, tx, true); err != nil {
			return createdUploadedToken{}, err
		}

		return createdUploadedToken{
			TokenID:   tokenID,
			AccountID: accountID,
			FileName:  params.FileName,
		}, nil
	})
}

func (s *Service) getUploadedTokenConflictTx(ctx context.Context, tx *sql.Tx, accountID string, accessTokenHash string) (bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM tokens
		WHERE account_id = ? OR access_token_hash = ?
		LIMIT 1
	`, accountID, accessTokenHash)

	var marker int
	if err := row.Scan(&marker); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("query upload conflict in transaction: %w", err)
	}
	return true, nil
}

func normalizeUploadContentJSON(contentJSON string) (map[string]any, error) {
	var payload any
	if err := json.Unmarshal([]byte(contentJSON), &payload); err != nil {
		return nil, err
	}
	return normalizeUploadContent(payload)
}

func normalizeUploadContent(payload any) (map[string]any, error) {
	root, ok := payload.(map[string]any)
	if !ok {
		return nil, invalidUploadFileError{status: "invalid_json", message: "JSON 根节点必须是对象"}
	}

	candidate := root
	if storage, ok := root["storage"].(map[string]any); ok {
		candidate = storage
	}

	required := []string{"access_token", "refresh_token", "account_id"}
	normalized := make(map[string]any, len(candidate))
	for key, value := range candidate {
		normalized[key] = value
	}
	missing := make([]string, 0)
	for _, key := range required {
		value := strings.TrimSpace(fmt.Sprint(candidate[key]))
		if value == "" || value == "<nil>" {
			missing = append(missing, key)
			continue
		}
		normalized[key] = value
	}
	if len(missing) > 0 {
		return nil, invalidUploadFileError{status: "missing_fields", message: "缺少必填字段：" + strings.Join(missing, ", ")}
	}
	return normalized, nil
}

type invalidUploadFileError struct {
	status  string
	message string
}

func (e invalidUploadFileError) Error() string {
	return e.message
}

func (e invalidUploadFileError) uploadStatus() string {
	return e.status
}

func normalizeUploadedFile(input uploadFileInput, maxFileSize int) (map[string]any, string, error) {
	name := filepath.Base(strings.TrimSpace(input.Name))
	if name == "" {
		name = "upload.json"
	}
	if !strings.HasSuffix(strings.ToLower(name), ".json") {
		return nil, "", invalidUploadFileError{status: "invalid_file", message: "仅支持上传 .json 文件"}
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(input.ContentBase64))
	if err != nil {
		return nil, "", invalidUploadFileError{status: "invalid_file", message: "文件内容编码无效"}
	}
	if len(raw) == 0 {
		return nil, "", invalidUploadFileError{status: "invalid_json", message: "文件为空"}
	}
	if len(raw) > maxFileSize {
		return nil, "", invalidUploadFileError{status: "file_too_large", message: fmt.Sprintf("单文件不能超过 %d 字节", maxFileSize)}
	}

	_, decoded, err := detectAndDecodeText(raw)
	if err != nil {
		return nil, "", invalidUploadFileError{status: "invalid_json", message: "JSON 解析失败"}
	}
	var payload any
	if err := json.Unmarshal([]byte(decoded), &payload); err != nil {
		return nil, "", invalidUploadFileError{status: "invalid_json", message: "JSON 解析失败"}
	}

	normalized, normErr := normalizeUploadContent(payload)
	if normErr != nil {
		if invalid, ok := normErr.(invalidUploadFileError); ok {
			return nil, "", invalid
		}
		return nil, "", invalidUploadFileError{status: "invalid_json", message: "JSON 解析失败"}
	}

	contentBytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, "", invalidUploadFileError{status: "invalid_json", message: "JSON 解析失败"}
	}
	return normalized, string(contentBytes), nil
}

func buildUploadedFileName(accountID string, uploadedAtTS int64) (string, error) {
	accountHash := sha256.Sum256([]byte(accountID))
	suffix, err := randomRequestID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("upload-%d-%s-%s.json", uploadedAtTS, hex.EncodeToString(accountHash[:])[:8], suffix[:6]), nil
}

func (s *Service) writeUploadedTokenFile(fileName string, contentJSON string) (string, string, error) {
	tokenDir := s.tokenDirPath()
	if err := os.MkdirAll(tokenDir, 0o755); err != nil {
		return "", "", fmt.Errorf("create token directory: %w", err)
	}

	absolutePath := filepath.Join(tokenDir, filepath.Base(fileName))
	if err := os.WriteFile(absolutePath, []byte(contentJSON), 0o644); err != nil {
		return "", "", fmt.Errorf("write uploaded token file: %w", err)
	}

	return absolutePath, s.tokenFileRelativePath(fileName), nil
}

func pendingUploadKeys(accountID string, accessTokenHash string) []string {
	keys := make([]string, 0, 2)
	if trimmed := strings.TrimSpace(accountID); trimmed != "" {
		keys = append(keys, "account:"+trimmed)
	}
	if trimmed := strings.TrimSpace(accessTokenHash); trimmed != "" {
		keys = append(keys, "access:"+trimmed)
	}
	return keys
}

func (snapshot uploadSnapshot) payload() map[string]any {
	return buildUploadResponse(
		cloneAnyMapSlice(snapshot.Items),
		snapshot.BatchID,
		snapshot.CreatedAt,
		cloneAnyMapSlice(snapshot.History),
		cloneAnyMap(snapshot.QueueStatus),
	)
}

func cloneUploadSnapshot(snapshot uploadSnapshot) uploadSnapshot {
	return uploadSnapshot{
		BatchID:     snapshot.BatchID,
		CreatedAt:   snapshot.CreatedAt,
		Items:       cloneAnyMapSlice(snapshot.Items),
		History:     cloneAnyMapSlice(snapshot.History),
		QueueStatus: cloneAnyMap(snapshot.QueueStatus),
	}
}

func clonePendingUploadRecord(item *uploadPendingRecord) *uploadPendingRecord {
	if item == nil {
		return nil
	}
	copied := *item
	copied.Events = cloneAnyMapSlice(item.Events)
	copied.Watchers = append([]uploadWatcher(nil), item.Watchers...)
	return &copied
}

func (item *uploadPendingRecord) snapshotItem(requestIndex int, fileName string, accountID string) map[string]any {
	if item == nil {
		return newUploadResultItem(requestIndex, fileName, accountID, nil)
	}
	base := map[string]any{
		"request_index":  requestIndex,
		"file_name":      fileName,
		"account_id":     firstNonEmpty(strings.TrimSpace(accountID), item.AccountID),
		"status":         item.Status,
		"reason":         item.Reason,
		"stage":          item.Stage,
		"stage_label":    item.StageLabel,
		"detail":         item.Detail,
		"updated_at":     item.UpdatedAt,
		"queue_position": item.QueuePosition,
		"queue_total":    item.QueueTotal,
		"events":         cloneAnyMapSlice(item.Events),
	}
	return applyUploadItemPatch(base, nil)
}

func newUploadResultItem(requestIndex int, fileName string, accountID string, patch map[string]any) map[string]any {
	base := map[string]any{
		"request_index": requestIndex,
		"file_name":     fileName,
	}
	if trimmed := strings.TrimSpace(accountID); trimmed != "" {
		base["account_id"] = trimmed
	}
	return applyUploadItemPatch(base, patch)
}

func applyUploadItemPatch(item map[string]any, patch map[string]any) map[string]any {
	updated := cloneAnyMap(item)
	if updated == nil {
		updated = make(map[string]any)
	}
	updated["events"] = cloneAnyMapSlice(eventSliceFromAny(updated["events"]))
	if patch == nil {
		return updated
	}
	for key, value := range patch {
		if key == "event" {
			continue
		}
		updated[key] = value
	}
	updated["events"] = appendUploadItemEvent(eventSliceFromAny(updated["events"]), patch["event"])
	return updated
}

func buildUploadStatePatch(spec uploadStateSpec) map[string]any {
	reason := strings.TrimSpace(spec.Reason)
	detail := strings.TrimSpace(spec.Detail)
	if detail == "" {
		detail = reason
	}
	stageLabel := strings.TrimSpace(spec.StageLabel)
	if stageLabel == "" {
		stageLabel = uploadStageLabelFromStatus(spec.Status)
	}
	updatedAt := isoformatNow()

	patch := map[string]any{
		"status":         strings.TrimSpace(spec.Status),
		"reason":         reason,
		"stage":          strings.TrimSpace(spec.Stage),
		"stage_label":    stageLabel,
		"detail":         detail,
		"updated_at":     updatedAt,
		"queue_position": maxInt(0, spec.QueuePosition),
		"queue_total":    maxInt(0, spec.QueueTotal),
	}
	if trimmed := strings.TrimSpace(spec.AccountID); trimmed != "" {
		patch["account_id"] = trimmed
	}
	if spec.RecordEvent {
		patch["event"] = map[string]any{
			"stage":  strings.TrimSpace(spec.Stage),
			"label":  stageLabel,
			"detail": detail,
			"status": strings.TrimSpace(spec.Status),
			"at":     updatedAt,
		}
	}
	return patch
}

func uploadStageLabelFromStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "queued":
		return "等待开始"
	case "processing":
		return "处理中"
	case "accepted":
		return "入库成功"
	case "duplicate":
		return "库存已存在"
	case "invalid_json", "missing_fields", "invalid_file", "file_too_large":
		return "文件校验失败"
	case "banned_401":
		return "账号失效"
	case "probe_timeout":
		return "测试超时"
	case "probe_failed":
		return "测试失败"
	case "rate_limited":
		return "额度不足"
	case "db_busy":
		return "数据库繁忙"
	default:
		return "处理中"
	}
}

func formatUploadQueueDetail(position int, total int) string {
	position = maxInt(0, position)
	total = maxInt(0, total)
	switch {
	case total <= 0:
		return "等待队列调度，轮到后会自动开始测试。"
	case position <= 1:
		return fmt.Sprintf("等待 %d/%d，前面没有其他文件，马上开始测试。", maxInt(1, position), total)
	default:
		return fmt.Sprintf("等待 %d/%d，前面还有 %d 个文件，轮到后会自动开始测试。", position, total, maxInt(0, position-1))
	}
}

func formatUploadProbeFailureDetail(httpStatus *int, detail string, fallback string) string {
	normalizedDetail := compactUploadDetail(detail)
	switch {
	case httpStatus != nil && normalizedDetail != "":
		return fmt.Sprintf("测试返回 HTTP %d：%s", *httpStatus, normalizedDetail)
	case httpStatus != nil:
		return fmt.Sprintf("测试返回 HTTP %d。", *httpStatus)
	case normalizedDetail != "":
		return fmt.Sprintf("测试返回：%s", normalizedDetail)
	default:
		return strings.TrimSpace(fallback)
	}
}

func compactUploadDetail(detail string) string {
	normalized := strings.Join(strings.Fields(strings.TrimSpace(detail)), " ")
	if normalized == "" {
		return ""
	}
	const maxDetailLength = 120
	if len(normalized) <= maxDetailLength {
		return normalized
	}
	return normalized[:maxDetailLength] + "..."
}

func eventSliceFromAny(value any) []map[string]any {
	switch typed := value.(type) {
	case nil:
		return []map[string]any{}
	case []map[string]any:
		return cloneAnyMapSlice(typed)
	case []any:
		items := make([]map[string]any, 0, len(typed))
		for _, entry := range typed {
			if mapped, ok := entry.(map[string]any); ok {
				items = append(items, cloneAnyMap(mapped))
			}
		}
		return items
	default:
		return []map[string]any{}
	}
}

func appendUploadItemEvent(events []map[string]any, rawEvent any) []map[string]any {
	event, ok := rawEvent.(map[string]any)
	if !ok || len(event) == 0 {
		return cloneAnyMapSlice(events)
	}
	clonedEvents := cloneAnyMapSlice(events)
	nextEvent := cloneAnyMap(event)
	if len(clonedEvents) > 0 {
		last := clonedEvents[len(clonedEvents)-1]
		if fmt.Sprint(last["stage"]) == fmt.Sprint(nextEvent["stage"]) &&
			fmt.Sprint(last["label"]) == fmt.Sprint(nextEvent["label"]) &&
			fmt.Sprint(last["detail"]) == fmt.Sprint(nextEvent["detail"]) {
			return clonedEvents
		}
	}
	return append(clonedEvents, nextEvent)
}

func (s *Service) refreshPendingUploadResultItems(items []map[string]any) []map[string]any {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()

	refreshed := make([]map[string]any, 0, len(items))
	for _, item := range items {
		requestIndex := intFromAny(item["request_index"])
		fileName := strings.TrimSpace(fmt.Sprint(item["file_name"]))
		accountID := strings.TrimSpace(fmt.Sprint(item["account_id"]))
		if pending := s.findPendingUploadTaskLocked(accountID, ""); pending != nil {
			refreshed = append(refreshed, pending.snapshotItem(requestIndex, fileName, accountID))
			continue
		}
		refreshed = append(refreshed, applyUploadItemPatch(item, nil))
	}
	return refreshed
}

func (s *Service) persistUploadSnapshotLocked(userID int64) {
	snapshot, ok := s.uploadSnapshots[userID]
	if !ok {
		return
	}
	payload := snapshot.payload()
	if s.cache != nil {
		key := s.userUploadResultsCacheKey(userID)
		var previous map[string]any
		changed := !s.cache.GetJSON(key, &previous) || !reflect.DeepEqual(previous, payload)
		if changed {
			s.cache.BumpScope("user-upload-results", userID)
			key = s.userUploadResultsCacheKey(userID)
		}
		s.cache.SetJSON(key, payload, s.uploadResultsTTL())
	}
	if s.uploadEvents != nil {
		s.uploadEvents.notify(userID)
	}
}

func cloneAnyMapSlice(items []map[string]any) []map[string]any {
	if len(items) == 0 {
		return []map[string]any{}
	}
	cloned := make([]map[string]any, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, cloneAnyMap(item))
	}
	return cloned
}

func cloneAnyMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func strconvFormatInt(value int64) string {
	return fmt.Sprintf("%d", value)
}

func hashTokenValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func buildUploadResponse(items []map[string]any, batchID string, createdAt string, history []map[string]any, queueStatus map[string]any) map[string]any {
	summary := buildDefaultUploadResultsSummary()
	summary["total"] = len(items)
	for _, item := range items {
		switch item["status"] {
		case "accepted":
			summary["accepted"]++
		case "duplicate":
			summary["duplicates"]++
		case "invalid_json", "missing_fields", "invalid_file", "file_too_large":
			summary["invalid"]++
		case "db_busy":
			summary["db_busy"]++
		case "queued":
			summary["queued"]++
		case "processing":
			summary["processing"]++
		default:
			summary["rejected"]++
		}
	}

	return map[string]any{
		"batch_id":     batchID,
		"created_at":   createdAt,
		"summary":      summary,
		"items":        items,
		"history":      historyOrEmpty(history),
		"queue_status": queueStatus,
	}
}

func historyOrEmpty(items []map[string]any) []map[string]any {
	if len(items) == 0 {
		return []map[string]any{}
	}
	return items
}

func normalizedUploadString(payload map[string]any, key string) string {
	return strings.TrimSpace(fmt.Sprint(payload[key]))
}
