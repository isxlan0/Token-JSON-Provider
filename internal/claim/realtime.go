package claim

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"token-atlas/internal/auth"
)

const (
	claimStatusProcessing    = "processing"
	claimStatusQueued        = "queued"
	claimStatusQueuedWaiting = "queued_waiting"
	claimStatusQueuedBlocked = "queued_blocked"
	claimStatusSucceeded     = "succeeded"
	claimStatusPartial       = "partial"
	claimStatusFailed        = "failed"
	claimStatusCancelled     = "cancelled"
	claimStatusExpired       = "expired"

	claimSourceSelf         = "self"
	claimSourceOtherSession = "other_session"
	claimSourceSystem       = "system"

	claimSourceKindUser   = "user"
	claimSourceKindSystem = "system"

	claimRealtimeMaxRequests = 20
)

type claimRequestAccepted struct {
	RequestID      string `json:"request_id"`
	Status         string `json:"status"`
	Queued         bool   `json:"queued"`
	QueueID        int64  `json:"queue_id,omitempty"`
	QueuePosition  int    `json:"queue_position,omitempty"`
	QueueRemaining int    `json:"queue_remaining,omitempty"`
	BlockReason    string `json:"block_reason,omitempty"`
	LastProgressAt string `json:"last_progress_at,omitempty"`
	NextRetryAt    string `json:"next_retry_at,omitempty"`
	Requested      int    `json:"requested"`
	AcceptedAt     string `json:"accepted_at"`
	Source         string `json:"source"`
}

type claimRealtimeRequest struct {
	RequestID       string              `json:"request_id"`
	Status          string              `json:"status"`
	Requested       int                 `json:"requested"`
	Granted         int                 `json:"granted"`
	Remaining       int                 `json:"remaining"`
	Queued          bool                `json:"queued"`
	QueueID         int64               `json:"queue_id,omitempty"`
	QueuePosition   int                 `json:"queue_position,omitempty"`
	QueueTotal      int                 `json:"queue_total,omitempty"`
	BlockReason     string              `json:"block_reason,omitempty"`
	LastProgressAt  string              `json:"last_progress_at,omitempty"`
	NextRetryAt     string              `json:"next_retry_at,omitempty"`
	Items           []claimResponseItem `json:"items,omitempty"`
	ReasonCode      string              `json:"reason_code,omitempty"`
	ReasonMessage   string              `json:"reason_message,omitempty"`
	Source          string              `json:"source,omitempty"`
	SourceKind      string              `json:"source_kind,omitempty"`
	OriginSessionID string              `json:"origin_session_id,omitempty"`
	OriginTabID     string              `json:"origin_tab_id,omitempty"`
	EnqueuedAt      string              `json:"enqueued_at,omitempty"`
	UpdatedAt       string              `json:"updated_at"`
	UpdatedAtTS     int64               `json:"updated_at_ts,omitempty"`
	Terminal        bool                `json:"terminal"`
}

type claimRealtimeSnapshot struct {
	Requests       []claimRealtimeRequest `json:"requests"`
	StreamRequired bool                   `json:"stream_required"`
	Transport      string                 `json:"transport"`
	GeneratedAt    string                 `json:"generated_at"`
}

func isClaimTerminalStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case claimStatusSucceeded, claimStatusPartial, claimStatusFailed, claimStatusCancelled, claimStatusExpired:
		return true
	default:
		return false
	}
}

func isClaimQueuedStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case claimStatusQueued, claimStatusQueuedWaiting, claimStatusQueuedBlocked:
		return true
	default:
		return false
	}
}

func cloneClaimResponseItems(items []claimResponseItem) []claimResponseItem {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]claimResponseItem, len(items))
	copy(cloned, items)
	return cloned
}

func claimSourceForSession(request claimRealtimeRequest, sessionID string) string {
	if strings.EqualFold(strings.TrimSpace(request.SourceKind), claimSourceKindSystem) {
		return claimSourceSystem
	}
	originSessionID := strings.TrimSpace(request.OriginSessionID)
	currentSessionID := strings.TrimSpace(sessionID)
	if originSessionID == "" || currentSessionID == "" || originSessionID == currentSessionID {
		return claimSourceSelf
	}
	return claimSourceOtherSession
}

func sortClaimRealtimeRequests(items []claimRealtimeRequest) {
	sort.SliceStable(items, func(i int, j int) bool {
		left := items[i]
		right := items[j]
		if left.Terminal != right.Terminal {
			return !left.Terminal
		}
		if left.UpdatedAtTS != right.UpdatedAtTS {
			return left.UpdatedAtTS > right.UpdatedAtTS
		}
		return left.RequestID > right.RequestID
	})
}

func trimClaimRealtimeRequests(items []claimRealtimeRequest) []claimRealtimeRequest {
	if len(items) == 0 {
		return []claimRealtimeRequest{}
	}
	sortClaimRealtimeRequests(items)
	result := make([]claimRealtimeRequest, 0, len(items))
	terminalCount := 0
	for _, item := range items {
		if item.Terminal {
			if terminalCount >= claimRealtimeMaxRequests {
				continue
			}
			terminalCount++
		}
		result = append(result, item)
	}
	return result
}

func sameClaimRealtimeRequestContent(left claimRealtimeRequest, right claimRealtimeRequest) bool {
	return left.RequestID == right.RequestID &&
		left.Status == right.Status &&
		left.Requested == right.Requested &&
		left.Granted == right.Granted &&
		left.Remaining == right.Remaining &&
		left.Queued == right.Queued &&
		left.QueueID == right.QueueID &&
		left.QueuePosition == right.QueuePosition &&
		left.QueueTotal == right.QueueTotal &&
		left.BlockReason == right.BlockReason &&
		left.LastProgressAt == right.LastProgressAt &&
		left.NextRetryAt == right.NextRetryAt &&
		left.ReasonCode == right.ReasonCode &&
		left.ReasonMessage == right.ReasonMessage &&
		left.SourceKind == right.SourceKind &&
		left.OriginSessionID == right.OriginSessionID &&
		left.OriginTabID == right.OriginTabID &&
		left.EnqueuedAt == right.EnqueuedAt &&
		left.Terminal == right.Terminal &&
		reflect.DeepEqual(left.Items, right.Items)
}

func claimRealtimeStatusPrecedence(status string) int {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case claimStatusSucceeded, claimStatusPartial, claimStatusFailed, claimStatusCancelled, claimStatusExpired:
		return 40
	case claimStatusQueuedBlocked:
		return 30
	case claimStatusQueuedWaiting:
		return 20
	case claimStatusQueued:
		return 10
	default:
		return 0
	}
}

func shouldKeepExistingClaimRealtime(existing claimRealtimeRequest, incoming claimRealtimeRequest) (bool, string) {
	if existing.Terminal && !incoming.Terminal {
		return true, "terminal_preserved"
	}
	if existing.UpdatedAtTS > 0 && incoming.UpdatedAtTS > 0 && incoming.UpdatedAtTS < existing.UpdatedAtTS {
		return true, "older_updated_at"
	}
	if incoming.Granted < existing.Granted {
		return true, "granted_regressed"
	}
	if incoming.Remaining > existing.Remaining {
		return true, "remaining_regressed"
	}
	if existing.QueuePosition > 0 && incoming.QueuePosition > existing.QueuePosition {
		return true, "position_regressed"
	}
	if incoming.UpdatedAtTS == existing.UpdatedAtTS {
		if claimRealtimeStatusPrecedence(incoming.Status) < claimRealtimeStatusPrecedence(existing.Status) {
			return true, "status_regressed"
		}
		if incoming.QueueTotal < existing.QueueTotal &&
			incoming.QueuePosition == existing.QueuePosition &&
			incoming.Granted == existing.Granted &&
			incoming.Remaining == existing.Remaining &&
			incoming.Status == existing.Status {
			return true, "queue_total_regressed"
		}
	}
	return false, ""
}

func (s *Service) claimRealtimeTTL() int {
	return maxInt(s.cfg.Cache.QueueTTL, 900)
}

func (s *Service) emptyClaimRealtimeSnapshot() claimRealtimeSnapshot {
	return claimRealtimeSnapshot{
		Requests:       []claimRealtimeRequest{},
		StreamRequired: true,
		Transport:      "sse",
		GeneratedAt:    isoformatNow(),
	}
}

func (s *Service) decorateClaimRealtimeSnapshot(snapshot claimRealtimeSnapshot, sessionID string) claimRealtimeSnapshot {
	decorated := snapshot
	decorated.GeneratedAt = isoformatNow()
	if len(snapshot.Requests) == 0 {
		decorated.Requests = []claimRealtimeRequest{}
		return decorated
	}

	requests := make([]claimRealtimeRequest, 0, len(snapshot.Requests))
	for _, item := range snapshot.Requests {
		next := item
		next.Items = cloneClaimResponseItems(item.Items)
		next.Source = claimSourceForSession(item, sessionID)
		requests = append(requests, next)
	}
	sortClaimRealtimeRequests(requests)
	decorated.Requests = requests
	return decorated
}

func (s *Service) getCachedStoredClaimRealtimeSnapshot(userID int64) (claimRealtimeSnapshot, bool) {
	if s.cache == nil {
		return claimRealtimeSnapshot{}, false
	}
	var payload claimRealtimeSnapshot
	if !s.cache.GetJSON(s.userClaimRealtimeCacheKey(userID), &payload) {
		return claimRealtimeSnapshot{}, false
	}
	if payload.Requests == nil {
		payload.Requests = []claimRealtimeRequest{}
	}
	if !payload.StreamRequired {
		payload.StreamRequired = true
	}
	if strings.TrimSpace(payload.Transport) == "" {
		payload.Transport = "sse"
	}
	return payload, true
}

func (s *Service) setClaimRealtimeSnapshot(userID int64, payload claimRealtimeSnapshot) claimRealtimeSnapshot {
	payload.StreamRequired = true
	if strings.TrimSpace(payload.Transport) == "" {
		payload.Transport = "sse"
	}
	if payload.Requests == nil {
		payload.Requests = []claimRealtimeRequest{}
	}
	payload.Requests = trimClaimRealtimeRequests(payload.Requests)
	payload.GeneratedAt = isoformatNow()

	if s.cache == nil {
		if s.claimEvents != nil {
			s.claimEvents.notify(userID)
		}
		return payload
	}

	key := s.userClaimRealtimeCacheKey(userID)
	var previous claimRealtimeSnapshot
	hadPrevious := s.cache.GetJSON(key, &previous)
	changed := !hadPrevious || !reflect.DeepEqual(previous.Requests, payload.Requests) || previous.StreamRequired != payload.StreamRequired || previous.Transport != payload.Transport
	if changed {
		s.cache.BumpScope("user-claim-realtime", userID)
		key = s.userClaimRealtimeCacheKey(userID)
	}
	s.cache.SetJSON(key, payload, s.claimRealtimeTTL())
	if changed && s.claimEvents != nil {
		s.claimEvents.notify(userID)
	}
	return payload
}

func (s *Service) getStoredClaimRealtimeSnapshot(ctx context.Context, userID int64) (claimRealtimeSnapshot, error) {
	if payload, ok := s.getCachedStoredClaimRealtimeSnapshot(userID); ok {
		return payload, nil
	}
	payload, err := s.rebuildClaimRealtimeSnapshot(ctx, userID)
	if err != nil {
		return claimRealtimeSnapshot{}, err
	}
	return s.setClaimRealtimeSnapshot(userID, payload), nil
}

func (s *Service) GetClaimRealtimeSnapshot(ctx context.Context, userID int64, sessionID string) (claimRealtimeSnapshot, error) {
	payload, err := s.getStoredClaimRealtimeSnapshot(ctx, userID)
	if err != nil {
		return claimRealtimeSnapshot{}, err
	}
	return s.decorateClaimRealtimeSnapshot(payload, sessionID), nil
}

func (s *Service) findClaimRealtimeRequest(ctx context.Context, userID int64, requestID string) (*claimRealtimeRequest, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, nil
	}
	snapshot, err := s.getStoredClaimRealtimeSnapshot(ctx, userID)
	if err != nil {
		return nil, err
	}
	for _, item := range snapshot.Requests {
		if item.RequestID == requestID {
			copied := item
			copied.Items = cloneClaimResponseItems(item.Items)
			return &copied, nil
		}
	}
	return nil, nil
}

func (s *Service) upsertClaimRealtimeRequest(ctx context.Context, userID int64, request claimRealtimeRequest) error {
	if userID <= 0 || strings.TrimSpace(request.RequestID) == "" {
		return nil
	}
	if strings.TrimSpace(request.Status) == "" {
		request.Status = claimStatusProcessing
	}
	request.Status = strings.ToLower(strings.TrimSpace(request.Status))
	request.Queued = isClaimQueuedStatus(request.Status)
	request.Terminal = isClaimTerminalStatus(request.Status)
	request.SourceKind = firstNonEmpty(strings.TrimSpace(request.SourceKind), claimSourceKindUser)
	if request.Items != nil {
		request.Items = cloneClaimResponseItems(request.Items)
	}
	if request.UpdatedAtTS <= 0 {
		request.UpdatedAtTS = time.Now().Unix()
	}
	if strings.TrimSpace(request.UpdatedAt) == "" {
		request.UpdatedAt = isoformatFromTS(request.UpdatedAtTS)
	}
	snapshot, err := s.getStoredClaimRealtimeSnapshot(ctx, userID)
	if err != nil {
		return err
	}
	replaced := false
	for index := range snapshot.Requests {
		if snapshot.Requests[index].RequestID != request.RequestID {
			continue
		}
		if sameClaimRealtimeRequestContent(snapshot.Requests[index], request) {
			return nil
		}
		if keepExisting, reason := shouldKeepExistingClaimRealtime(snapshot.Requests[index], request); keepExisting {
			s.logger.Info(
				"drop stale claim realtime update",
				"user_id", userID,
				"request_id", request.RequestID,
				"reason", reason,
				"existing_updated_at_ts", snapshot.Requests[index].UpdatedAtTS,
				"incoming_updated_at_ts", request.UpdatedAtTS,
			)
			return nil
		}
		snapshot.Requests[index] = request
		replaced = true
		break
	}
	if !replaced {
		snapshot.Requests = append(snapshot.Requests, request)
	}
	s.setClaimRealtimeSnapshot(userID, snapshot)
	return nil
}

func (s *Service) buildClaimRealtimeAck(result *claimResult) *claimRequestAccepted {
	if result == nil {
		return nil
	}
	status := claimStatusProcessing
	if result.Queued && isClaimQueuedStatus(result.QueueStatus) {
		status = strings.ToLower(strings.TrimSpace(result.QueueStatus))
	}
	return &claimRequestAccepted{
		RequestID:      result.RequestID,
		Status:         status,
		Queued:         result.Queued,
		QueueID:        result.QueueID,
		QueuePosition:  result.QueuePosition,
		QueueRemaining: result.QueueRemaining,
		BlockReason:    strings.TrimSpace(result.BlockReason),
		LastProgressAt: strings.TrimSpace(result.LastProgressAt),
		NextRetryAt:    strings.TrimSpace(result.NextRetryAt),
		Requested:      result.Requested,
		AcceptedAt:     isoformatNow(),
		Source:         claimSourceSelf,
	}
}

func (s *Service) logClaimRealtimeRequest(message string, userID int64, request claimRealtimeRequest) {
	if s == nil || s.logger == nil {
		return
	}
	s.logger.Info(
		message,
		"request_id",
		request.RequestID,
		"user_id",
		userID,
		"status",
		request.Status,
		"queued",
		request.Queued,
		"queue_id",
		request.QueueID,
		"queue_position",
		request.QueuePosition,
		"queue_total",
		request.QueueTotal,
		"requested",
		request.Requested,
		"granted",
		request.Granted,
		"remaining",
		request.Remaining,
		"source_kind",
		request.SourceKind,
		"reason_code",
		request.ReasonCode,
	)
}

func (s *Service) CreateClaimRequest(ctx context.Context, requestContext *auth.RequestContext, apiKeyID *int64, count int, clientTabID string) (*claimRequestAccepted, error) {
	if requestContext == nil {
		return nil, fmt.Errorf("request context is required")
	}
	result, err := s.claimTokens(ctx, requestContext.UserID, apiKeyID, count, requestContext.SessionID, clientTabID)
	if err != nil {
		return nil, err
	}

	s.logger.Info(
		"claim request created",
		"request_id",
		result.RequestID,
		"user_id",
		requestContext.UserID,
		"session_id",
		requestContext.SessionID,
		"client_tab_id",
		strings.TrimSpace(clientTabID),
		"queued",
		result.Queued,
		"queue_id",
		result.QueueID,
		"queue_position",
		result.QueuePosition,
		"queue_remaining",
		result.QueueRemaining,
		"requested",
		result.Requested,
	)

	if result.Queued {
		if err := s.publishQueuedClaimRequest(ctx, requestContext.UserID, requestContext.SessionID, clientTabID, result); err != nil {
			s.logger.Warn("publish queued claim request", "request_id", result.RequestID, "user_id", requestContext.UserID, "error", err)
		}
	} else {
		if err := s.publishTerminalClaimResult(ctx, requestContext.UserID, requestContext.SessionID, clientTabID, result); err != nil {
			s.logger.Warn("publish terminal claim result", "request_id", result.RequestID, "user_id", requestContext.UserID, "error", err)
		}
	}

	return s.buildClaimRealtimeAck(result), nil
}

func (s *Service) publishQueuedClaimRequest(ctx context.Context, userID int64, sessionID string, tabID string, result *claimResult) error {
	if result == nil {
		return nil
	}
	status := strings.ToLower(strings.TrimSpace(result.QueueStatus))
	if !isClaimQueuedStatus(status) {
		status = claimStatusQueuedWaiting
	}
	request := claimRealtimeRequest{
		RequestID:       result.RequestID,
		Status:          status,
		Requested:       result.Requested,
		Granted:         maxInt(0, result.Requested-result.QueueRemaining),
		Remaining:       result.QueueRemaining,
		Queued:          true,
		QueueID:         result.QueueID,
		QueuePosition:   result.QueuePosition,
		QueueTotal:      result.QueuePosition,
		BlockReason:     strings.TrimSpace(result.BlockReason),
		LastProgressAt:  strings.TrimSpace(result.LastProgressAt),
		NextRetryAt:     strings.TrimSpace(result.NextRetryAt),
		SourceKind:      claimSourceKindUser,
		OriginSessionID: strings.TrimSpace(sessionID),
		OriginTabID:     strings.TrimSpace(tabID),
		UpdatedAtTS:     time.Now().Unix(),
		UpdatedAt:       isoformatNow(),
		Terminal:        false,
	}
	if err := s.upsertClaimRealtimeRequest(ctx, userID, request); err != nil {
		return err
	}
	s.logClaimRealtimeRequest("claim request queued", userID, request)
	return nil
}

func claimTerminalStatusFromResult(result *claimResult) string {
	if result == nil {
		return claimStatusFailed
	}
	if result.Granted <= 0 {
		return claimStatusFailed
	}
	if result.Granted < result.Requested {
		return claimStatusPartial
	}
	return claimStatusSucceeded
}

func terminalReasonMessage(status string, reasonCode string) string {
	trimmed := strings.TrimSpace(reasonCode)
	switch strings.ToLower(strings.TrimSpace(status)) {
	case claimStatusCancelled:
		if trimmed == queueCancelReasonStartupReset {
			return "服务重启，未完成的排队请求已结束。"
		}
		if trimmed != "" {
			return fmt.Sprintf("领取请求已取消：%s", trimmed)
		}
		return "领取请求已取消。"
	case claimStatusExpired:
		if trimmed != "" {
			return fmt.Sprintf("领取请求已过期：%s", trimmed)
		}
		return "领取请求已过期。"
	case claimStatusFailed:
		if trimmed != "" {
			return fmt.Sprintf("领取请求失败：%s", trimmed)
		}
		return "领取请求失败。"
	case claimStatusPartial:
		return "领取请求部分完成。"
	default:
		return ""
	}
}

func queueEntrySourceKind(entry userQueueEntry) string {
	switch strings.ToLower(strings.TrimSpace(entry.Status)) {
	case queueStatusCancelled, queueStatusExpired, queueStatusFailed:
		return claimSourceKindSystem
	default:
		return claimSourceKindUser
	}
}

func claimRealtimeStatusFromQueueEntry(entry userQueueEntry) string {
	granted := maxInt(0, entry.Requested-entry.Remaining)
	switch strings.ToLower(strings.TrimSpace(entry.Status)) {
	case queueStatusQueued:
		return claimStatusQueuedWaiting
	case queueStatusQueuedWaiting:
		return claimStatusQueuedWaiting
	case queueStatusQueuedBlocked:
		return claimStatusQueuedBlocked
	case queueStatusSucceeded:
		return claimStatusSucceeded
	case queueStatusPartial:
		return claimStatusPartial
	case queueStatusFailed:
		if granted > 0 {
			return claimStatusPartial
		}
		return claimStatusFailed
	case queueStatusCancelled:
		if granted > 0 {
			return claimStatusPartial
		}
		return claimStatusCancelled
	case queueStatusExpired:
		if granted > 0 {
			return claimStatusPartial
		}
		return claimStatusExpired
	default:
		if granted > 0 && entry.Remaining <= 0 {
			return claimStatusSucceeded
		}
		return claimStatusFailed
	}
}

func claimRealtimeUpdatedAtTSFromQueueEntry(entry userQueueEntry) int64 {
	updatedAtTS := entry.EnqueuedAtTS
	if entry.LastProgressAtTS.Valid && entry.LastProgressAtTS.Int64 > updatedAtTS {
		updatedAtTS = entry.LastProgressAtTS.Int64
	}
	if entry.CancelledAtTS.Valid && entry.CancelledAtTS.Int64 > updatedAtTS {
		updatedAtTS = entry.CancelledAtTS.Int64
	}
	if entry.TerminalAtTS.Valid && entry.TerminalAtTS.Int64 > updatedAtTS {
		updatedAtTS = entry.TerminalAtTS.Int64
	}
	return updatedAtTS
}

func claimRealtimeRequestFromQueueEntry(entry userQueueEntry, queueTotal int, items []claimResponseItem) claimRealtimeRequest {
	status := claimRealtimeStatusFromQueueEntry(entry)
	request := claimRealtimeRequest{
		RequestID:       entry.RequestID,
		Status:          status,
		Requested:       entry.Requested,
		Granted:         maxInt(0, entry.Requested-entry.Remaining),
		Remaining:       maxInt(0, entry.Remaining),
		Queued:          isClaimQueuedStatus(status),
		QueueID:         entry.ID,
		BlockReason:     strings.TrimSpace(entry.BlockReason.String),
		LastProgressAt:  isoformatFromNullableTS(entry.LastProgressAtTS),
		NextRetryAt:     isoformatFromNullableTS(entry.NextRetryAtTS),
		Items:           cloneClaimResponseItems(items),
		ReasonCode:      strings.TrimSpace(entry.CancelReason.String),
		ReasonMessage:   "",
		SourceKind:      queueEntrySourceKind(entry),
		OriginSessionID: strings.TrimSpace(entry.OriginSessionID.String),
		OriginTabID:     strings.TrimSpace(entry.OriginTabID.String),
		EnqueuedAt:      isoformatFromTS(entry.EnqueuedAtTS),
		UpdatedAtTS:     claimRealtimeUpdatedAtTSFromQueueEntry(entry),
		Terminal:        !isClaimQueuedStatus(status),
	}
	if request.Queued {
		request.QueuePosition = entry.QueueRank
		request.QueueTotal = queueTotal
	}
	if request.UpdatedAtTS <= 0 {
		request.UpdatedAtTS = time.Now().Unix()
	}
	request.UpdatedAt = isoformatFromTS(request.UpdatedAtTS)
	if request.Terminal {
		request.ReasonMessage = terminalReasonMessage(request.Status, request.ReasonCode)
	}
	return request
}

func (s *Service) publishTerminalClaimResult(ctx context.Context, userID int64, sessionID string, tabID string, result *claimResult) error {
	if result == nil {
		return nil
	}
	status := claimTerminalStatusFromResult(result)
	request := claimRealtimeRequest{
		RequestID:       result.RequestID,
		Status:          status,
		Requested:       result.Requested,
		Granted:         result.Granted,
		Remaining:       maxInt(0, result.Requested-result.Granted),
		Queued:          false,
		Items:           cloneClaimResponseItems(result.Items),
		ReasonMessage:   terminalReasonMessage(status, ""),
		SourceKind:      claimSourceKindUser,
		OriginSessionID: strings.TrimSpace(sessionID),
		OriginTabID:     strings.TrimSpace(tabID),
		UpdatedAtTS:     time.Now().Unix(),
		UpdatedAt:       isoformatNow(),
		Terminal:        true,
	}
	if err := s.upsertClaimRealtimeRequest(ctx, userID, request); err != nil {
		return err
	}
	s.logClaimRealtimeRequest("claim request completed", userID, request)
	return nil
}

func (s *Service) publishQueueCompletion(ctx context.Context, entry userQueueEntry) error {
	if entry.UserID <= 0 || strings.TrimSpace(entry.RequestID) == "" {
		return nil
	}
	items, err := s.listClaimItemsByRequestID(ctx, entry.UserID, entry.RequestID)
	if err != nil {
		return err
	}
	nowTS := time.Now().Unix()
	entry.Status = queueStatusSucceeded
	entry.Remaining = 0
	entry.QueueRank = 0
	entry.BlockReason = sql.NullString{}
	entry.NextRetryAtTS = sql.NullInt64{}
	entry.LastProgressAtTS = sql.NullInt64{Int64: nowTS, Valid: true}
	entry.TerminalAtTS = sql.NullInt64{Int64: nowTS, Valid: true}
	entry.CancelReason = sql.NullString{}
	entry.CancelledAtTS = sql.NullInt64{}
	request := claimRealtimeRequestFromQueueEntry(entry, 0, items)
	if err := s.upsertClaimRealtimeRequest(ctx, entry.UserID, request); err != nil {
		return err
	}
	s.logClaimRealtimeRequest("queued claim request completed", entry.UserID, request)
	return nil
}

func (s *Service) publishQueueProgress(ctx context.Context, entry userQueueEntry, remainingAfter int, queueTotal int) error {
	if entry.UserID <= 0 || strings.TrimSpace(entry.RequestID) == "" || remainingAfter <= 0 {
		return nil
	}
	items, err := s.listClaimItemsByRequestID(ctx, entry.UserID, entry.RequestID)
	if err != nil {
		return err
	}
	nowTS := time.Now().Unix()
	entry.Status = queueStatusQueuedWaiting
	entry.Remaining = remainingAfter
	entry.QueueRank = maxInt(1, entry.QueueRank)
	entry.BlockReason = sql.NullString{}
	entry.NextRetryAtTS = sql.NullInt64{}
	entry.LastProgressAtTS = sql.NullInt64{Int64: nowTS, Valid: true}
	entry.TerminalAtTS = sql.NullInt64{}
	entry.CancelReason = sql.NullString{}
	entry.CancelledAtTS = sql.NullInt64{}
	request := claimRealtimeRequestFromQueueEntry(entry, maxInt(queueTotal, entry.QueueRank), items)
	if err := s.upsertClaimRealtimeRequest(ctx, entry.UserID, request); err != nil {
		return err
	}
	s.logClaimRealtimeRequest("queued claim request progress", entry.UserID, request)
	return nil
}

func (s *Service) listClaimItemsByRequestID(ctx context.Context, userID int64, requestID string) ([]claimResponseItem, error) {
	if userID <= 0 || strings.TrimSpace(requestID) == "" {
		return []claimResponseItem{}, nil
	}
	rows, err := s.store.DB().QueryContext(ctx, `
		SELECT token_claims.id,
		       token_claims.token_id,
		       COALESCE(token_claims.claim_file_name, tokens.file_name) AS file_name,
		       COALESCE(token_claims.claim_file_path, tokens.file_path) AS file_path,
		       COALESCE(token_claims.claim_encoding, tokens.encoding) AS encoding,
		       COALESCE(token_claims.claim_content_json, tokens.content_json) AS content_json
		FROM token_claims
		LEFT JOIN tokens ON tokens.id = token_claims.token_id
		WHERE token_claims.user_id = ? AND token_claims.request_id = ? AND token_claims.is_hidden = 0
		ORDER BY token_claims.id ASC
	`, userID, requestID)
	if err != nil {
		return nil, fmt.Errorf("list claim items by request id %q: %w", requestID, err)
	}
	defer rows.Close()

	items := make([]claimResponseItem, 0)
	for rows.Next() {
		var (
			item        claimResponseItem
			claimID     int64
			contentJSON string
		)
		if err := rows.Scan(&claimID, &item.TokenID, &item.FileName, &item.FilePath, &item.Encoding, &contentJSON); err != nil {
			return nil, fmt.Errorf("scan claim item by request id %q: %w", requestID, err)
		}
		content, err := decodeJSONContent(contentJSON)
		if err != nil {
			return nil, err
		}
		item.ClaimID = claimID
		item.Content = content
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claim items by request id %q: %w", requestID, err)
	}
	return items, nil
}

func (s *Service) publishQueueTerminalState(ctx context.Context, entry userQueueEntry, status string, reasonCode string) error {
	if entry.UserID <= 0 || strings.TrimSpace(entry.RequestID) == "" {
		return nil
	}
	items, err := s.listClaimItemsByRequestID(ctx, entry.UserID, entry.RequestID)
	if err != nil {
		return err
	}
	nowTS := time.Now().Unix()
	entry.Status = strings.ToLower(strings.TrimSpace(status))
	entry.QueueRank = 0
	entry.BlockReason = sql.NullString{}
	entry.NextRetryAtTS = sql.NullInt64{}
	entry.CancelReason = sql.NullString{String: strings.TrimSpace(reasonCode), Valid: strings.TrimSpace(reasonCode) != ""}
	entry.CancelledAtTS = sql.NullInt64{Int64: nowTS, Valid: true}
	entry.TerminalAtTS = sql.NullInt64{Int64: nowTS, Valid: true}
	request := claimRealtimeRequestFromQueueEntry(entry, 0, items)
	if err := s.upsertClaimRealtimeRequest(ctx, entry.UserID, request); err != nil {
		return err
	}
	s.logClaimRealtimeRequest("queued claim request closed", entry.UserID, request)
	s.publishAdminQueueTerminal(entry, status, reasonCode)
	return nil
}

func (s *Service) syncClaimRealtimeQueueStatus(ctx context.Context, userID int64, status queueStatusPayload) error {
	if userID <= 0 || !status.Queued || strings.TrimSpace(status.RequestID) == "" {
		return nil
	}
	existing, err := s.findClaimRealtimeRequest(ctx, userID, status.RequestID)
	if err != nil {
		return err
	}
	request := claimRealtimeRequest{
		RequestID:       status.RequestID,
		Status:          firstNonEmpty(strings.ToLower(strings.TrimSpace(status.Status)), claimStatusQueuedWaiting),
		Requested:       status.Requested,
		Granted:         maxInt(0, status.Requested-status.Remaining),
		Remaining:       status.Remaining,
		Queued:          true,
		QueueID:         status.QueueID,
		QueuePosition:   status.Position,
		QueueTotal:      status.TotalQueued,
		BlockReason:     strings.TrimSpace(status.BlockReason),
		LastProgressAt:  strings.TrimSpace(status.LastProgressAt),
		NextRetryAt:     strings.TrimSpace(status.NextRetryAt),
		EnqueuedAt:      status.EnqueuedAt,
		SourceKind:      claimSourceKindUser,
		UpdatedAtTS:     time.Now().Unix(),
		UpdatedAt:       isoformatNow(),
		Terminal:        false,
		OriginSessionID: "",
		OriginTabID:     "",
	}
	if existing != nil {
		request.SourceKind = firstNonEmpty(existing.SourceKind, claimSourceKindUser)
		request.OriginSessionID = existing.OriginSessionID
		request.OriginTabID = existing.OriginTabID
		request.Items = cloneClaimResponseItems(existing.Items)
	}
	positionChanged := existing == nil ||
		existing.Status != request.Status ||
		existing.QueueID != request.QueueID ||
		existing.QueuePosition != request.QueuePosition ||
		existing.QueueTotal != request.QueueTotal ||
		existing.Remaining != request.Remaining ||
		existing.BlockReason != request.BlockReason ||
		existing.LastProgressAt != request.LastProgressAt ||
		existing.NextRetryAt != request.NextRetryAt
	if err := s.upsertClaimRealtimeRequest(ctx, userID, request); err != nil {
		return err
	}
	if positionChanged {
		s.logClaimRealtimeRequest("claim queue position updated", userID, request)
	}
	return nil
}

func (s *Service) rebuildClaimRealtimeSnapshot(ctx context.Context, userID int64) (claimRealtimeSnapshot, error) {
	snapshot := s.emptyClaimRealtimeSnapshot()
	if userID <= 0 {
		return snapshot, nil
	}

	requestsByID := make(map[string]claimRealtimeRequest)
	claims, err := s.ListClaims(ctx, userID)
	if err != nil {
		return claimRealtimeSnapshot{}, err
	}
	itemsByRequestID := make(map[string][]claimResponseItem)
	for _, item := range claims {
		if strings.TrimSpace(item.RequestID) == "" {
			continue
		}
		itemsByRequestID[item.RequestID] = append(itemsByRequestID[item.RequestID], claimResponseItem{
			ClaimID:     item.ClaimID,
			TokenID:     item.TokenID,
			FileName:    item.FileName,
			FilePath:    item.FilePath,
			Encoding:    item.Encoding,
			Content:     item.Content,
			DownloadURL: item.DownloadURL,
		})
	}

	queueTotal, err := s.getTotalQueued(ctx, s.store.DB())
	if err != nil {
		return claimRealtimeSnapshot{}, err
	}

	requestRows, err := s.listRecentClaimRequestEntries(ctx, userID, claimRealtimeMaxRequests)
	if err != nil {
		return claimRealtimeSnapshot{}, err
	}
	for _, row := range requestRows {
		if strings.TrimSpace(row.RequestID) == "" {
			continue
		}
		requestsByID[row.RequestID] = claimRealtimeRequestFromQueueEntry(row, queueTotal, itemsByRequestID[row.RequestID])
	}

	// Compatibility fallback for legacy rows created before claim_queue became the request ledger.
	for requestID, items := range itemsByRequestID {
		if strings.TrimSpace(requestID) == "" {
			continue
		}
		if _, exists := requestsByID[requestID]; exists {
			continue
		}
		request := claimRealtimeRequest{
			RequestID:     requestID,
			Status:        claimStatusSucceeded,
			Requested:     len(items),
			Granted:       len(items),
			Remaining:     0,
			Queued:        false,
			Items:         cloneClaimResponseItems(items),
			SourceKind:    claimSourceKindUser,
			UpdatedAtTS:   0,
			UpdatedAt:     "",
			Terminal:      true,
			ReasonMessage: terminalReasonMessage(claimStatusSucceeded, ""),
		}
		for _, claimItem := range claims {
			if claimItem.RequestID != requestID {
				continue
			}
			request.UpdatedAtTS = maxClaimRealtimeInt64(request.UpdatedAtTS, parseISOUnix(claimItem.ClaimedAt))
		}
		if request.UpdatedAtTS <= 0 {
			request.UpdatedAtTS = time.Now().Unix()
		}
		request.UpdatedAt = isoformatFromTS(request.UpdatedAtTS)
		requestsByID[requestID] = request
	}

	snapshot.Requests = make([]claimRealtimeRequest, 0, len(requestsByID))
	for _, item := range requestsByID {
		if item.RequestID == "" {
			continue
		}
		if item.Requested <= 0 {
			item.Requested = maxInt(item.Granted, item.Requested)
		}
		if item.UpdatedAtTS <= 0 {
			item.UpdatedAtTS = time.Now().Unix()
		}
		if strings.TrimSpace(item.UpdatedAt) == "" {
			item.UpdatedAt = isoformatFromTS(item.UpdatedAtTS)
		}
		if item.Terminal && item.Status == claimStatusCancelled && item.Granted > 0 {
			item.Status = claimStatusPartial
			item.ReasonMessage = terminalReasonMessage(item.Status, item.ReasonCode)
		}
		snapshot.Requests = append(snapshot.Requests, item)
	}
	snapshot.Requests = trimClaimRealtimeRequests(snapshot.Requests)
	return snapshot, nil
}

func parseISOUnix(value string) int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return 0
	}
	return parsed.Unix()
}

func maxClaimRealtimeInt64(left int64, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func (s *Service) listRecentClaimRequestEntries(ctx context.Context, userID int64, limit int) ([]userQueueEntry, error) {
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
		WHERE user_id = ?
		  AND (
		       (status IN (?, ?, ?) AND remaining > 0)
		       OR status IN (?, ?, ?, ?, ?)
		  )
		ORDER BY
		       CASE WHEN status IN (?, ?, ?) AND remaining > 0 THEN 0 ELSE 1 END ASC,
		       CASE WHEN status IN (?, ?, ?) AND remaining > 0 THEN enqueued_at_ts ELSE 0 END ASC,
		       CASE WHEN status IN (?, ?, ?) AND remaining > 0 THEN id ELSE 0 END ASC,
		       COALESCE(terminal_at_ts, cancelled_at_ts, last_progress_at_ts, enqueued_at_ts) DESC,
		       id DESC
		LIMIT ?
	`, userID,
		queueStatusQueued, queueStatusQueuedWaiting, queueStatusQueuedBlocked,
		queueStatusSucceeded, queueStatusPartial, queueStatusFailed, queueStatusCancelled, queueStatusExpired,
		queueStatusQueued, queueStatusQueuedWaiting, queueStatusQueuedBlocked,
		queueStatusQueued, queueStatusQueuedWaiting, queueStatusQueuedBlocked,
		queueStatusQueued, queueStatusQueuedWaiting, queueStatusQueuedBlocked,
		maxInt(1, limit+1))
	if err != nil {
		return nil, fmt.Errorf("list recent claim request entries: %w", err)
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
			return nil, fmt.Errorf("scan recent claim request entry: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent claim request entries: %w", err)
	}
	for index := range items {
		position, err := s.lookupQueuePosition(ctx, s.store.DB(), items[index])
		if err != nil {
			return nil, err
		}
		items[index].QueueRank = position
	}
	return items, nil
}
