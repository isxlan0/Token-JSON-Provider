package claim

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"token-atlas/internal/auth"
)

const (
	claimStatusProcessing = "processing"
	claimStatusQueued     = "queued"
	claimStatusSucceeded  = "succeeded"
	claimStatusPartial    = "partial"
	claimStatusFailed     = "failed"
	claimStatusCancelled  = "cancelled"
	claimStatusExpired    = "expired"

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
		left.ReasonCode == right.ReasonCode &&
		left.ReasonMessage == right.ReasonMessage &&
		left.SourceKind == right.SourceKind &&
		left.OriginSessionID == right.OriginSessionID &&
		left.OriginTabID == right.OriginTabID &&
		left.EnqueuedAt == right.EnqueuedAt &&
		left.Terminal == right.Terminal &&
		reflect.DeepEqual(left.Items, right.Items)
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
	request.Queued = request.Status == claimStatusQueued
	request.Terminal = isClaimTerminalStatus(request.Status)
	request.SourceKind = firstNonEmpty(strings.TrimSpace(request.SourceKind), claimSourceKindUser)
	if request.Items != nil {
		request.Items = cloneClaimResponseItems(request.Items)
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
		if request.UpdatedAtTS <= 0 {
			request.UpdatedAtTS = time.Now().Unix()
		}
		if strings.TrimSpace(request.UpdatedAt) == "" {
			request.UpdatedAt = isoformatFromTS(request.UpdatedAtTS)
		}
		snapshot.Requests[index] = request
		replaced = true
		break
	}
	if !replaced {
		if request.UpdatedAtTS <= 0 {
			request.UpdatedAtTS = time.Now().Unix()
		}
		if strings.TrimSpace(request.UpdatedAt) == "" {
			request.UpdatedAt = isoformatFromTS(request.UpdatedAtTS)
		}
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
	if result.Queued {
		status = claimStatusQueued
	}
	return &claimRequestAccepted{
		RequestID:      result.RequestID,
		Status:         status,
		Queued:         result.Queued,
		QueueID:        result.QueueID,
		QueuePosition:  result.QueuePosition,
		QueueRemaining: result.QueueRemaining,
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
	result, err := s.ClaimTokens(ctx, requestContext.UserID, apiKeyID, count)
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
	queueTotal := result.QueuePosition
	if queueStatus, err := s.GetQueueStatus(ctx, userID); err == nil && queueStatus.Queued && queueStatus.RequestID == result.RequestID {
		queueTotal = queueStatus.TotalQueued
	}
	request := claimRealtimeRequest{
		RequestID:       result.RequestID,
		Status:          claimStatusQueued,
		Requested:       result.Requested,
		Granted:         maxInt(0, result.Requested-result.QueueRemaining),
		Remaining:       result.QueueRemaining,
		Queued:          true,
		QueueID:         result.QueueID,
		QueuePosition:   result.QueuePosition,
		QueueTotal:      queueTotal,
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
	existing, err := s.findClaimRealtimeRequest(ctx, entry.UserID, entry.RequestID)
	if err != nil {
		return err
	}
	items, err := s.listClaimItemsByRequestID(ctx, entry.UserID, entry.RequestID)
	if err != nil {
		return err
	}
	requested := entry.Requested
	sourceKind := claimSourceKindUser
	originSessionID := ""
	originTabID := ""
	if existing != nil {
		if existing.Requested > 0 {
			requested = existing.Requested
		}
		sourceKind = firstNonEmpty(existing.SourceKind, claimSourceKindUser)
		originSessionID = existing.OriginSessionID
		originTabID = existing.OriginTabID
	}
	status := claimStatusSucceeded
	if len(items) < requested {
		status = claimStatusPartial
	}
	request := claimRealtimeRequest{
		RequestID:       entry.RequestID,
		Status:          status,
		Requested:       requested,
		Granted:         len(items),
		Remaining:       maxInt(0, requested-len(items)),
		Queued:          false,
		Items:           cloneClaimResponseItems(items),
		ReasonMessage:   terminalReasonMessage(status, ""),
		SourceKind:      sourceKind,
		OriginSessionID: originSessionID,
		OriginTabID:     originTabID,
		UpdatedAtTS:     time.Now().Unix(),
		UpdatedAt:       isoformatNow(),
		Terminal:        true,
	}
	if err := s.upsertClaimRealtimeRequest(ctx, entry.UserID, request); err != nil {
		return err
	}
	s.logClaimRealtimeRequest("queued claim request completed", entry.UserID, request)
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
	existing, err := s.findClaimRealtimeRequest(ctx, entry.UserID, entry.RequestID)
	if err != nil {
		return err
	}
	items, err := s.listClaimItemsByRequestID(ctx, entry.UserID, entry.RequestID)
	if err != nil {
		return err
	}
	granted := len(items)
	requested := entry.Requested
	if existing != nil && existing.Requested > 0 {
		requested = existing.Requested
	}
	remaining := maxInt(0, requested-granted)
	finalStatus := strings.ToLower(strings.TrimSpace(status))
	if (finalStatus == claimStatusCancelled || finalStatus == claimStatusExpired) && granted > 0 {
		finalStatus = claimStatusPartial
	}

	request := claimRealtimeRequest{
		RequestID:       entry.RequestID,
		Status:          finalStatus,
		Requested:       requested,
		Granted:         granted,
		Remaining:       remaining,
		Queued:          false,
		Items:           cloneClaimResponseItems(items),
		ReasonCode:      strings.TrimSpace(reasonCode),
		ReasonMessage:   terminalReasonMessage(finalStatus, reasonCode),
		SourceKind:      claimSourceKindSystem,
		UpdatedAtTS:     time.Now().Unix(),
		UpdatedAt:       isoformatNow(),
		Terminal:        true,
		OriginSessionID: "",
		OriginTabID:     "",
	}
	if existing != nil {
		request.SourceKind = firstNonEmpty(existing.SourceKind, claimSourceKindSystem)
		request.OriginSessionID = existing.OriginSessionID
		request.OriginTabID = existing.OriginTabID
	}
	if err := s.upsertClaimRealtimeRequest(ctx, entry.UserID, request); err != nil {
		return err
	}
	s.logClaimRealtimeRequest("queued claim request closed", entry.UserID, request)
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
		Status:          claimStatusQueued,
		Requested:       status.Requested,
		Granted:         maxInt(0, status.Requested-status.Remaining),
		Remaining:       status.Remaining,
		Queued:          true,
		QueueID:         status.QueueID,
		QueuePosition:   status.Position,
		QueueTotal:      status.TotalQueued,
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
		existing.Remaining != request.Remaining
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

	if queueStatus, err := s.loadQueueStatusSnapshot(ctx, userID); err == nil && queueStatus.Queued && strings.TrimSpace(queueStatus.RequestID) != "" {
		requestsByID[queueStatus.RequestID] = claimRealtimeRequest{
			RequestID:     queueStatus.RequestID,
			Status:        claimStatusQueued,
			Requested:     queueStatus.Requested,
			Granted:       maxInt(0, queueStatus.Requested-queueStatus.Remaining),
			Remaining:     queueStatus.Remaining,
			Queued:        true,
			QueueID:       queueStatus.QueueID,
			QueuePosition: queueStatus.Position,
			QueueTotal:    queueStatus.TotalQueued,
			EnqueuedAt:    queueStatus.EnqueuedAt,
			SourceKind:    claimSourceKindUser,
			UpdatedAtTS:   time.Now().Unix(),
			UpdatedAt:     isoformatNow(),
			Terminal:      false,
		}
	} else if err != nil {
		return claimRealtimeSnapshot{}, err
	}

	claims, err := s.ListClaims(ctx, userID)
	if err != nil {
		return claimRealtimeSnapshot{}, err
	}
	for _, item := range claims {
		if strings.TrimSpace(item.RequestID) == "" {
			continue
		}
		request := requestsByID[item.RequestID]
		request.RequestID = item.RequestID
		request.Granted++
		request.SourceKind = firstNonEmpty(request.SourceKind, claimSourceKindUser)
		request.Items = append(request.Items, claimResponseItem{
			ClaimID:     item.ClaimID,
			TokenID:     item.TokenID,
			FileName:    item.FileName,
			FilePath:    item.FilePath,
			Encoding:    item.Encoding,
			Content:     item.Content,
			DownloadURL: item.DownloadURL,
		})
		request.UpdatedAt = item.ClaimedAt
		request.UpdatedAtTS = maxClaimRealtimeInt64(request.UpdatedAtTS, parseISOUnix(item.ClaimedAt))
		if request.Status != claimStatusQueued {
			request.Status = claimStatusSucceeded
			request.Queued = false
			request.Terminal = true
		}
		requestsByID[item.RequestID] = request
	}

	terminalRows, err := s.listRecentQueueTerminalEntries(ctx, userID, claimRealtimeMaxRequests)
	if err != nil {
		return claimRealtimeSnapshot{}, err
	}
	for _, row := range terminalRows {
		request := requestsByID[row.RequestID]
		if request.RequestID == "" {
			request.RequestID = row.RequestID
			request.Requested = row.Requested
			request.Remaining = row.Remaining
			request.Granted = maxInt(0, row.Requested-row.Remaining)
			request.SourceKind = claimSourceKindSystem
			request.UpdatedAtTS = row.CancelledAtTS.Int64
			request.UpdatedAt = isoformatFromTS(row.CancelledAtTS.Int64)
			request.Terminal = true
			if row.Status == queueStatusExpired {
				request.Status = claimStatusExpired
			} else {
				request.Status = claimStatusCancelled
			}
			request.ReasonCode = strings.TrimSpace(row.CancelReason.String)
			request.ReasonMessage = terminalReasonMessage(request.Status, request.ReasonCode)
			requestsByID[row.RequestID] = request
			continue
		}
		if request.Terminal {
			continue
		}
		request.Terminal = true
		if row.Status == queueStatusExpired {
			request.Status = claimStatusExpired
		} else {
			request.Status = claimStatusCancelled
		}
		request.ReasonCode = strings.TrimSpace(row.CancelReason.String)
		request.ReasonMessage = terminalReasonMessage(request.Status, request.ReasonCode)
		request.UpdatedAtTS = maxClaimRealtimeInt64(request.UpdatedAtTS, row.CancelledAtTS.Int64)
		request.UpdatedAt = isoformatFromTS(request.UpdatedAtTS)
		requestsByID[row.RequestID] = request
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

func (s *Service) listRecentQueueTerminalEntries(ctx context.Context, userID int64, limit int) ([]userQueueEntry, error) {
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
		WHERE user_id = ?
		  AND status IN (?, ?)
		ORDER BY COALESCE(cancelled_at_ts, enqueued_at_ts) DESC, id DESC
		LIMIT ?
	`, userID, queueStatusCancelled, queueStatusExpired, maxInt(1, limit))
	if err != nil {
		return nil, fmt.Errorf("list recent queue terminal entries: %w", err)
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
			return nil, fmt.Errorf("scan recent queue terminal entry: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent queue terminal entries: %w", err)
	}
	return items, nil
}
