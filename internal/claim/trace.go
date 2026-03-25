package claim

import (
	"strings"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
)

type debugSettingsPayload struct {
	ClaimTrace bool   `json:"claim_trace"`
	LogLevel   string `json:"log_level,omitempty"`
}

func (s *Service) debugSettingsPayload() debugSettingsPayload {
	level := "info"
	if s != nil {
		level = strings.ToLower(strings.TrimSpace(s.cfg.Logging.Level))
		if level == "" {
			level = "info"
		}
	}
	return debugSettingsPayload{
		ClaimTrace: s.claimTraceEnabled(),
		LogLevel:   level,
	}
}

func (s *Service) claimTraceEnabled() bool {
	return s != nil && s.cfg.Logging.ClaimTrace
}

func (s *Service) claimTrace(message string, args ...any) {
	if s == nil || s.logger == nil || !s.claimTraceEnabled() {
		return
	}
	prefixed := make([]any, 0, len(args)+2)
	prefixed = append(prefixed, "trace", "claim")
	prefixed = append(prefixed, args...)
	s.logger.Info(message, prefixed...)
}

func (s *Service) claimTraceWithDB(message string, args ...any) {
	if s == nil || !s.claimTraceEnabled() {
		return
	}
	withDB := make([]any, 0, len(args)+8)
	withDB = append(withDB, args...)
	withDB = append(withDB, s.dbStatsLogArgs()...)
	s.claimTrace(message, withDB...)
}

func claimTraceRequestContextArgs(requestContext *auth.RequestContext) []any {
	if requestContext == nil {
		return []any{
			"user_id", int64(0),
			"session_id", "",
			"is_admin", false,
		}
	}
	return []any{
		"user_id", requestContext.UserID,
		"session_id", strings.TrimSpace(requestContext.SessionID),
		"is_admin", requestContext.IsAdmin,
	}
}

func claimTraceQueueEntrySummary(entry userQueueEntry) map[string]any {
	return map[string]any{
		"queue_id":          entry.ID,
		"request_id":        entry.RequestID,
		"user_id":           entry.UserID,
		"status":            strings.ToLower(strings.TrimSpace(entry.Status)),
		"requested":         entry.Requested,
		"remaining":         entry.Remaining,
		"queue_position":    entry.QueueRank,
		"block_reason":      strings.TrimSpace(entry.BlockReason.String),
		"last_error_reason": strings.TrimSpace(entry.LastErrorReason.String),
	}
}

func claimTraceClaimResultSummary(result *claimResult) map[string]any {
	if result == nil {
		return map[string]any{}
	}
	terminalStatus := ""
	terminalReason := ""
	if !result.Queued {
		terminalStatus = claimResultTerminalStatus(result)
		terminalReason = claimResultTerminalReason(result)
	}
	return map[string]any{
		"request_id":      result.RequestID,
		"queued":          result.Queued,
		"queue_status":    strings.ToLower(strings.TrimSpace(result.QueueStatus)),
		"terminal_status": terminalStatus,
		"queue_id":        result.QueueID,
		"queue_position":  result.QueuePosition,
		"queue_total":     result.QueueTotal,
		"queue_remaining": result.QueueRemaining,
		"requested":       result.Requested,
		"granted":         result.Granted,
		"block_reason":    strings.TrimSpace(result.BlockReason),
		"terminal_reason": terminalReason,
		"item_count":      len(result.Items),
	}
}

func claimTraceRealtimeRequestsSummary(items []claimRealtimeRequest) []map[string]any {
	if len(items) == 0 {
		return []map[string]any{}
	}
	limit := len(items)
	if limit > 5 {
		limit = 5
	}
	summary := make([]map[string]any, 0, limit)
	for index := 0; index < limit; index++ {
		item := items[index]
		summary = append(summary, map[string]any{
			"request_id":     item.RequestID,
			"status":         strings.ToLower(strings.TrimSpace(item.Status)),
			"queued":         item.Queued,
			"terminal":       item.Terminal,
			"requested":      item.Requested,
			"granted":        item.Granted,
			"remaining":      item.Remaining,
			"queue_id":       item.QueueID,
			"queue_position": item.QueuePosition,
			"queue_total":    item.QueueTotal,
			"block_reason":   strings.TrimSpace(item.BlockReason),
		})
	}
	return summary
}

func claimTraceClaimAcceptedSummary(result *claimRequestAccepted) map[string]any {
	if result == nil {
		return map[string]any{}
	}
	return map[string]any{
		"request_id":      result.RequestID,
		"status":          strings.ToLower(strings.TrimSpace(result.Status)),
		"queued":          result.Queued,
		"requested":       result.Requested,
		"granted":         result.Granted,
		"remaining":       result.Remaining,
		"queue_id":        result.QueueID,
		"queue_position":  result.QueuePosition,
		"queue_total":     result.QueueTotal,
		"queue_remaining": result.QueueRemaining,
		"block_reason":    strings.TrimSpace(result.BlockReason),
		"terminal":        result.Terminal,
	}
}

func claimTraceRealtimeSnapshotSummary(snapshot claimRealtimeSnapshot) map[string]any {
	return map[string]any{
		"request_count":     len(snapshot.Requests),
		"stream_required":   snapshot.StreamRequired,
		"transport":         strings.TrimSpace(snapshot.Transport),
		"generated_at":      strings.TrimSpace(snapshot.GeneratedAt),
		"request_summaries": claimTraceRealtimeRequestsSummary(snapshot.Requests),
	}
}

func claimTraceEchoRequestArgs(c echo.Context) []any {
	if c == nil || c.Request() == nil {
		return nil
	}
	request := c.Request()
	return []any{
		"method", request.Method,
		"path", c.Path(),
		"url_path", request.URL.Path,
		"client_build", strings.TrimSpace(request.Header.Get("X-App-Build")),
		"client_entry", strings.TrimSpace(request.Header.Get("X-App-Entry")),
		"user_agent", strings.TrimSpace(request.Header.Get("User-Agent")),
		"remote_ip", strings.TrimSpace(c.RealIP()),
	}
}
