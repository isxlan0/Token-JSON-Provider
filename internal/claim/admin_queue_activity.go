package claim

import (
	"fmt"
	"strings"
	"time"
)

const (
	adminQueueActivityMaxEntries   = 200
	adminQueueActivityDefaultLimit = 40
	adminQueueActivityBrokerUserID = int64(0)
)

type adminQueueActivityEntry struct {
	Sequence      int64  `json:"sequence"`
	Kind          string `json:"kind"`
	Stage         string `json:"stage"`
	Message       string `json:"message"`
	Detail        string `json:"detail,omitempty"`
	QueueID       int64  `json:"queue_id,omitempty"`
	UserID        int64  `json:"user_id,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	Status        string `json:"status,omitempty"`
	BlockReason   string `json:"block_reason,omitempty"`
	QueuePosition int    `json:"queue_position,omitempty"`
	Remaining     int    `json:"remaining,omitempty"`
	Granted       int    `json:"granted,omitempty"`
	NextRetryAt   string `json:"next_retry_at,omitempty"`
	CreatedAt     string `json:"created_at"`
	CreatedAtTS   int64  `json:"created_at_ts"`
}

type adminQueueActivitySnapshot struct {
	Items       []adminQueueActivityEntry `json:"items"`
	Total       int                       `json:"total"`
	GeneratedAt string                    `json:"generated_at"`
}

type adminQueueActivityEvent struct {
	Kind          string
	Stage         string
	Message       string
	Detail        string
	QueueID       int64
	UserID        int64
	RequestID     string
	Status        string
	BlockReason   string
	QueuePosition int
	Remaining     int
	Granted       int
	NextRetryAtTS int64
}

func normalizeAdminQueueActivityKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "success", "warn", "error":
		return strings.ToLower(strings.TrimSpace(kind))
	default:
		return "info"
	}
}

func truncateAdminQueueActivityText(text string, limit int) string {
	trimmed := strings.TrimSpace(text)
	runes := []rune(trimmed)
	if limit <= 0 || len(runes) <= limit {
		return trimmed
	}
	if limit <= 3 {
		return string(runes[:limit])
	}
	return string(runes[:limit-3]) + "..."
}

func adminQueueActivityEquals(left adminQueueActivityEntry, right adminQueueActivityEntry) bool {
	return left.Kind == right.Kind &&
		left.Stage == right.Stage &&
		left.Message == right.Message &&
		left.Detail == right.Detail &&
		left.QueueID == right.QueueID &&
		left.UserID == right.UserID &&
		left.RequestID == right.RequestID &&
		left.Status == right.Status &&
		left.BlockReason == right.BlockReason &&
		left.QueuePosition == right.QueuePosition &&
		left.Remaining == right.Remaining &&
		left.Granted == right.Granted &&
		left.NextRetryAt == right.NextRetryAt
}

func (s *Service) publishAdminQueueActivity(event adminQueueActivityEvent) {
	if s == nil {
		return
	}

	message := strings.TrimSpace(event.Message)
	if message == "" {
		return
	}

	nowTS := time.Now().Unix()
	entry := adminQueueActivityEntry{
		Kind:          normalizeAdminQueueActivityKind(event.Kind),
		Stage:         strings.TrimSpace(event.Stage),
		Message:       message,
		Detail:        truncateAdminQueueActivityText(event.Detail, 240),
		QueueID:       event.QueueID,
		UserID:        event.UserID,
		RequestID:     strings.TrimSpace(event.RequestID),
		Status:        strings.TrimSpace(event.Status),
		BlockReason:   strings.TrimSpace(event.BlockReason),
		QueuePosition: maxInt(0, event.QueuePosition),
		Remaining:     maxInt(0, event.Remaining),
		Granted:       maxInt(0, event.Granted),
		CreatedAt:     isoformatFromTS(nowTS),
		CreatedAtTS:   nowTS,
	}
	if event.NextRetryAtTS > 0 {
		entry.NextRetryAt = isoformatFromTS(event.NextRetryAtTS)
	}

	shouldNotify := false
	s.adminQueueActivityMu.Lock()
	if len(s.adminQueueActivity) == 0 || !adminQueueActivityEquals(s.adminQueueActivity[len(s.adminQueueActivity)-1], entry) {
		s.adminQueueActivitySeq++
		entry.Sequence = s.adminQueueActivitySeq
		s.adminQueueActivity = append(s.adminQueueActivity, entry)
		if overflow := len(s.adminQueueActivity) - adminQueueActivityMaxEntries; overflow > 0 {
			s.adminQueueActivity = append([]adminQueueActivityEntry(nil), s.adminQueueActivity[overflow:]...)
		}
		shouldNotify = true
	}
	s.adminQueueActivityMu.Unlock()

	if shouldNotify && s.adminQueueEvents != nil {
		s.adminQueueEvents.notify(adminQueueActivityBrokerUserID)
	}
}

func (s *Service) GetAdminQueueActivitySnapshot(limit int) adminQueueActivitySnapshot {
	if s == nil {
		return adminQueueActivitySnapshot{
			Items:       []adminQueueActivityEntry{},
			Total:       0,
			GeneratedAt: isoformatNow(),
		}
	}

	limit = clampInt(limit, 1, adminQueueActivityMaxEntries, adminQueueActivityDefaultLimit)
	s.adminQueueActivityMu.Lock()
	defer s.adminQueueActivityMu.Unlock()

	total := len(s.adminQueueActivity)
	items := make([]adminQueueActivityEntry, 0, minInt(limit, total))
	for index := total - 1; index >= 0 && len(items) < limit; index-- {
		items = append(items, s.adminQueueActivity[index])
	}

	return adminQueueActivitySnapshot{
		Items:       items,
		Total:       total,
		GeneratedAt: isoformatNow(),
	}
}

func describeAdminQueueBlockReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case queueBlockReasonHourlyQuotaExhausted:
		return "用户小时额度已耗尽"
	case queueBlockReasonAPIKeyRateLimited:
		return "API Key 分钟限流中"
	case queueBlockReasonInventoryUnavailable:
		return "当前没有可领取库存"
	case queueBlockReasonProbeLocksPending:
		return "候选账号仍在验活锁中"
	case queueBlockReasonNoEligibleTokens:
		return "当前没有符合该用户条件的账号"
	case queueBlockReasonAllocationStalled:
		return "本轮未分配成功，等待下一轮重试"
	default:
		trimmed := strings.TrimSpace(reason)
		if trimmed == "" {
			return "等待下一轮调度"
		}
		return trimmed
	}
}

func describeAdminQueueCancelReason(reason string) string {
	trimmed := strings.TrimSpace(reason)
	switch {
	case trimmed == "":
		return "队列已结束"
	case strings.HasPrefix(trimmed, "admin:"):
		adminReason := strings.TrimSpace(strings.TrimPrefix(trimmed, "admin:"))
		if adminReason == "" {
			return "管理员已取消该排队项"
		}
		return "管理员取消：" + truncateAdminQueueActivityText(adminReason, 120)
	case trimmed == queueCancelReasonTimeout:
		return "等待时间超过上限，系统已自动取消"
	case trimmed == queueCancelReasonFailureThreshold:
		return "连续处理失败次数达到阈值，系统已停止继续处理"
	case trimmed == queueCancelReasonStartupReset:
		return "服务启动时清理了遗留排队项"
	case trimmed == queueCancelReasonUserMissing:
		return "关联用户不存在，队列已取消"
	case trimmed == queueCancelReasonUserBanned:
		return "关联用户已被封禁，队列已取消"
	case trimmed == queueCancelReasonAPIKeyUnavailable:
		return "关联 API Key 不可用，队列已取消"
	default:
		return truncateAdminQueueActivityText(trimmed, 120)
	}
}

func adminQueueRetrySuffix(nextRetryAtTS int64) string {
	if nextRetryAtTS <= 0 {
		return ""
	}
	return fmt.Sprintf("，预计 %s 再次尝试", isoformatFromTS(nextRetryAtTS))
}

func (s *Service) publishAdminQueueEnqueued(entry userQueueEntry) {
	detail := fmt.Sprintf(
		"队列 #%d 已进入排队，当前位置 %d，待领取 %d 个账号",
		entry.ID,
		maxInt(1, entry.QueueRank),
		maxInt(0, entry.Remaining),
	)
	if reason := strings.TrimSpace(entry.BlockReason.String); reason != "" {
		detail += "，当前受限：" + describeAdminQueueBlockReason(reason) + adminQueueRetrySuffix(entry.NextRetryAtTS.Int64)
	}
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "info",
		Stage:         "queue_enqueued",
		Message:       "开始排队",
		Detail:        detail,
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        entry.Status,
		BlockReason:   entry.BlockReason.String,
		QueuePosition: entry.QueueRank,
		Remaining:     entry.Remaining,
		NextRetryAtTS: entry.NextRetryAtTS.Int64,
	})
}

func (s *Service) publishAdminQueueProcessingStarted(entry userQueueEntry) {
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "info",
		Stage:         "processing_started",
		Message:       "开始处理队头",
		Detail:        fmt.Sprintf("正在处理队列 #%d，当前还需分配 %d 个账号", entry.ID, maxInt(0, entry.Remaining)),
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        entry.Status,
		BlockReason:   entry.BlockReason.String,
		QueuePosition: entry.QueueRank,
		Remaining:     entry.Remaining,
	})
}

func (s *Service) publishAdminQueueRetryPending(entry userQueueEntry) {
	reason := strings.TrimSpace(entry.BlockReason.String)
	if reason == "" {
		reason = "retry_pending"
	}
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "warn",
		Stage:         "retry_pending",
		Message:       "等待重试窗口",
		Detail:        fmt.Sprintf("队列 #%d 仍在等待：%s%s", entry.ID, describeAdminQueueBlockReason(reason), adminQueueRetrySuffix(entry.NextRetryAtTS.Int64)),
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        entry.Status,
		BlockReason:   reason,
		QueuePosition: entry.QueueRank,
		Remaining:     entry.Remaining,
		NextRetryAtTS: entry.NextRetryAtTS.Int64,
	})
}

func (s *Service) publishAdminQueueBlocked(entry userQueueEntry, status string, blockReason string, nextRetryAtTS int64) {
	normalizedStatus := strings.TrimSpace(status)
	message := "等待下一轮调度"
	if normalizedStatus == queueStatusQueuedBlocked {
		message = "队列暂时阻塞"
	}
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "warn",
		Stage:         "queue_blocked",
		Message:       message,
		Detail:        fmt.Sprintf("队列 #%d 当前无法继续：%s%s", entry.ID, describeAdminQueueBlockReason(blockReason), adminQueueRetrySuffix(nextRetryAtTS)),
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        normalizedStatus,
		BlockReason:   blockReason,
		QueuePosition: entry.QueueRank,
		Remaining:     entry.Remaining,
		NextRetryAtTS: nextRetryAtTS,
	})
}

func (s *Service) publishAdminQueueProbeStarted(entry userQueueEntry, allowed int) {
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "info",
		Stage:         "probe_started",
		Message:       "开始验活",
		Detail:        fmt.Sprintf("队列 #%d 正在探活并尝试分配，当前目标最多 %d 个账号", entry.ID, maxInt(0, allowed)),
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        entry.Status,
		BlockReason:   entry.BlockReason.String,
		QueuePosition: entry.QueueRank,
		Remaining:     entry.Remaining,
	})
}

func (s *Service) publishAdminQueueProgress(entry userQueueEntry, granted int, remainingAfter int) {
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "success",
		Stage:         "queue_progress",
		Message:       "队列有新进度",
		Detail:        fmt.Sprintf("队列 #%d 本轮已分配 %d 个账号，剩余 %d 个待领取", entry.ID, maxInt(0, granted), maxInt(0, remainingAfter)),
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        queueStatusQueuedWaiting,
		QueuePosition: entry.QueueRank,
		Remaining:     remainingAfter,
		Granted:       granted,
	})
}

func (s *Service) publishAdminQueueCompleted(entry userQueueEntry, granted int) {
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "success",
		Stage:         "queue_completed",
		Message:       "队列处理完成",
		Detail:        fmt.Sprintf("队列 #%d 已全部分配完成，本轮发放 %d 个账号", entry.ID, maxInt(0, granted)),
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        queueStatusSucceeded,
		QueuePosition: entry.QueueRank,
		Granted:       granted,
	})
}

func (s *Service) publishAdminQueueFailure(entry userQueueEntry, reason string, failureCount int, cancelled bool) {
	reasonText := truncateAdminQueueActivityText(reason, 140)
	if strings.TrimSpace(reasonText) == "" {
		reasonText = "queue_processing_error"
	}
	detail := fmt.Sprintf(
		"队列 #%d 处理报错：%s（连续失败 %d 次）",
		entry.ID,
		reasonText,
		maxInt(0, failureCount),
	)
	if cancelled {
		detail += "，已达到失败阈值"
	}
	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "error",
		Stage:         "queue_failure",
		Message:       "队列处理失败",
		Detail:        detail,
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        entry.Status,
		BlockReason:   entry.BlockReason.String,
		QueuePosition: entry.QueueRank,
		Remaining:     entry.Remaining,
	})
}

func (s *Service) publishAdminQueueTerminal(entry userQueueEntry, status string, reason string) {
	message := "排队已结束"
	switch strings.TrimSpace(status) {
	case queueStatusCancelled:
		message = "排队已取消"
	case queueStatusExpired:
		message = "排队已过期"
	case queueStatusFailed:
		message = "排队处理失败"
	case queueStatusSucceeded:
		message = "排队已完成"
	}

	s.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:          "error",
		Stage:         "queue_terminal",
		Message:       message,
		Detail:        fmt.Sprintf("队列 #%d：%s", entry.ID, describeAdminQueueCancelReason(reason)),
		QueueID:       entry.ID,
		UserID:        entry.UserID,
		RequestID:     entry.RequestID,
		Status:        strings.TrimSpace(status),
		BlockReason:   entry.BlockReason.String,
		QueuePosition: entry.QueueRank,
		Remaining:     entry.Remaining,
	})
}
