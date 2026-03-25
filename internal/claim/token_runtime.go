package claim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	tokenDeleteRetryBaseDelay = 250 * time.Millisecond
	tokenDeleteRetryMaxDelay  = 2 * time.Second
)

func (s *Service) setLifecycleContext(ctx context.Context) {
	s.lifecycleMu.Lock()
	s.lifecycleCtx = ctx
	s.lifecycleMu.Unlock()
}

func (s *Service) serviceContext() (context.Context, bool) {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.lifecycleCtx == nil {
		return nil, false
	}
	return s.lifecycleCtx, true
}

func (s *Service) tokenDirPath() string {
	trimmed := strings.TrimSpace(s.cfg.Files.TokenDir)
	if trimmed == "" {
		return filepath.Clean(filepath.Join(".", "token"))
	}
	return filepath.Clean(trimmed)
}

func (s *Service) tokenFileAbsolutePath(fileName string) string {
	return filepath.Join(s.tokenDirPath(), filepath.Base(strings.TrimSpace(fileName)))
}

func (s *Service) tokenFileRelativePath(fileName string) string {
	baseName := filepath.Base(strings.TrimSpace(fileName))
	if baseName == "." || baseName == string(filepath.Separator) || baseName == "" {
		return ""
	}

	absolutePath := s.tokenFileAbsolutePath(baseName)
	if cwd, err := os.Getwd(); err == nil {
		if relativePath, err := filepath.Rel(cwd, absolutePath); err == nil {
			cleaned := filepath.Clean(relativePath)
			if cleaned != "." && cleaned != ".." && !strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
				return filepath.ToSlash(cleaned)
			}
		}
	}

	return filepath.ToSlash(absolutePath)
}

func (s *Service) markStartupReconcileAttempt(attempt int) {
	s.startupMu.Lock()
	s.startupInProgress = true
	s.startupAttempts = attempt
	s.startupLastStartedAt = isoformatNow()
	s.startupLastError = ""
	s.startupMu.Unlock()
	s.claimTrace("startup reconcile attempt started", "attempt", attempt)
}

func (s *Service) markStartupReconcileError(err error) {
	s.startupMu.Lock()
	s.startupInProgress = true
	if err != nil {
		s.startupLastError = err.Error()
	}
	s.startupMu.Unlock()
	s.claimTrace("startup reconcile attempt failed", "error", err)
}

func (s *Service) markStartupReconcileCanceled(err error) {
	s.startupMu.Lock()
	s.startupInProgress = false
	if err != nil {
		s.startupLastError = err.Error()
	}
	s.startupMu.Unlock()
	s.claimTrace("startup reconcile cancelled", "error", err)
}

func (s *Service) markStartupReady(summary map[string]int) {
	s.startupMu.Lock()
	s.startupReady = true
	s.startupInProgress = false
	s.startupLastError = ""
	s.startupReadyAt = isoformatNow()
	s.startupLastSummary = cloneIntMap(summary)
	s.startupMu.Unlock()

	s.startupReadyOnce.Do(func() {
		close(s.startupReadyCh)
	})
	s.claimTrace("startup reconcile ready", "summary", summary)
}

func (s *Service) isStartupReady() bool {
	s.startupMu.RLock()
	defer s.startupMu.RUnlock()
	return s.startupReady
}

func (s *Service) waitForStartupReady(ctx context.Context, component string) bool {
	component = strings.TrimSpace(component)
	if component == "" {
		component = "unknown"
	}
	if s.isStartupReady() {
		s.claimTrace("startup already ready", "component", component)
		return true
	}

	s.claimTrace("waiting for startup ready", "component", component, "startup", s.startupHealthPayload())
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.claimTrace("startup wait cancelled", "component", component, "reason", ctx.Err())
			return false
		case <-s.startupReadyCh:
			s.claimTrace("startup wait finished", "component", component, "startup", s.startupHealthPayload())
			return true
		case <-ticker.C:
			if s.claimTraceEnabled() {
				s.logger.Warn(
					"startup ready wait still blocked",
					"trace", "claim",
					"component", component,
					"startup", s.startupHealthPayload(),
				)
			}
		}
	}
}

func (s *Service) startupHealthPayload() map[string]any {
	s.startupMu.RLock()
	defer s.startupMu.RUnlock()

	payload := map[string]any{
		"ready":              s.startupReady,
		"in_progress":        s.startupInProgress,
		"attempt":            s.startupAttempts,
		"last_started_at":    nullableStringOrNil(s.startupLastStartedAt),
		"ready_at":           nullableStringOrNil(s.startupReadyAt),
		"initial_scan_stats": nil,
	}
	if s.startupLastError != "" {
		payload["last_error"] = s.startupLastError
	}
	if len(s.startupLastSummary) > 0 {
		payload["initial_scan_stats"] = cloneIntMap(s.startupLastSummary)
	}
	return payload
}

func cloneIntMap(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]int, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func (s *Service) removeTokenFileNow(fileName string) error {
	trimmed := filepath.Base(strings.TrimSpace(fileName))
	if trimmed == "." || trimmed == string(filepath.Separator) || trimmed == "" {
		return nil
	}
	if err := s.removeTokenFile(s.tokenFileAbsolutePath(trimmed)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove token file %s: %w", trimmed, err)
	}
	return nil
}

func (s *Service) scheduleTokenFileDeleteRetry(fileName string, reason string, initialErr error) {
	trimmed := filepath.Base(strings.TrimSpace(fileName))
	if trimmed == "." || trimmed == string(filepath.Separator) || trimmed == "" {
		return
	}

	ctx, ok := s.serviceContext()
	if !ok {
		s.logger.Warn("skip token file delete retry because service context is unavailable", "file_name", trimmed, "reason", reason, "error", initialErr)
		return
	}

	s.tokenDeleteMu.Lock()
	if _, exists := s.tokenDeletePending[trimmed]; exists {
		s.tokenDeleteMu.Unlock()
		return
	}
	s.tokenDeletePending[trimmed] = struct{}{}
	s.tokenDeleteMu.Unlock()

	s.goWorker(func() {
		defer func() {
			s.tokenDeleteMu.Lock()
			delete(s.tokenDeletePending, trimmed)
			s.tokenDeleteMu.Unlock()
		}()

		for attempt := 1; ; attempt++ {
			err := s.removeTokenFileNow(trimmed)
			if err == nil {
				if attempt > 1 {
					s.logger.Info("token file delete retry succeeded", "file_name", trimmed, "reason", reason, "attempt", attempt)
				}
				return
			}

			delay := tokenDeleteRetryDelay(attempt)
			s.logger.Warn("token file delete retry scheduled", "file_name", trimmed, "reason", reason, "attempt", attempt, "error", err, "retry_in", delay.String())

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	})
}

func tokenDeleteRetryDelay(attempt int) time.Duration {
	delay := tokenDeleteRetryBaseDelay
	for step := 1; step < attempt; step++ {
		if delay >= tokenDeleteRetryMaxDelay/2 {
			return tokenDeleteRetryMaxDelay
		}
		delay *= 2
	}
	if delay > tokenDeleteRetryMaxDelay {
		return tokenDeleteRetryMaxDelay
	}
	return delay
}
