package claim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	tokenImportMaxAttempts    = 6
	tokenImportRetryBaseDelay = 250 * time.Millisecond
	tokenImportRetryMaxDelay  = 2 * time.Second
	startupQueueResetTimeout  = 3 * time.Second
	startupTokenSyncTimeout   = 3 * time.Second
)

type hideClaimsTask struct {
	userID   int64
	claimIDs []int64
}

type tokenImportRequest struct {
	fileName string
	reason   string
	attempt  int
}

type tokenImportResult struct {
	fileName string
	changed  bool
}

func (s *Service) hideClaimsWorkerLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case task := <-s.hideClaimsCh:
			if ctx.Err() != nil {
				return
			}
			if err := s.applyHideClaims(ctx, task.userID, task.claimIDs); err != nil {
				s.logger.Error("hide claims task failed", "error", err, "user_id", task.userID)
			}
		}
	}
}

func (s *Service) tokenImportLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case request := <-s.tokenImportCh:
			if ctx.Err() != nil {
				s.finishTokenImport(request.fileName)
				return
			}
			changed, err := s.importTokenFile(ctx, request.fileName)
			if err != nil {
				if ctx.Err() != nil {
					s.finishTokenImport(request.fileName)
					return
				}
				if s.retryTokenImport(ctx, request, err) {
					continue
				}
				s.logger.Error("import token file", "error", err, "file_name", request.fileName, "reason", request.reason, "attempt", request.attempt+1)
			}
			s.finishTokenImport(request.fileName)
			if changed {
				select {
				case s.tokenImportDoneCh <- tokenImportResult{fileName: request.fileName, changed: true}:
				default:
				}
			}
		}
	}
}

func (s *Service) retryTokenImport(ctx context.Context, request tokenImportRequest, err error) bool {
	if !isRetriableTokenImportError(err) {
		return false
	}
	if request.attempt+1 >= tokenImportMaxAttempts {
		return false
	}

	next := request
	next.attempt++
	delay := tokenImportRetryDelay(next.attempt)

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			s.finishTokenImport(next.fileName)
			return
		case <-timer.C:
		}

		select {
		case <-ctx.Done():
			s.finishTokenImport(next.fileName)
		case s.tokenImportCh <- next:
		}
	}()
	return true
}

func tokenImportRetryDelay(attempt int) time.Duration {
	delay := tokenImportRetryBaseDelay
	for step := 1; step < attempt; step++ {
		if delay >= tokenImportRetryMaxDelay/2 {
			return tokenImportRetryMaxDelay
		}
		delay *= 2
	}
	if delay > tokenImportRetryMaxDelay {
		return tokenImportRetryMaxDelay
	}
	return delay
}

func (s *Service) finishTokenImport(fileName string) {
	s.tokenImportMu.Lock()
	delete(s.tokenImportPending, fileName)
	s.tokenImportMu.Unlock()
}

func (s *Service) ensureTokenDir() error {
	return os.MkdirAll(s.tokenDirPath(), 0o755)
}

func (s *Service) startupReconcileTokenFiles(ctx context.Context) {
	if _, err := s.startupSyncTokenFileNames(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.logger.Info("startup reconcile token files cancelled")
			return
		}
		s.logger.Error("startup reconcile token files", "error", err)
	}
}

func (s *Service) awaitStartupReconcile(ctx context.Context) bool {
	if ctx.Err() != nil {
		s.markStartupReconcileCanceled(ctx.Err())
		return false
	}

	const attempt = 1
	s.markStartupReconcileAttempt(attempt)

	summary := make(map[string]int)
	failOpen := false

	queueSummary, queueErr := s.runStartupReconcileStage(ctx, "queue reset", startupQueueResetTimeout, s.startupReconcileQueue)
	if queueErr != nil {
		if ctx.Err() != nil {
			s.markStartupReconcileCanceled(ctx.Err())
			return false
		}
		failOpen = true
		summary["queue_failed_open"] = 1
		s.logger.Warn("startup queue reconcile failed open", "trace", "claim", "error", queueErr)
	} else {
		for key, value := range queueSummary {
			summary["queue_"+key] = value
		}
	}

	tokenSummary, tokenErr := s.runStartupReconcileStage(ctx, "token name sync", startupTokenSyncTimeout, s.startupSyncTokenFileNames)
	if tokenErr != nil {
		if ctx.Err() != nil {
			s.markStartupReconcileCanceled(ctx.Err())
			return false
		}
		failOpen = true
		summary["token_sync_failed_open"] = 1
		s.logger.Warn("startup token name sync failed open", "trace", "claim", "error", tokenErr)
	} else {
		for key, value := range tokenSummary {
			summary[key] = value
		}
	}

	if !failOpen {
		s.claimTrace("startup reconcile attempt succeeded", "attempt", attempt, "summary", summary)
	} else {
		s.claimTrace("startup reconcile completed with fail-open", "attempt", attempt, "summary", summary)
	}
	s.markStartupReady(summary)
	return true
}

func (s *Service) runStartupReconcileStage(ctx context.Context, stage string, timeout time.Duration, fn func(context.Context) (map[string]int, error)) (map[string]int, error) {
	stageCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	s.claimTrace("startup reconcile stage started", "stage", stage, "timeout_ms", durationMillis(timeout))
	startedAt := time.Now()
	summary, err := fn(stageCtx)
	if err != nil {
		s.claimTrace(
			"startup reconcile stage failed",
			"stage",
			stage,
			"duration_ms",
			durationMillis(time.Since(startedAt)),
			"error",
			err,
		)
		return nil, err
	}
	s.claimTrace(
		"startup reconcile stage completed",
		"stage",
		stage,
		"duration_ms",
		durationMillis(time.Since(startedAt)),
		"summary",
		summary,
	)
	return summary, nil
}

func (s *Service) enqueueTokenImportsAsync(fileNames []string, reason string) int {
	requests := make([]tokenImportRequest, 0, len(fileNames))

	s.tokenImportMu.Lock()
	for _, fileName := range fileNames {
		trimmed := strings.TrimSpace(fileName)
		if trimmed == "" {
			continue
		}
		if _, ok := s.tokenImportPending[trimmed]; ok {
			continue
		}
		s.tokenImportPending[trimmed] = 1
		requests = append(requests, tokenImportRequest{fileName: trimmed, reason: reason})
	}
	s.tokenImportMu.Unlock()

	if len(requests) == 0 {
		return 0
	}

	dispatchCtx, ok := s.serviceContext()
	if !ok || dispatchCtx == nil {
		dispatchCtx = context.Background()
	}

	s.goWorker(func() {
		for _, request := range requests {
			select {
			case <-dispatchCtx.Done():
				s.finishTokenImport(request.fileName)
			case s.tokenImportCh <- request:
			}
		}
	})
	return len(requests)
}

func (s *Service) enqueueTokenImport(ctx context.Context, fileName string, reason string) {
	trimmed := strings.TrimSpace(fileName)
	if trimmed == "" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.tokenImportMu.Lock()
	if _, ok := s.tokenImportPending[trimmed]; ok {
		s.tokenImportMu.Unlock()
		return
	}
	s.tokenImportPending[trimmed] = 1
	s.tokenImportMu.Unlock()

	select {
	case s.tokenImportCh <- tokenImportRequest{fileName: trimmed, reason: reason}:
	case <-ctx.Done():
		s.finishTokenImport(trimmed)
	}
}

func (s *Service) markInternalTokenWrite(fileName string) {
	s.tokenImportMu.Lock()
	s.tokenInternalWrites[fileName] = time.Now().Add(5 * time.Second)
	s.tokenImportMu.Unlock()
}

func (s *Service) shouldIgnoreInternalTokenWrite(fileName string) bool {
	s.tokenImportMu.Lock()
	defer s.tokenImportMu.Unlock()

	until, ok := s.tokenInternalWrites[fileName]
	if !ok {
		return false
	}
	if until.After(time.Now()) {
		return true
	}
	delete(s.tokenInternalWrites, fileName)
	return false
}

func (s *Service) applyHideClaims(ctx context.Context, userID int64, claimIDs []int64) error {
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

	s.invalidateUserClaimsCache(userID)
	return nil
}
