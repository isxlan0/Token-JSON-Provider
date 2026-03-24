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
	if !s.awaitStartupReconcile(ctx) {
		return
	}

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
	if _, err := s.reconcileTokenFiles(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.logger.Info("startup reconcile token files cancelled")
			return
		}
		s.logger.Error("startup reconcile token files", "error", err)
	}
}

func (s *Service) awaitStartupReconcile(ctx context.Context) bool {
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			s.markStartupReconcileCanceled(ctx.Err())
			return false
		}

		s.markStartupReconcileAttempt(attempt)
		tokenSummary, err := s.reconcileTokenFiles(ctx)
		if err == nil {
			queueSummary, queueErr := s.startupReconcileQueue(ctx)
			if queueErr == nil {
				s.markStartupReady(mergeStartupReconcileSummary(tokenSummary, queueSummary))
				return true
			}
			err = queueErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.logger.Info("startup reconcile token files cancelled")
			s.markStartupReconcileCanceled(err)
			return false
		}

		s.markStartupReconcileError(err)
		s.logger.Error("startup reconcile token files", "error", err, "attempt", attempt)

		timer := time.NewTimer(startupReconcileRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			s.markStartupReconcileCanceled(ctx.Err())
			return false
		case <-timer.C:
		}
	}
}

func mergeStartupReconcileSummary(tokenSummary map[string]int, queueSummary map[string]int) map[string]int {
	merged := cloneIntMap(tokenSummary)
	if merged == nil {
		merged = make(map[string]int)
	}
	for key, value := range queueSummary {
		merged["queue_"+key] = value
	}
	return merged
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
