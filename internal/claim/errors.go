package claim

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

var errCorruptClaimData = errors.New("stored claim content is corrupted")

var errDatabaseBusyWaitExceeded = errors.New("database busy wait exceeded")

const (
	dbBusyRetryAttempts  = 4
	dbBusyRetryBaseDelay = 200 * time.Millisecond
	dbBusyRetryMaxDelay  = 1500 * time.Millisecond
)

func wrapCorruptClaimDataError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errCorruptClaimData) {
		return err
	}
	return errors.Join(errCorruptClaimData, err)
}

func mapClaimDataError(err error) error {
	if !errors.Is(err, errCorruptClaimData) {
		return err
	}
	return echo.NewHTTPError(http.StatusInternalServerError, "Stored claim content is corrupted.")
}

func mapDatabaseBusyError(c echo.Context, err error) error {
	if !isDatabaseBusyError(err) {
		return err
	}
	c.Response().Header().Set("Retry-After", "3")
	return echo.NewHTTPError(http.StatusServiceUnavailable, "数据库正忙，请稍后重试。")
}

func isDatabaseBusyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errDatabaseBusyWaitExceeded) {
		return true
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked") ||
		strings.Contains(message, "database schema is locked")
}

func dbBusyRetryDelay(attempt int) time.Duration {
	delay := dbBusyRetryBaseDelay
	for step := 1; step < attempt; step++ {
		if delay >= dbBusyRetryMaxDelay/2 {
			return dbBusyRetryMaxDelay
		}
		delay *= 2
	}
	if delay > dbBusyRetryMaxDelay {
		return dbBusyRetryMaxDelay
	}
	return delay
}

func runWithDatabaseBusyRetry[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T

	for attempt := 0; attempt < dbBusyRetryAttempts; attempt++ {
		value, err := fn()
		if err == nil {
			return value, nil
		}
		if !isDatabaseBusyError(err) || attempt+1 >= dbBusyRetryAttempts {
			return zero, err
		}

		delay := dbBusyRetryDelay(attempt + 1)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
	}

	return zero, nil
}

func wrapDatabaseBusyWaitError(waitErr error, cause error) error {
	switch {
	case waitErr == nil && cause == nil:
		return errDatabaseBusyWaitExceeded
	case waitErr == nil:
		return errors.Join(errDatabaseBusyWaitExceeded, cause)
	case cause == nil:
		return errors.Join(errDatabaseBusyWaitExceeded, waitErr)
	default:
		return errors.Join(errDatabaseBusyWaitExceeded, waitErr, cause)
	}
}

func waitForDatabaseAvailability[T any](ctx context.Context, onBusy func(attempt int), fn func() (T, error)) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt < dbBusyRetryAttempts; attempt++ {
		value, err := fn()
		if err == nil {
			return value, nil
		}
		if !isDatabaseBusyError(err) {
			return zero, err
		}
		lastErr = err
		if onBusy != nil {
			onBusy(attempt + 1)
		}

		if attempt+1 >= dbBusyRetryAttempts {
			return zero, wrapDatabaseBusyWaitError(nil, lastErr)
		}

		delay := dbBusyRetryDelay(attempt + 1)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, wrapDatabaseBusyWaitError(ctx.Err(), lastErr)
		case <-timer.C:
		}
	}

	return zero, wrapDatabaseBusyWaitError(nil, lastErr)
}
