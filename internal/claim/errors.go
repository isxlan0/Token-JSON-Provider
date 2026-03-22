package claim

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

var errCorruptClaimData = errors.New("stored claim content is corrupted")

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
