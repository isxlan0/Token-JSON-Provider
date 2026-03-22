package auth

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Service) loginRemoved(c echo.Context) error {
	return echo.NewHTTPError(http.StatusGone, "Access key login has been removed.")
}
