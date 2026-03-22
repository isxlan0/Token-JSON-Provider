package httpapi

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

func InstallErrorHandler(e *echo.Echo) {
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}

		var httpErr *echo.HTTPError
		if errors.As(err, &httpErr) {
			statusCode := httpErr.Code
			switch message := httpErr.Message.(type) {
			case map[string]any:
				_ = c.JSON(statusCode, message)
				return
			case string:
				_ = c.JSON(statusCode, map[string]any{"detail": message})
				return
			default:
				_ = c.JSON(statusCode, map[string]any{"detail": http.StatusText(statusCode)})
				return
			}
		}

		_ = c.JSON(http.StatusInternalServerError, map[string]any{
			"detail": "Internal server error",
		})
	}
}
