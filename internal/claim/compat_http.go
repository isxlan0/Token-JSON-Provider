package claim

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
)

type archivePayload struct {
	Names []string `json:"names"`
}

func (s *Service) downloadClaimsArchive(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	items, err := s.ListClaimFiles(c.Request().Context(), requestContext.UserID)
	if err != nil {
		return mapClaimDataError(err)
	}
	if len(items) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, "No claimed token files available for archive.")
	}

	zipItems := make([]zipItem, 0, len(items))
	for _, item := range items {
		zipItems = append(zipItems, zipItem{Name: item.FileName, Content: item.Content})
	}
	buffer, _, err := buildZipBuffer(zipItems)
	if err != nil {
		return err
	}

	fileName := "claimed-" + time.Now().Format("20060102-150405") + ".zip"
	c.Response().Header().Set(echo.HeaderContentDisposition, buildContentDisposition(fileName))
	return c.Blob(http.StatusOK, "application/zip", buffer.Bytes())
}

func (s *Service) getDashboardLeaderboard(c echo.Context) error {
	payload, err := s.getLeaderboard(c.Request().Context(), parseWindowSeconds(c.QueryParam("window"), 24*3600, 7*24*3600), parseBoundedInt(c.QueryParam("limit"), 10, 1, 10))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getDashboardRecentClaims(c echo.Context) error {
	items, err := s.getRecentClaims(c.Request().Context(), parseBoundedInt(c.QueryParam("limit"), 10, 1, 10))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (s *Service) getDashboardTrends(c echo.Context) error {
	series, err := s.getClaimTrends(c.Request().Context(), parseWindowSeconds(c.QueryParam("window"), 7*24*3600, 14*24*3600), parseBucketSeconds(c.QueryParam("bucket"), 3600))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{
		"window": parseWindowSeconds(c.QueryParam("window"), 7*24*3600, 14*24*3600),
		"bucket": parseBucketSeconds(c.QueryParam("bucket"), 3600),
		"series": series,
	})
}

func (s *Service) getDashboardSystemStatus(c echo.Context) error {
	payload, err := s.getSystemStatus(c.Request().Context())
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getDashboardStatsRoute(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}
	stats, err := s.getDashboardStats(c.Request().Context(), requestContext.UserID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{
		"total_tokens":          stats["total_tokens"],
		"available_tokens":      stats["available_tokens"],
		"claimed_total":         stats["claimed_total"],
		"claimed_unique":        stats["claimed_unique"],
		"others_claimed_total":  stats["others_claimed_total"],
		"others_claimed_unique": stats["others_claimed_unique"],
	})
}

func (s *Service) getJSONIndex(c echo.Context) error {
	record, ok := auth.APIKeyRecordFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Invalid API key.")
	}
	documents, err := s.buildClaimedDocuments(c.Request().Context(), record.UserID)
	if err != nil {
		return err
	}

	items := make([]map[string]any, 0, len(documents))
	for _, item := range documents {
		items = append(items, item.toIndexPayload())
	}
	return c.JSON(http.StatusOK, map[string]any{
		"count":      len(documents),
		"updated_at": isoformatNow(),
		"items":      items,
	})
}

func (s *Service) getJSONItem(c echo.Context) error {
	record, ok := auth.APIKeyRecordFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Invalid API key.")
	}

	name := strings.TrimSpace(c.QueryParam("name"))
	id := strings.TrimSpace(c.QueryParam("id"))
	indexRaw := strings.TrimSpace(c.QueryParam("index"))
	if name == "" && id == "" && indexRaw == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Provide one of: name, id, index.")
	}

	documents, err := s.buildClaimedDocuments(c.Request().Context(), record.UserID)
	if err != nil {
		return err
	}

	var item *tokenDocument
	switch {
	case name != "":
		for idx := range documents {
			if documents[idx].Name == name {
				item = &documents[idx]
				break
			}
		}
	case id != "":
		for idx := range documents {
			if documents[idx].ID == id {
				item = &documents[idx]
				break
			}
		}
	case indexRaw != "":
		index, err := strconv.Atoi(indexRaw)
		if err == nil && index >= 0 && index < len(documents) {
			item = &documents[index]
		}
	}

	if item == nil {
		return echo.NewHTTPError(http.StatusNotFound, "Token file not found.")
	}
	if strings.TrimSpace(item.Error) != "" {
		return c.JSON(http.StatusUnprocessableEntity, map[string]any{"detail": item.Error})
	}
	return c.JSON(http.StatusOK, item.toDetailPayload())
}

func (s *Service) downloadJSONArchive(c echo.Context) error {
	record, ok := auth.APIKeyRecordFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Invalid API key.")
	}

	var payload archivePayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	documents, err := s.buildClaimedDocuments(c.Request().Context(), record.UserID)
	if err != nil {
		return err
	}
	if len(payload.Names) > 0 {
		allowed := make(map[string]struct{}, len(payload.Names))
		for _, name := range payload.Names {
			allowed[strings.TrimSpace(name)] = struct{}{}
		}
		filtered := make([]tokenDocument, 0, len(documents))
		for _, item := range documents {
			if _, ok := allowed[item.Name]; ok {
				filtered = append(filtered, item)
			}
		}
		documents = filtered
	}
	if len(documents) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, "No token files available for archive.")
	}

	zipItems := make([]zipItem, 0, len(documents))
	for _, item := range documents {
		zipItems = append(zipItems, zipItem{Name: item.Name, Content: item.Content})
	}
	buffer, _, err := buildZipBuffer(zipItems)
	if err != nil {
		return err
	}

	fileName := "token-atlas-" + time.Now().Format("20060102-150405") + ".zip"
	c.Response().Header().Set(echo.HeaderContentDisposition, buildContentDisposition(fileName))
	return c.Blob(http.StatusOK, "application/zip", buffer.Bytes())
}

func (s *Service) downloadAllArchive(c echo.Context) error {
	return s.downloadJSONArchive(c)
}

func (s *Service) getHealth(c echo.Context) error {
	system, err := s.getSystemStatus(c.Request().Context())
	if err != nil {
		return err
	}
	inventory, _ := system["inventory"].(map[string]any)
	index, _ := system["index"].(map[string]any)
	return c.JSON(http.StatusOK, map[string]any{
		"status":      "ok",
		"token_count": inventory["total"],
		"updated_at":  index["updated_at"],
	})
}
