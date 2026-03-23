package claim

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
)

type claimRequestPayload struct {
	Count       int    `json:"count"`
	ClientTabID string `json:"client_tab_id,omitempty"`
}

type hideClaimsPayload struct {
	ClaimIDs []int64 `json:"claim_ids"`
}

func (s *Service) RegisterRoutes(e *echo.Echo) {
	if s.auth == nil {
		return
	}

	me := e.Group("/me", s.auth.RequireUserMiddleware)
	me.GET("", s.getMe)
	me.GET("/quota", s.getQuota)
	me.GET("/claims-summary", s.getClaimsSummary)
	me.GET("/claims", s.getClaims)
	me.GET("/claims/archive", s.downloadClaimsArchive)
	me.POST("/claims/hide", s.hideClaims)
	me.GET("/runtime-snapshot", s.getRuntimeSnapshot)
	me.GET("/bootstrap", s.getBootstrap)
	me.GET("/queue-status", s.getQueueStatus)
	me.GET("/claimable-now", s.getClaimableNow)
	me.GET("/queue-stream", s.getQueueStream)
	me.GET("/uploads/results", s.getUploadResults)
	me.GET("/uploads/stream", s.getUploadResultsStream)
	me.POST("/uploads/tokens", s.uploadTokens)
	me.POST("/claim", s.claimBySession)

	e.POST("/api/claim", s.claimByAPIKey, s.auth.RequireAPIKeyMiddleware)
	e.GET("/api/download/:token_id", s.downloadClaimedToken)

	dashboard := e.Group("/dashboard", s.auth.RequireUserMiddleware)
	dashboard.GET("/leaderboard", s.getDashboardLeaderboard)
	dashboard.GET("/summary", s.getDashboardSummary)
	dashboard.GET("/recent-claims", s.getDashboardRecentClaims)
	dashboard.GET("/trends", s.getDashboardTrends)
	dashboard.GET("/system-status", s.getDashboardSystemStatus)
	dashboard.GET("/stats", s.getDashboardStatsRoute)

	admin := e.Group("/admin", s.auth.RequireAdminMiddleware)
	admin.GET("/bootstrap", s.getAdminBootstrap)
	admin.GET("/me", s.getAdminMe)
	admin.GET("/users", s.adminListUsers)
	admin.GET("/users/:linuxdo_user_id", s.adminGetUserDetail)
	admin.POST("/users/:linuxdo_user_id/ban", s.adminBanUser)
	admin.POST("/users/:linuxdo_user_id/unban", s.adminUnbanUser)
	admin.GET("/bans", s.adminListBans)
	admin.GET("/tokens", s.adminListTokens)
	admin.POST("/tokens/:token_id/activate", s.adminActivateToken)
	admin.POST("/tokens/:token_id/deactivate", s.adminDeactivateToken)
	admin.POST("/tokens/cleanup-exhausted", s.adminCleanupExhaustedTokens)
	admin.GET("/queue", s.adminListQueue)
	admin.POST("/queue/refresh", s.adminRefreshQueue)
	admin.POST("/queue/:queue_id/cancel", s.adminCancelQueueEntry)
	admin.POST("/queue/users/:user_id/cancel", s.adminCancelUserQueue)
	admin.GET("/policy", s.adminGetPolicy)

	e.GET("/json", s.getJSONIndex, s.auth.RequireAPIKeyMiddleware)
	e.GET("/json/item", s.getJSONItem, s.auth.RequireAPIKeyMiddleware)
	e.POST("/json/archive", s.downloadJSONArchive, s.auth.RequireAPIKeyMiddleware)
	e.GET("/zip", s.downloadAllArchive, s.auth.RequireAPIKeyMiddleware)
	e.GET("/health", s.getHealth)
}

func (s *Service) getMe(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	payload, err := s.GetProfile(c.Request().Context(), requestContext)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getQuota(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	payload, err := s.GetQuotaUsage(c.Request().Context(), requestContext.UserID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getClaimsSummary(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	payload, err := s.GetUserClaimTotals(c.Request().Context(), requestContext.UserID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getClaims(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	items, err := s.ListClaims(c.Request().Context(), requestContext.UserID)
	if err != nil {
		return err
	}
	for index := range items {
		items[index].DownloadURL = s.buildDownloadURL(c, items[index].TokenID)
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (s *Service) hideClaims(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	var payload hideClaimsPayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	claimIDs := uniqueClaimIDs(payload.ClaimIDs)
	if len(claimIDs) == 0 {
		return c.JSON(http.StatusOK, map[string]int{
			"queued":   0,
			"accepted": 0,
		})
	}

	s.invalidateUserClaimsCache(requestContext.UserID)
	select {
	case s.hideClaimsCh <- hideClaimsTask{userID: requestContext.UserID, claimIDs: append([]int64(nil), claimIDs...)}:
	case <-c.Request().Context().Done():
		return c.Request().Context().Err()
	}

	return c.JSON(http.StatusOK, map[string]int{
		"queued":   len(claimIDs),
		"accepted": len(claimIDs),
	})
}

func (s *Service) getRuntimeSnapshot(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	readCtx, cancel := context.WithTimeout(c.Request().Context(), runtimeSnapshotReadTimeout)
	defer cancel()

	payload, err := s.GetRuntimeSnapshot(readCtx, requestContext.UserID)
	if err != nil {
		if !isReadDegradeError(err) {
			return err
		}
		if cached, ok := s.getCachedRuntimeSnapshot(requestContext.UserID); ok {
			payload = cached
			payload.DataSource = dataSourceCache
			payload.StaleAt = staleTimestamp(payload.GeneratedAt)
		} else {
			payload = s.defaultRuntimeSnapshotPayload(requestContext)
		}
		payload = s.mergeRuntimeSnapshotWithRequestContext(payload, requestContext)
		payload.Degraded = true
		payload.DegradedReason = degradedReasonForError(err)
		return c.JSON(http.StatusOK, payload)
	}
	payload = s.mergeRuntimeSnapshotWithRequestContext(payload, requestContext)
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getBootstrap(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	readCtx, cancel := context.WithTimeout(c.Request().Context(), bootstrapReadTimeout)
	defer cancel()

	payload, err := s.GetBootstrap(readCtx, requestContext)
	if err != nil {
		if !isReadDegradeError(err) {
			return err
		}
		payload = s.defaultBootstrapPayload(requestContext)
		payload["degraded_reason"] = degradedReasonForError(err)
	}
	s.logger.Info(
		"bootstrap request source",
		"user_id", requestContext.UserID,
		"client_build", strings.TrimSpace(c.Request().Header.Get("X-App-Build")),
		"client_entry", strings.TrimSpace(c.Request().Header.Get("X-App-Entry")),
		"referer", strings.TrimSpace(c.Request().Header.Get("Referer")),
		"user_agent", strings.TrimSpace(c.Request().Header.Get("User-Agent")),
		"sec_fetch_site", strings.TrimSpace(c.Request().Header.Get("Sec-Fetch-Site")),
		"sec_fetch_mode", strings.TrimSpace(c.Request().Header.Get("Sec-Fetch-Mode")),
		"sec_fetch_dest", strings.TrimSpace(c.Request().Header.Get("Sec-Fetch-Dest")),
		"data_source", payload["data_source"],
		"degraded", payload["degraded"],
	)
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getQueueStatus(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	readCtx, cancel := context.WithTimeout(c.Request().Context(), queueStatusReadTimeout)
	defer cancel()

	payload, err := s.GetQueueStatus(readCtx, requestContext.UserID)
	if err != nil {
		if !isReadDegradeError(err) {
			return err
		}
		if cached, ok := s.getCachedStaleQueueStatus(requestContext.UserID); ok {
			payload = cached
			payload = withQueueStatusMetadata(
				payload,
				dataSourceStale,
				payload.GeneratedAt,
				staleTimestamp(payload.GeneratedAt),
				degradedReasonForError(err),
				true,
			)
		} else {
			payload = s.defaultQueueStatusPayload()
			payload = withQueueStatusMetadata(
				payload,
				dataSourceUnavailable,
				payload.GeneratedAt,
				"",
				degradedReasonForError(err),
				true,
			)
		}
		payload = s.attachCachedClaimableNow(requestContext.UserID, payload)
		return c.JSON(http.StatusOK, payload)
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getClaimableNow(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	readCtx, cancel := context.WithTimeout(c.Request().Context(), queueStatusReadTimeout)
	defer cancel()

	payload, err := s.GetClaimableNow(readCtx, requestContext.UserID)
	if err != nil {
		if !isReadDegradeError(err) {
			return err
		}
		if cached, ok := s.getCachedStaleClaimableNow(requestContext.UserID); ok {
			payload = claimableNowPayloadFromSnapshot(
				cached,
				dataSourceStale,
				staleTimestamp(cached.GeneratedAt),
				degradedReasonForError(err),
				true,
			)
		} else {
			payload = unavailableClaimableNowPayload(degradedReasonForError(err))
		}
		return c.JSON(http.StatusOK, payload)
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getUploadResults(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}
	return c.JSON(http.StatusOK, s.GetUploadResults(requestContext.UserID))
}

func (s *Service) claimBySession(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	payload := claimRequestPayload{Count: 1}
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}
	if payload.Count < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, "count must be greater than or equal to 1")
	}

	result, err := s.CreateClaimRequest(c.Request().Context(), requestContext, nil, payload.Count, payload.ClientTabID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) claimByAPIKey(c echo.Context) error {
	record, ok := auth.APIKeyRecordFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Invalid API key.")
	}

	apiKeyID := record.APIKeyID
	result, err := s.claimResultFromPayload(c, record.UserID, &apiKeyID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) claimResultFromPayload(c echo.Context, userID int64, apiKeyID *int64) (*claimResult, error) {
	payload := claimRequestPayload{Count: 1}
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return nil, echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}
	if payload.Count < 1 {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "count must be greater than or equal to 1")
	}

	result, err := s.ClaimTokens(c.Request().Context(), userID, apiKeyID, payload.Count)
	if err != nil {
		return nil, err
	}

	for index := range result.Items {
		result.Items[index].DownloadURL = s.buildDownloadURL(c, result.Items[index].TokenID)
	}
	return result, nil
}

func (s *Service) getDashboardSummary(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	options := dashboardSummaryOptions{
		Window:                 c.QueryParam("window"),
		Bucket:                 c.QueryParam("bucket"),
		LeaderboardWindow:      c.QueryParam("leaderboard_window"),
		LeaderboardLimit:       parseBoundedInt(c.QueryParam("leaderboard_limit"), 10, 1, 10),
		RecentLimit:            parseBoundedInt(c.QueryParam("recent_limit"), 10, 1, 10),
		ContributorLimit:       parseBoundedInt(c.QueryParam("contributor_limit"), 10, 1, 10),
		RecentContributorLimit: parseBoundedInt(c.QueryParam("recent_contributor_limit"), 10, 1, 10),
	}

	readCtx, cancel := context.WithTimeout(c.Request().Context(), dashboardSummaryReadTimeout)
	defer cancel()

	payload, err := s.GetDashboardSummaryWithOptions(readCtx, requestContext.UserID, options)
	if err != nil {
		if !isReadDegradeError(err) {
			return err
		}
		if cached, ok := s.getCachedStaleDashboardSummary(requestContext.UserID, options); ok && cached.Value != nil {
			payload = annotateDashboardPayload(
				cached.Value,
				dataSourceStale,
				cached.GeneratedAt,
				staleTimestamp(cached.GeneratedAt),
				degradedReasonForError(err),
				true,
			)
		} else {
			payload = s.defaultDashboardSummaryPayload(options)
			payload = annotateDashboardPayload(
				payload,
				dataSourceUnavailable,
				"",
				"",
				degradedReasonForError(err),
				true,
			)
		}
		return c.JSON(http.StatusOK, payload)
	}
	return c.JSON(http.StatusOK, payload)
}

func uniqueClaimIDs(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	unique := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}
