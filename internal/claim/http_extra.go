package claim

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
	"token-atlas/internal/runtimecache"
)

type uploadTokensPayload struct {
	Files []uploadFileInput `json:"files"`
}

type adminBanPayload struct {
	Reason    string  `json:"reason"`
	ExpiresAt *string `json:"expires_at"`
}

type tokenCleanupPayload struct {
	Mode string `json:"mode"`
}

type adminQueueCancelPayload struct {
	Reason string `json:"reason"`
}

func (s *Service) getQueueStream(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	response := c.Response()
	response.Header().Set(echo.HeaderContentType, "text/event-stream")
	response.Header().Set(echo.HeaderCacheControl, "no-cache")
	response.Header().Set("Connection", "keep-alive")
	response.Header().Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)

	flusher, ok := response.Writer.(http.Flusher)
	if !ok {
		return echo.NewHTTPError(http.StatusInternalServerError, "Streaming unsupported.")
	}

	subscription, lastVersion := s.claimEvents.subscribe(requestContext.UserID)
	defer s.claimEvents.unsubscribe(subscription)

	writeStatus := func() error {
		payload, err := s.GetClaimRealtimeSnapshot(c.Request().Context(), requestContext.UserID, requestContext.SessionID)
		if err != nil {
			return err
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := response.Write([]byte("event: claim_snapshot\ndata: " + string(body) + "\n\n")); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if _, err := response.Write([]byte("event: stream_status\ndata: {\"status\":\"ready\",\"transport\":\"sse\"}\n\n")); err != nil {
		return nil
	}
	flusher.Flush()

	if err := writeStatus(); err != nil {
		return err
	}

	keepAliveSec := 10 * time.Second
	if queuePumpInterval*3 > keepAliveSec {
		keepAliveSec = queuePumpInterval * 3
	}
	keepAliveTicker := time.NewTicker(keepAliveSec)
	defer keepAliveTicker.Stop()

	for {
		select {
		case <-c.Request().Context().Done():
			return nil
		case version := <-subscription.ch:
			if version == lastVersion {
				continue
			}
			lastVersion = version
			if err := writeStatus(); err != nil {
				return nil
			}
		case <-keepAliveTicker.C:
			if _, err := response.Write([]byte(": keepalive\n\n")); err != nil {
				return nil
			}
			flusher.Flush()
		}
	}
}

func (s *Service) getUploadResultsStream(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	response := c.Response()
	response.Header().Set(echo.HeaderContentType, "text/event-stream")
	response.Header().Set(echo.HeaderCacheControl, "no-cache")
	response.Header().Set("Connection", "keep-alive")
	response.Header().Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)

	flusher, ok := response.Writer.(http.Flusher)
	if !ok {
		return echo.NewHTTPError(http.StatusInternalServerError, "Streaming unsupported.")
	}

	subscription, lastVersion := s.uploadEvents.subscribe(requestContext.UserID)
	defer s.uploadEvents.unsubscribe(subscription)

	writeResults := func() error {
		payload := s.GetUploadResults(requestContext.UserID)
		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := response.Write([]byte("event: upload_results\ndata: " + string(body) + "\n\n")); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := writeResults(); err != nil {
		return err
	}

	keepAliveSec := 10 * time.Second
	if queuePumpInterval*3 > keepAliveSec {
		keepAliveSec = queuePumpInterval * 3
	}
	keepAliveTicker := time.NewTicker(keepAliveSec)
	defer keepAliveTicker.Stop()

	for {
		select {
		case <-c.Request().Context().Done():
			return nil
		case version := <-subscription.ch:
			if version == lastVersion {
				continue
			}
			lastVersion = version
			if err := writeResults(); err != nil {
				return nil
			}
		case <-keepAliveTicker.C:
			if _, err := response.Write([]byte(": keepalive\n\n")); err != nil {
				return nil
			}
			flusher.Flush()
		}
	}
}

func (s *Service) uploadTokens(c echo.Context) error {
	if err := s.requireWebUploadRequest(c); err != nil {
		return err
	}

	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	var payload uploadTokensPayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	result, err := s.QueueUploadBatch(c.Request().Context(), requestContext, payload.Files)
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) downloadClaimedToken(c echo.Context) error {
	tokenID, err := strconv.ParseInt(c.Param("token_id"), 10, 64)
	if err != nil || tokenID <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid token id.")
	}

	userID, err := s.resolveDownloadUserID(c)
	if err != nil {
		return err
	}

	item, err := s.GetClaimedTokenForDownload(c.Request().Context(), tokenID, userID)
	if err != nil {
		return mapClaimDataError(err)
	}
	if item == nil {
		access, err := s.GetClaimDownloadAccessSummary(c.Request().Context(), tokenID, userID)
		if err != nil {
			return err
		}
		if access.OtherVisibleClaim {
			return echo.NewHTTPError(http.StatusForbidden, "You do not have access to this claimed token.")
		}
		if access.UserHiddenClaim {
			return echo.NewHTTPError(http.StatusNotFound, "Claimed token download is no longer available.")
		}
		return echo.NewHTTPError(http.StatusNotFound, "Claimed token download not found.")
	}

	contentBytes, err := encodeDownloadContent(item)
	if err != nil {
		return err
	}

	c.Response().Header().Set(echo.HeaderContentType, "application/json")
	c.Response().Header().Set(echo.HeaderContentDisposition, buildContentDisposition(item.FileName))
	return c.Blob(http.StatusOK, "application/json", contentBytes)
}

func (s *Service) getAdminMe(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	payload, err := s.GetAdminMe(c.Request().Context(), requestContext)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getAdminBootstrap(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	payload, err := s.GetAdminBootstrap(c.Request().Context(), requestContext)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) adminListUsers(c echo.Context) error {
	search := c.QueryParam("search")
	banStatus := c.QueryParam("ban_status")
	limit := parseBoundedInt(c.QueryParam("limit"), 100, 1, 200)
	offset := parseBoundedInt(c.QueryParam("offset"), 0, 0, 1<<30)
	payload, err := s.cachedAdminUsersPage(c.Request().Context(), search, banStatus, limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) adminGetUserDetail(c echo.Context) error {
	linuxDOUserID := c.Param("linuxdo_user_id")
	cacheKey := s.adminCacheKey("admin-user-detail", strings.TrimSpace(linuxDOUserID))
	payload, err := runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.AdminTTL, func() (map[string]any, error) {
		return s.GetAdminUserDetail(c.Request().Context(), linuxDOUserID)
	})
	if err != nil {
		return err
	}
	if payload == nil {
		return echo.NewHTTPError(http.StatusNotFound, "User not found.")
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) adminBanUser(c echo.Context) error {
	var payload adminBanPayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	targetUser, err := s.getUserByLinuxDOID(c.Request().Context(), c.Param("linuxdo_user_id"))
	if err != nil {
		return err
	}
	if targetUser == nil {
		return echo.NewHTTPError(http.StatusNotFound, "User not found.")
	}

	expiresAtTS, err := parseExpiresAtToTS(payload.ExpiresAt)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid expires_at. Use ISO datetime or leave empty.")
	}
	if expiresAtTS != nil && *expiresAtTS <= time.Now().Unix() {
		return echo.NewHTTPError(http.StatusBadRequest, "expires_at must be in the future.")
	}

	ban, err := s.BanUser(c.Request().Context(), targetUser.LinuxDOUserID, targetUser.LinuxDOUsername, payload.Reason, requestContext.UserID, expiresAtTS)
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	s.invalidateUserCache(targetUser.ID)
	s.invalidateUserQueueCache(targetUser.ID)
	s.invalidateAdminCache()
	return c.JSON(http.StatusOK, map[string]any{"ok": true, "ban": ban})
}

func (s *Service) adminUnbanUser(c echo.Context) error {
	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	changed, err := s.UnbanUser(c.Request().Context(), c.Param("linuxdo_user_id"), requestContext.UserID)
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	if changed {
		targetUser, lookupErr := s.getUserByLinuxDOID(c.Request().Context(), c.Param("linuxdo_user_id"))
		if lookupErr != nil {
			return lookupErr
		}
		if targetUser != nil {
			s.invalidateUserCache(targetUser.ID)
			s.invalidateUserQueueCache(targetUser.ID)
		}
		s.invalidateAdminCache()
	}
	return c.JSON(http.StatusOK, map[string]any{"ok": changed})
}

func (s *Service) adminListBans(c echo.Context) error {
	statusFilter := c.QueryParam("status")
	search := c.QueryParam("search")
	limit := parseBoundedInt(c.QueryParam("limit"), 100, 1, 200)
	offset := parseBoundedInt(c.QueryParam("offset"), 0, 0, 1<<30)
	payload, err := s.cachedAdminBansPage(c.Request().Context(), statusFilter, search, limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) adminListTokens(c echo.Context) error {
	search := c.QueryParam("search")
	statusFilter := c.QueryParam("status")
	limit := parseBoundedInt(c.QueryParam("limit"), 200, 1, 500)
	offset := parseBoundedInt(c.QueryParam("offset"), 0, 0, 1<<30)
	payload, err := s.cachedAdminTokensPage(c.Request().Context(), search, statusFilter, limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) adminListQueue(c echo.Context) error {
	search := c.QueryParam("search")
	statusFilter := c.QueryParam("status")
	onlyFilter := c.QueryParam("only")
	limit := parseBoundedInt(c.QueryParam("limit"), 100, 1, 200)
	offset := parseBoundedInt(c.QueryParam("offset"), 0, 0, 1<<30)
	payload, err := s.cachedAdminQueuePage(c.Request().Context(), search, statusFilter, onlyFilter, limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) adminRefreshQueue(c echo.Context) error {
	result, err := s.RefreshQueue(c.Request().Context())
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) adminCancelQueueEntry(c echo.Context) error {
	var payload adminQueueCancelPayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	queueID, err := strconv.ParseInt(c.Param("queue_id"), 10, 64)
	if err != nil || queueID <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid queue id.")
	}

	trimmedReason := strings.TrimSpace(payload.Reason)
	if trimmedReason == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "取消原因必填。")
	}

	entry, err := s.cancelQueueEntry(c.Request().Context(), queueID, queueStatusCancelled, "admin:"+trimmedReason, &requestContext.UserID)
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	if entry == nil {
		return echo.NewHTTPError(http.StatusNotFound, "Queue entry not found.")
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ok":            true,
		"queue_id":      entry.ID,
		"user_id":       entry.UserID,
		"status":        queueStatusCancelled,
		"cancel_reason": "admin:" + trimmedReason,
	})
}

func (s *Service) adminCancelUserQueue(c echo.Context) error {
	var payload adminQueueCancelPayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	requestContext, ok := auth.RequestContextFromEcho(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	targetUserID, err := strconv.ParseInt(c.Param("user_id"), 10, 64)
	if err != nil || targetUserID <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid user id.")
	}

	trimmedReason := strings.TrimSpace(payload.Reason)
	if trimmedReason == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "取消原因必填。")
	}

	items, err := s.cancelQueuedEntriesByUser(c.Request().Context(), targetUserID, queueStatusCancelled, "admin:"+trimmedReason, &requestContext.UserID)
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	if len(items) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, "No active queue entries for this user.")
	}

	queueIDs := make([]int64, 0, len(items))
	for _, item := range items {
		queueIDs = append(queueIDs, item.ID)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ok":            true,
		"user_id":       targetUserID,
		"cancelled":     len(items),
		"queue_ids":     queueIDs,
		"status":        queueStatusCancelled,
		"cancel_reason": "admin:" + trimmedReason,
	})
}

func (s *Service) adminActivateToken(c echo.Context) error {
	return s.adminSetTokenEnabled(c, true)
}

func (s *Service) adminDeactivateToken(c echo.Context) error {
	return s.adminSetTokenEnabled(c, false)
}

func (s *Service) adminSetTokenEnabled(c echo.Context, enabled bool) error {
	tokenID, err := strconv.ParseInt(c.Param("token_id"), 10, 64)
	if err != nil || tokenID <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid token id.")
	}

	item, err := s.SetTokenEnabled(c.Request().Context(), tokenID, enabled)
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	if item == nil {
		return echo.NewHTTPError(http.StatusNotFound, "Token not found.")
	}
	return c.JSON(http.StatusOK, map[string]any{"ok": true, "item": item})
}

func (s *Service) adminCleanupExhaustedTokens(c echo.Context) error {
	var payload tokenCleanupPayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	result, err := s.CleanupExhaustedTokens(c.Request().Context(), payload.Mode)
	if err != nil {
		return mapDatabaseBusyError(c, err)
	}
	result["ok"] = true
	return c.JSON(http.StatusOK, result)
}

func (s *Service) adminGetPolicy(c echo.Context) error {
	payload, err := s.GetAdminPolicy(c.Request().Context())
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) resolveDownloadUserID(c echo.Context) (int64, error) {
	if apiKey := extractAPIKeyFromRequest(c); apiKey != "" {
		record, err := s.auth.RequireAPIKey(c)
		if err != nil {
			return 0, err
		}
		return record.UserID, nil
	}

	requestContext, err := s.auth.RequireSessionUser(c)
	if err != nil {
		return 0, err
	}
	return requestContext.UserID, nil
}

func extractAPIKeyFromRequest(c echo.Context) string {
	for _, candidate := range []string{
		c.Request().Header.Get("X-API-Key"),
		c.Request().Header.Get("X-Access-Key"),
		c.QueryParam("key"),
	} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func parseBoundedInt(raw string, fallback int, minimum int, maximum int) int {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func parseExpiresAtToTS(raw *string) (*int64, error) {
	if raw == nil {
		return nil, nil
	}
	trimmed := strings.TrimSpace(*raw)
	if trimmed == "" {
		return nil, nil
	}

	for _, layout := range []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04",
		"2006-01-02T15:04:05",
	} {
		parsed, err := time.ParseInLocation(layout, trimmed, time.Local)
		if err == nil {
			value := parsed.Unix()
			return &value, nil
		}
	}
	return nil, strconv.ErrSyntax
}

func (s *Service) requireWebUploadRequest(c echo.Context) error {
	origin := strings.TrimRight(strings.TrimSpace(c.Request().Header.Get("Origin")), "/")
	referer := strings.TrimSpace(c.Request().Header.Get("Referer"))
	fetchSite := strings.ToLower(strings.TrimSpace(c.Request().Header.Get("Sec-Fetch-Site")))
	uploadSource := strings.ToLower(strings.TrimSpace(c.Request().Header.Get("X-Upload-Source")))
	expectedOrigin := s.expectedRequestOrigin(c)

	switch {
	case uploadSource != "web":
		return echo.NewHTTPError(http.StatusForbidden, "上传仅允许从网页端发起。")
	case origin != expectedOrigin:
		return echo.NewHTTPError(http.StatusForbidden, "上传来源无效。")
	case fetchSite != "same-origin" && fetchSite != "same-site":
		return echo.NewHTTPError(http.StatusForbidden, "仅允许网页端同源上传。")
	case referer != "" && !strings.HasPrefix(referer, expectedOrigin+"/"):
		return echo.NewHTTPError(http.StatusForbidden, "上传来源无效。")
	default:
		return nil
	}
}

func (s *Service) expectedRequestOrigin(c echo.Context) string {
	if baseURL := strings.TrimRight(strings.TrimSpace(s.cfg.Server.ProviderBaseURL), "/"); baseURL != "" {
		if idx := strings.Index(baseURL, "://"); idx > 0 {
			scheme := baseURL[:idx]
			rest := baseURL[idx+3:]
			host := rest
			if slash := strings.Index(rest, "/"); slash >= 0 {
				host = rest[:slash]
			}
			if scheme != "" && host != "" {
				return scheme + "://" + host
			}
		}
	}

	forwardedProto := strings.TrimSpace(strings.SplitN(c.Request().Header.Get("X-Forwarded-Proto"), ",", 2)[0])
	forwardedHost := strings.TrimSpace(strings.SplitN(c.Request().Header.Get("X-Forwarded-Host"), ",", 2)[0])
	if forwardedProto != "" && forwardedHost != "" {
		return strings.TrimRight(forwardedProto+"://"+forwardedHost, "/")
	}

	return strings.TrimRight(c.Scheme()+"://"+c.Request().Host, "/")
}
