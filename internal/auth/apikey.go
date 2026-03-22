package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/runtimecache"
)

type apiKeyCreatePayload struct {
	Name *string `json:"name"`
}

type apiKeyItem struct {
	ID         int64   `json:"id"`
	Name       *string `json:"name"`
	Prefix     string  `json:"prefix"`
	Token      *string `json:"token"`
	Status     string  `json:"status"`
	CreatedAt  string  `json:"created_at"`
	LastUsedAt *string `json:"last_used_at"`
}

type apiKeysPayload struct {
	Items  []apiKeyItem `json:"items"`
	Limit  int          `json:"limit"`
	Active int          `json:"active"`
}

func (s *Service) registerAPIKeyRoutes(e *echo.Echo) {
	e.GET("/me/api-keys", s.listAPIKeys)
	e.POST("/me/api-keys", s.createAPIKey)
	e.POST("/me/api-keys/:apiKeyID/revoke", s.revokeAPIKey)
	e.GET("/me/api-key-summary", s.getAPIKeySummary)
}

func (s *Service) listAPIKeys(c echo.Context) error {
	session, err := s.RequireSessionUser(c)
	if err != nil {
		return err
	}

	payload, err := s.getAPIKeysPayload(c.Request().Context(), session.UserID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) createAPIKey(c echo.Context) error {
	session, err := s.RequireSessionUser(c)
	if err != nil {
		return err
	}

	var payload apiKeyCreatePayload
	if c.Request().ContentLength > 0 {
		if err := c.Bind(&payload); err != nil && err != io.EOF {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON body.")
		}
	}

	created, err := s.createAPIKeyForUser(c.Request().Context(), session.UserID, payload.Name)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, created)
}

func (s *Service) revokeAPIKey(c echo.Context) error {
	session, err := s.RequireSessionUser(c)
	if err != nil {
		return err
	}

	apiKeyID, err := parseInt64Param(c.Param("apiKeyID"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid API key id.")
	}

	payload, err := s.revokeAPIKeyForUser(c.Request().Context(), session.UserID, apiKeyID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getAPIKeySummary(c echo.Context) error {
	session, err := s.RequireSessionUser(c)
	if err != nil {
		return err
	}

	payload, err := s.getAPIKeySummaryPayload(c.Request().Context(), session.UserID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, payload)
}

func (s *Service) getAPIKeysPayload(ctx context.Context, userID int64) (*apiKeysPayload, error) {
	if s.cache == nil {
		return s.loadAPIKeysPayload(ctx, userID)
	}

	key := runtimecache.BuildSnapshotCacheKey("user-apikeys", userID, fmt.Sprintf("v%d", s.cache.ScopeVersion("user-apikeys", userID)))
	return runtimecache.CacheJSON(s.cache, key, s.cfg.Cache.MeTTL, func() (*apiKeysPayload, error) {
		return s.loadAPIKeysPayload(ctx, userID)
	})
}

func (s *Service) getAPIKeySummaryPayload(ctx context.Context, userID int64) (map[string]int, error) {
	if s.cache == nil {
		return s.loadAPIKeySummaryPayload(ctx, userID)
	}

	key := runtimecache.BuildSnapshotCacheKey("user-apikey-summary", userID, fmt.Sprintf("v%d", s.cache.ScopeVersion("user-apikey-summary", userID)))
	return runtimecache.CacheJSON(s.cache, key, s.cfg.Cache.MeTTL, func() (map[string]int, error) {
		return s.loadAPIKeySummaryPayload(ctx, userID)
	})
}

func (s *Service) loadAPIKeysPayload(ctx context.Context, userID int64) (*apiKeysPayload, error) {
	items, err := s.listAPIKeyItems(ctx, userID)
	if err != nil {
		return nil, err
	}

	active := 0
	for _, item := range items {
		if item.Status == "active" {
			active++
		}
	}

	return &apiKeysPayload{
		Items:  items,
		Limit:  s.cfg.APIKeys.MaxPerUser,
		Active: active,
	}, nil
}

func (s *Service) loadAPIKeySummaryPayload(ctx context.Context, userID int64) (map[string]int, error) {
	payload, err := s.loadAPIKeysPayload(ctx, userID)
	if err != nil {
		return nil, err
	}
	return map[string]int{
		"limit":  payload.Limit,
		"active": payload.Active,
	}, nil
}

func (s *Service) listAPIKeyItems(ctx context.Context, userID int64) ([]apiKeyItem, error) {
	rows, err := s.store.DB().QueryContext(ctx, `
		SELECT id, name, key_prefix, key_value, status, created_at_ts, last_used_at_ts
		FROM api_keys
		WHERE user_id = ?
		ORDER BY id DESC
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("list api keys for user %d: %w", userID, err)
	}
	defer rows.Close()

	var items []apiKeyItem
	for rows.Next() {
		var (
			item         apiKeyItem
			name         sql.NullString
			token        sql.NullString
			lastUsedAtTS sql.NullInt64
			createdAtTS  int64
		)
		if err := rows.Scan(&item.ID, &name, &item.Prefix, &token, &item.Status, &createdAtTS, &lastUsedAtTS); err != nil {
			return nil, fmt.Errorf("scan api key item: %w", err)
		}
		if name.Valid {
			value := name.String
			item.Name = &value
		}
		if token.Valid {
			value := token.String
			item.Token = &value
		}
		item.CreatedAt = isoformatFromTS(createdAtTS)
		if lastUsedAtTS.Valid {
			value := isoformatFromTS(lastUsedAtTS.Int64)
			item.LastUsedAt = &value
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate api key items: %w", err)
	}

	return items, nil
}

func (s *Service) createAPIKeyForUser(ctx context.Context, userID int64, name *string) (*apiKeysPayload, error) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin api key create transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var activeCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM api_keys
		WHERE user_id = ? AND status = 'active'
	`, userID).Scan(&activeCount); err != nil {
		return nil, fmt.Errorf("count active api keys: %w", err)
	}
	if activeCount >= s.cfg.APIKeys.MaxPerUser {
		return nil, echo.NewHTTPError(http.StatusForbidden, "API key limit reached.")
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		return nil, fmt.Errorf("generate api key: %w", err)
	}
	now := nowUnix()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO api_keys (
			user_id,
			name,
			key_hash,
			key_prefix,
			key_value,
			status,
			created_at_ts
		) VALUES (?, ?, ?, ?, ?, 'active', ?)
	`, userID, nullableOptionalString(name), hashAPIKey(apiKey), apiKeyPrefix(apiKey), apiKey, now); err != nil {
		return nil, fmt.Errorf("insert api key: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit api key create transaction: %w", err)
	}

	s.invalidateUserAPIKeysCache(userID)
	s.invalidateUserProfileCache(userID)
	return s.getAPIKeysPayload(ctx, userID)
}

func (s *Service) revokeAPIKeyForUser(ctx context.Context, userID int64, apiKeyID int64) (*apiKeysPayload, error) {
	if _, err := s.store.DB().ExecContext(ctx, `
		UPDATE api_keys
		SET status = 'revoked', revoked_at_ts = ?
		WHERE id = ? AND user_id = ?
	`, nowUnix(), apiKeyID, userID); err != nil {
		return nil, fmt.Errorf("revoke api key %d: %w", apiKeyID, err)
	}

	s.invalidateUserAPIKeysCache(userID)
	s.invalidateUserProfileCache(userID)
	return s.getAPIKeysPayload(ctx, userID)
}

func generateAPIKey() (string, error) {
	buffer := make([]byte, 24)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return "tk_" + base64.RawURLEncoding.EncodeToString(buffer), nil
}

func apiKeyPrefix(value string) string {
	if len(value) <= 8 {
		return value
	}
	return value[:8]
}

func parseInt64Param(value string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(value), 10, 64)
}

func nullableOptionalString(value *string) any {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}
