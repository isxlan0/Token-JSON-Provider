package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/config"
	"token-atlas/internal/database"
	"token-atlas/internal/runtimecache"
)

const (
	apiKeyHeader          = "X-API-Key"
	legacyAccessKeyHeader = "X-Access-Key"

	contextKeyRequestContext = "auth.request_context"
	contextKeyAPIKeyRecord   = "auth.api_key_record"
)

type Service struct {
	cfg      config.Config
	store    *database.Store
	sessions *SessionManager
	client   *http.Client
	cache    *runtimecache.AppCache
}

type LinuxDOUser struct {
	ID             int64   `json:"id"`
	Username       string  `json:"username"`
	Name           *string `json:"name"`
	Active         bool    `json:"active"`
	TrustLevel     int64   `json:"trust_level"`
	Silenced       bool    `json:"silenced"`
	AvatarTemplate *string `json:"avatar_template"`
}

type RequestContext struct {
	UserID   int64 `json:"user_id"`
	DBUser   *database.User
	User     UserPayload `json:"user"`
	IsAdmin  bool        `json:"is_admin"`
	Ban      *BanPayload `json:"ban"`
	IsBanned bool        `json:"is_banned"`
}

type UserPayload struct {
	ID         string `json:"id"`
	Username   string `json:"username"`
	Name       string `json:"name"`
	TrustLevel int64  `json:"trust_level"`
	IsAdmin    bool   `json:"is_admin"`
}

type BanPayload struct {
	ID               int64         `json:"id"`
	LinuxDOUserID    string        `json:"linuxdo_user_id"`
	UsernameSnapshot *string       `json:"username_snapshot"`
	Reason           string        `json:"reason"`
	BannedAt         string        `json:"banned_at"`
	ExpiresAt        *string       `json:"expires_at"`
	UnbannedAt       *string       `json:"unbanned_at"`
	BannedBy         *ActorPayload `json:"banned_by"`
	UnbannedBy       *ActorPayload `json:"unbanned_by"`
	IsActive         bool          `json:"is_active"`
}

type ActorPayload struct {
	Username string `json:"username"`
	Name     string `json:"name"`
}

type APIKeyRecord struct {
	APIKeyID int64 `json:"api_key_id"`
	UserID   int64 `json:"user_id"`
}

func NewService(cfg config.Config, store *database.Store, client *http.Client, cache *runtimecache.AppCache) *Service {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	return &Service{
		cfg:      cfg,
		store:    store,
		sessions: NewSessionManager(cfg.Session.Secret),
		client:   client,
		cache:    cache,
	}
}

func (s *Service) Enabled() bool {
	return strings.TrimSpace(s.cfg.LinuxDO.ClientID) != "" && strings.TrimSpace(s.cfg.LinuxDO.ClientSecret) != ""
}

func (s *Service) Sessions() *SessionManager {
	return s.sessions
}

func (s *Service) RegisterRoutes(e *echo.Echo) {
	e.POST("/auth/login", s.loginRemoved)
	e.GET("/auth/linuxdo/login", s.startLinuxDOLogin)
	e.GET("/auth/linuxdo/callback", s.linuxDOCallback)
	e.GET("/auth/status", s.getAuthStatus)
	e.POST("/auth/logout", s.logout)
	s.registerAPIKeyRoutes(e)
}

func (s *Service) RequireUserMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, err := s.RequireSessionUser(c)
		if err != nil {
			return err
		}
		c.Set(contextKeyRequestContext, ctx)
		return next(c)
	}
}

func (s *Service) RequireAdminMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, err := s.RequireAdminUser(c)
		if err != nil {
			return err
		}
		c.Set(contextKeyRequestContext, ctx)
		return next(c)
	}
}

func (s *Service) RequireAPIKeyMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		record, err := s.RequireAPIKey(c)
		if err != nil {
			return err
		}
		c.Set(contextKeyAPIKeyRecord, record)
		return next(c)
	}
}

func RequestContextFromEcho(c echo.Context) (*RequestContext, bool) {
	value := c.Get(contextKeyRequestContext)
	context, ok := value.(*RequestContext)
	return context, ok
}

func APIKeyRecordFromEcho(c echo.Context) (*APIKeyRecord, bool) {
	value := c.Get(contextKeyAPIKeyRecord)
	record, ok := value.(*APIKeyRecord)
	return record, ok
}

func (s *Service) RequireSessionUser(c echo.Context) (*RequestContext, error) {
	ctx, err := s.getRequestContext(c)
	if err != nil {
		return nil, err
	}
	if ctx.IsBanned {
		return nil, echo.NewHTTPError(http.StatusForbidden, map[string]any{
			"detail": "当前账号已被封禁",
			"ban":    ctx.Ban,
			"user":   ctx.User,
		})
	}
	return ctx, nil
}

func (s *Service) RequireAdminUser(c echo.Context) (*RequestContext, error) {
	ctx, err := s.getRequestContext(c)
	if err != nil {
		return nil, err
	}
	if !ctx.IsAdmin {
		return nil, echo.NewHTTPError(http.StatusForbidden, "Admin access required.")
	}
	return ctx, nil
}

func (s *Service) RequireAPIKey(c echo.Context) (*APIKeyRecord, error) {
	apiKey := extractAPIKey(c)
	if apiKey == "" {
		return nil, echo.NewHTTPError(http.StatusUnauthorized, "API key is required.")
	}

	record, err := s.resolveAPIKey(c.Request().Context(), apiKey)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, echo.NewHTTPError(http.StatusUnauthorized, "Invalid API key.")
	}

	owner, err := s.getUserByID(c.Request().Context(), record.UserID)
	if err != nil {
		return nil, err
	}
	if owner == nil {
		return nil, echo.NewHTTPError(http.StatusUnauthorized, "Invalid API key owner.")
	}

	ban, err := s.getActiveBan(c.Request().Context(), owner.LinuxDOUserID)
	if err != nil {
		return nil, err
	}
	if ban != nil {
		return nil, echo.NewHTTPError(http.StatusForbidden, map[string]any{
			"detail": "当前账号已被封禁",
			"ban":    ban,
			"user": map[string]any{
				"id":       owner.LinuxDOUserID,
				"username": owner.LinuxDOUsername,
				"name":     nullableString(owner.LinuxDOName, owner.LinuxDOUsername),
			},
		})
	}

	return record, nil
}

func (s *Service) getRequestContext(c echo.Context) (*RequestContext, error) {
	sessionState := s.sessions.Read(c)
	if sessionState.Auth == nil || sessionState.Auth.Method != "linuxdo" || sessionState.Auth.UserID == 0 {
		return nil, echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	dbUser, err := s.getUserByID(c.Request().Context(), sessionState.Auth.UserID)
	if err != nil {
		return nil, err
	}
	if dbUser == nil {
		return nil, echo.NewHTTPError(http.StatusUnauthorized, "Authentication required.")
	}

	username := dbUser.LinuxDOUsername
	userPayload := UserPayload{
		ID:         dbUser.LinuxDOUserID,
		Username:   username,
		Name:       nullableString(dbUser.LinuxDOName, username),
		TrustLevel: dbUser.TrustLevel,
		IsAdmin:    s.isAdminIdentity(dbUser.LinuxDOUserID, username),
	}

	ban, err := s.getActiveBan(c.Request().Context(), dbUser.LinuxDOUserID)
	if err != nil {
		return nil, err
	}

	return &RequestContext{
		UserID:   dbUser.ID,
		DBUser:   dbUser,
		User:     userPayload,
		IsAdmin:  userPayload.IsAdmin,
		Ban:      ban,
		IsBanned: ban != nil,
	}, nil
}

func (s *Service) isAdminIdentity(linuxDOUserID string, username string) bool {
	if _, ok := s.cfg.APIKeys.AdminIdentities.IDs[strings.TrimSpace(linuxDOUserID)]; ok {
		return true
	}
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(username), "@")))
	_, ok := s.cfg.APIKeys.AdminIdentities.Usernames[normalized]
	return ok
}

func (s *Service) upsertUser(ctx context.Context, user LinuxDOUser) (int64, error) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin user upsert transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := nowUnix()
	var existingID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM users WHERE linuxdo_user_id = ?`, fmt.Sprint(user.ID)).Scan(&existingID)
	switch {
	case err == nil:
		if _, err := tx.ExecContext(ctx, `
			UPDATE users
			SET linuxdo_username = ?,
				linuxdo_name = ?,
				trust_level = ?,
				last_login_at_ts = ?
			WHERE id = ?
		`, user.Username, nullableName(user.Name), user.TrustLevel, now, existingID); err != nil {
			return 0, fmt.Errorf("update user: %w", err)
		}
	case err == sql.ErrNoRows:
		result, execErr := tx.ExecContext(ctx, `
			INSERT INTO users (
				linuxdo_user_id,
				linuxdo_username,
				linuxdo_name,
				trust_level,
				created_at_ts,
				last_login_at_ts
			) VALUES (?, ?, ?, ?, ?, ?)
		`, fmt.Sprint(user.ID), user.Username, nullableName(user.Name), user.TrustLevel, now, now)
		if execErr != nil {
			return 0, fmt.Errorf("insert user: %w", execErr)
		}
		insertedID, execErr := result.LastInsertId()
		if execErr != nil {
			return 0, fmt.Errorf("read inserted user id: %w", execErr)
		}
		existingID = insertedID
	default:
		return 0, fmt.Errorf("select existing user: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit user upsert transaction: %w", err)
	}

	return existingID, nil
}

func (s *Service) getUserByID(ctx context.Context, userID int64) (*database.User, error) {
	row := s.store.DB().QueryRowContext(ctx, `
		SELECT id, linuxdo_user_id, linuxdo_username, linuxdo_name, trust_level, created_at_ts, last_login_at_ts
		FROM users
		WHERE id = ?
	`, userID)

	var user database.User
	if err := row.Scan(
		&user.ID,
		&user.LinuxDOUserID,
		&user.LinuxDOUsername,
		&user.LinuxDOName,
		&user.TrustLevel,
		&user.CreatedAtTS,
		&user.LastLoginAtTS,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query user by id %d: %w", userID, err)
	}

	return &user, nil
}

func (s *Service) resolveAPIKey(ctx context.Context, apiKey string) (*APIKeyRecord, error) {
	row := s.store.DB().QueryRowContext(ctx, `
		SELECT id, user_id
		FROM api_keys
		WHERE key_hash = ? AND status = 'active'
	`, hashAPIKey(apiKey))

	record := &APIKeyRecord{}
	if err := row.Scan(&record.APIKeyID, &record.UserID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("resolve api key: %w", err)
	}

	return record, nil
}

func (s *Service) getActiveBan(ctx context.Context, linuxDOUserID string) (*BanPayload, error) {
	row := s.store.DB().QueryRowContext(ctx, `
		SELECT user_bans.id,
		       user_bans.linuxdo_user_id,
		       user_bans.username_snapshot,
		       user_bans.reason,
		       user_bans.banned_at_ts,
		       user_bans.expires_at_ts,
		       user_bans.unbanned_at_ts,
		       users.linuxdo_username as banned_by_username,
		       users.linuxdo_name as banned_by_name,
		       unbanners.linuxdo_username as unbanned_by_username,
		       unbanners.linuxdo_name as unbanned_by_name
		FROM user_bans
		LEFT JOIN users ON users.id = user_bans.banned_by_user_id
		LEFT JOIN users AS unbanners ON unbanners.id = user_bans.unbanned_by_user_id
		WHERE user_bans.linuxdo_user_id = ?
		  AND user_bans.unbanned_at_ts IS NULL
		  AND (user_bans.expires_at_ts IS NULL OR user_bans.expires_at_ts > ?)
		ORDER BY user_bans.banned_at_ts DESC, user_bans.id DESC
		LIMIT 1
	`, linuxDOUserID, nowUnix())

	var (
		ban                BanPayload
		usernameSnapshot   sql.NullString
		bannedAtTS         int64
		expiresAtTS        sql.NullInt64
		unbannedAtTS       sql.NullInt64
		bannedByUsername   sql.NullString
		bannedByName       sql.NullString
		unbannedByUsername sql.NullString
		unbannedByName     sql.NullString
	)

	if err := row.Scan(
		&ban.ID,
		&ban.LinuxDOUserID,
		&usernameSnapshot,
		&ban.Reason,
		&bannedAtTS,
		&expiresAtTS,
		&unbannedAtTS,
		&bannedByUsername,
		&bannedByName,
		&unbannedByUsername,
		&unbannedByName,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query active ban for %s: %w", linuxDOUserID, err)
	}

	if usernameSnapshot.Valid {
		value := usernameSnapshot.String
		ban.UsernameSnapshot = &value
	}
	ban.BannedAt = isoformatFromTS(bannedAtTS)
	if expiresAtTS.Valid {
		value := isoformatFromTS(expiresAtTS.Int64)
		ban.ExpiresAt = &value
	}
	if unbannedAtTS.Valid {
		value := isoformatFromTS(unbannedAtTS.Int64)
		ban.UnbannedAt = &value
	}
	if bannedByUsername.Valid {
		ban.BannedBy = &ActorPayload{
			Username: bannedByUsername.String,
			Name:     nullableString(bannedByName, bannedByUsername.String),
		}
	}
	if unbannedByUsername.Valid {
		ban.UnbannedBy = &ActorPayload{
			Username: unbannedByUsername.String,
			Name:     nullableString(unbannedByName, unbannedByUsername.String),
		}
	}
	ban.IsActive = !unbannedAtTS.Valid && (!expiresAtTS.Valid || expiresAtTS.Int64 > nowUnix())

	return &ban, nil
}

func extractAPIKey(c echo.Context) string {
	if value := strings.TrimSpace(c.Request().Header.Get(apiKeyHeader)); value != "" {
		return value
	}
	if value := strings.TrimSpace(c.Request().Header.Get(legacyAccessKeyHeader)); value != "" {
		return value
	}
	return strings.TrimSpace(c.QueryParam("key"))
}

func hashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

func isoformatNow() string {
	return time.Now().UTC().Local().Format(time.RFC3339)
}

func isoformatFromTS(value int64) string {
	return time.Unix(value, 0).UTC().Local().Format(time.RFC3339)
}

func nullableString(value sql.NullString, fallback string) string {
	if value.Valid && strings.TrimSpace(value.String) != "" {
		return value.String
	}
	return fallback
}

func nullableName(value *string) any {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func nowUnix() int64 {
	return time.Now().Unix()
}

func (s *Service) invalidateUserProfileCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user-basic", userID)
	s.cache.BumpScope("user-profile", userID)
}

func (s *Service) invalidateUserAPIKeysCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user-apikeys", userID)
	s.cache.BumpScope("user-apikey-summary", userID)
	s.cache.BumpScope("user-profile", userID)
}
