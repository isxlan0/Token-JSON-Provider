package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v4"
)

const (
	linuxDOAuthorizeURL = "https://connect.linux.do/oauth2/authorize"
	linuxDOTokenURL     = "https://connect.linux.do/oauth2/token"
	linuxDOUserURL      = "https://connect.linux.do/api/user"
)

func (s *Service) buildLinuxDORedirectURI(c echo.Context) string {
	if value := strings.TrimSpace(s.cfg.LinuxDO.RedirectURI); value != "" {
		return value
	}
	return fmt.Sprintf("%s://%s/auth/linuxdo/callback", c.Scheme(), c.Request().Host)
}

func (s *Service) buildLinuxDOAuthorizeURL(c echo.Context, state string) string {
	query := url.Values{}
	query.Set("client_id", s.cfg.LinuxDO.ClientID)
	query.Set("response_type", "code")
	query.Set("redirect_uri", s.buildLinuxDORedirectURI(c))
	query.Set("scope", s.cfg.LinuxDO.Scope)
	query.Set("state", state)
	return linuxDOAuthorizeURL + "?" + query.Encode()
}

func (s *Service) exchangeLinuxDOCode(ctx context.Context, c echo.Context, code string) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", s.buildLinuxDORedirectURI(c))

	credentials := s.cfg.LinuxDO.ClientID + ":" + s.cfg.LinuxDO.ClientSecret
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, linuxDOTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build linuxdo token request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(credentials)))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	response, err := s.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("exchange linuxdo code: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("read linuxdo token response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("linuxdo token endpoint returned status %d", response.StatusCode)
	}

	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("decode linuxdo token response: %w", err)
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", fmt.Errorf("linuxdo token response did not include access_token")
	}
	return strings.TrimSpace(payload.AccessToken), nil
}

func (s *Service) fetchLinuxDOUser(ctx context.Context, accessToken string) (*LinuxDOUser, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, linuxDOUserURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build linuxdo user request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+accessToken)

	response, err := s.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch linuxdo user: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read linuxdo user response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("linuxdo user endpoint returned status %d", response.StatusCode)
	}

	var user LinuxDOUser
	if err := json.Unmarshal(body, &user); err != nil {
		return nil, fmt.Errorf("decode linuxdo user response: %w", err)
	}
	return &user, nil
}

func (s *Service) validateLinuxDOUser(user *LinuxDOUser) error {
	if user == nil {
		return echo.NewHTTPError(http.StatusForbidden, "Linux.do user is not active")
	}
	if !user.Active {
		return echo.NewHTTPError(http.StatusForbidden, "Linux.do user is not active")
	}
	if user.Silenced {
		return echo.NewHTTPError(http.StatusForbidden, "Linux.do user is silenced")
	}
	if user.TrustLevel < int64(s.cfg.LinuxDO.MinTrustLevel) {
		return echo.NewHTTPError(http.StatusForbidden, "Linux.do trust level is below the configured minimum")
	}
	if len(s.cfg.LinuxDO.AllowedIDs) > 0 {
		if _, ok := s.cfg.LinuxDO.AllowedIDs[fmt.Sprint(user.ID)]; !ok {
			return echo.NewHTTPError(http.StatusForbidden, "Linux.do user is not in the allowlist")
		}
	}
	return nil
}

func (s *Service) startLinuxDOLogin(c echo.Context) error {
	if !s.Enabled() {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "Linux.do OAuth is not configured.")
	}

	next := c.QueryParam("next")
	if next == "" {
		next = "/"
	}

	sessionState := s.sessions.Read(c)
	if isSafeRelativeRedirect(next) {
		sessionState.PostLoginRedirect = next
	} else {
		sessionState.PostLoginRedirect = "/"
	}
	sessionState.OAuthState = randomState()

	if err := s.sessions.Write(c, sessionState); err != nil {
		return err
	}
	return c.Redirect(http.StatusFound, s.buildLinuxDOAuthorizeURL(c, sessionState.OAuthState))
}

func (s *Service) linuxDOCallback(c echo.Context) error {
	sessionState := s.sessions.Read(c)
	expectedState := sessionState.OAuthState
	postLoginRedirect := sessionState.PostLoginRedirect
	sessionState.OAuthState = ""
	sessionState.PostLoginRedirect = ""
	if !isSafeRelativeRedirect(postLoginRedirect) {
		postLoginRedirect = "/"
	}

	if oauthError := strings.TrimSpace(c.QueryParam("error")); oauthError != "" {
		s.sessions.Clear(c)
		return c.Redirect(http.StatusFound, postLoginRedirect+"?auth_error="+escapeAuthError(oauthError))
	}
	if !s.Enabled() {
		s.sessions.Clear(c)
		return c.Redirect(http.StatusFound, postLoginRedirect+"?auth_error=linuxdo_not_configured")
	}

	code := strings.TrimSpace(c.QueryParam("code"))
	state := strings.TrimSpace(c.QueryParam("state"))
	if code == "" || state == "" || state != expectedState {
		s.sessions.Clear(c)
		return c.Redirect(http.StatusFound, postLoginRedirect+"?auth_error=invalid_oauth_state")
	}

	accessToken, err := s.exchangeLinuxDOCode(c.Request().Context(), c, code)
	if err == nil {
		var user *LinuxDOUser
		user, err = s.fetchLinuxDOUser(c.Request().Context(), accessToken)
		if err == nil {
			err = s.validateLinuxDOUser(user)
			if err == nil {
				var dbUserID int64
				dbUserID, err = s.upsertUser(c.Request().Context(), *user)
				if err == nil {
					sessionState.Auth = &SessionAuth{
						Method:     "linuxdo",
						UserID:     dbUserID,
						LoggedInAt: isoformatNow(),
						User: SessionUserSnapshot{
							ID:             user.ID,
							Username:       user.Username,
							Name:           sessionUserName(user),
							TrustLevel:     user.TrustLevel,
							AvatarTemplate: user.AvatarTemplate,
							IsAdmin:        s.isAdminIdentity(fmt.Sprint(user.ID), user.Username),
						},
					}
					if err = s.sessions.Write(c, sessionState); err == nil {
						return c.Redirect(http.StatusFound, s.postLoginRedirectURL(postLoginRedirect))
					}
				}
			}
		}
	}

	s.sessions.Clear(c)
	return c.Redirect(http.StatusFound, postLoginRedirect+"?auth_error=linuxdo_login_failed")
}

func (s *Service) getAuthStatus(c echo.Context) error {
	sessionState := s.sessions.Read(c)
	auth := sessionState.Auth

	var userPayload map[string]any
	if auth != nil {
		ctx, err := s.getRequestContext(c)
		if err == nil {
			userPayload = map[string]any{
				"id":             ctx.User.ID,
				"username":       ctx.User.Username,
				"name":           ctx.User.Name,
				"trust_level":    ctx.User.TrustLevel,
				"is_admin":       ctx.User.IsAdmin,
				"is_banned":      ctx.IsBanned,
				"ban_reason":     banReason(ctx.Ban),
				"ban_expires_at": banExpiresAt(ctx.Ban),
			}
		} else {
			auth = nil
			s.sessions.Clear(c)
		}
	}

	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, map[string]any{
		"authenticated": auth != nil,
		"method":        authMethod(auth),
		"user":          userPayload,
		"oauth": map[string]any{
			"linuxdo_enabled":   s.Enabled(),
			"linuxdo_login_url": loginURL(s.Enabled()),
		},
	})
}

func (s *Service) logout(c echo.Context) error {
	s.sessions.Clear(c)
	return c.NoContent(http.StatusNoContent)
}

func isSafeRelativeRedirect(value string) bool {
	return strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "//")
}

func randomState() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	buffer := make([]byte, 43)
	for index := range buffer {
		buffer[index] = alphabet[rand.IntN(len(alphabet))]
	}
	return string(buffer)
}

func escapeAuthError(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func sessionUserName(user *LinuxDOUser) string {
	if user == nil || user.Name == nil || strings.TrimSpace(*user.Name) == "" {
		return user.Username
	}
	return strings.TrimSpace(*user.Name)
}

func (s *Service) postLoginRedirectURL(postLoginRedirect string) string {
	if s.cfg.Server.ProviderBaseURL != "" {
		return s.cfg.Server.ProviderBaseURL + postLoginRedirect + "?auth=linuxdo"
	}
	return postLoginRedirect + "?auth=linuxdo"
}

func authMethod(auth *SessionAuth) any {
	if auth == nil {
		return nil
	}
	return auth.Method
}

func loginURL(enabled bool) any {
	if !enabled {
		return nil
	}
	return "/auth/linuxdo/login"
}

func banReason(ban *BanPayload) any {
	if ban == nil {
		return nil
	}
	return ban.Reason
}

func banExpiresAt(ban *BanPayload) any {
	if ban == nil {
		return nil
	}
	return ban.ExpiresAt
}
