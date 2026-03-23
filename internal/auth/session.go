package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

const (
	sessionCookieName         = "token_atlas_session"
	sessionDuration           = 7 * 24 * time.Hour
	sessionStateKeyAuth       = "auth"
	sessionStateKeyOAuthState = "linuxdo_oauth_state"
	sessionStateKeyPostLogin  = "post_login_redirect"
)

type SessionManager struct {
	secret []byte
}

type SessionState struct {
	Auth              *SessionAuth `json:"auth,omitempty"`
	OAuthState        string       `json:"linuxdo_oauth_state,omitempty"`
	PostLoginRedirect string       `json:"post_login_redirect,omitempty"`
}

type SessionAuth struct {
	SessionID  string              `json:"session_id,omitempty"`
	Method     string              `json:"method"`
	UserID     int64               `json:"user_id"`
	LoggedInAt string              `json:"logged_in_at"`
	User       SessionUserSnapshot `json:"user"`
}

type SessionUserSnapshot struct {
	ID             int64   `json:"id"`
	Username       string  `json:"username"`
	Name           string  `json:"name"`
	TrustLevel     int64   `json:"trust_level"`
	AvatarTemplate *string `json:"avatar_template,omitempty"`
	IsAdmin        bool    `json:"is_admin"`
}

func NewSessionManager(secret string) *SessionManager {
	return &SessionManager{secret: []byte(secret)}
}

func (m *SessionManager) Read(c echo.Context) SessionState {
	cookie, err := c.Cookie(sessionCookieName)
	if err != nil || cookie == nil || cookie.Value == "" {
		return SessionState{}
	}

	state, err := m.decode(cookie.Value)
	if err != nil {
		return SessionState{}
	}
	return state
}

func (m *SessionManager) Write(c echo.Context, state SessionState) error {
	encoded, err := m.encode(state)
	if err != nil {
		return fmt.Errorf("encode session state: %w", err)
	}

	c.SetCookie(&http.Cookie{
		Name:     sessionCookieName,
		Value:    encoded,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionDuration.Seconds()),
		Expires:  time.Now().Add(sessionDuration),
	})
	return nil
}

func (m *SessionManager) Clear(c echo.Context) {
	c.SetCookie(&http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
}

func (m *SessionManager) encode(state SessionState) (string, error) {
	payload, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	payloadPart := base64.RawURLEncoding.EncodeToString(payload)
	signaturePart := base64.RawURLEncoding.EncodeToString(m.sign(payload))
	return payloadPart + "." + signaturePart, nil
}

func (m *SessionManager) decode(value string) (SessionState, error) {
	var state SessionState

	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return state, fmt.Errorf("invalid session value")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return state, fmt.Errorf("decode session payload: %w", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return state, fmt.Errorf("decode session signature: %w", err)
	}
	expected := m.sign(payload)
	if subtle.ConstantTimeCompare(signature, expected) != 1 {
		return state, fmt.Errorf("invalid session signature")
	}
	if err := json.Unmarshal(payload, &state); err != nil {
		return state, fmt.Errorf("decode session json: %w", err)
	}
	return state, nil
}

func (m *SessionManager) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, m.secret)
	mac.Write(payload)
	return mac.Sum(nil)
}
