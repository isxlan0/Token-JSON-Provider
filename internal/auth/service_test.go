package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/config"
	"token-atlas/internal/database"
	"token-atlas/internal/httpapi"
)

func TestAuthStatusUnauthenticated(t *testing.T) {
	service := newTestService(t, testConfig(true), nil)
	e := newAuthTestEcho(service)

	request := httptest.NewRequest(http.MethodGet, "/auth/status", nil)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}

	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload["authenticated"] != false {
		t.Fatalf("expected unauthenticated payload, got %#v", payload["authenticated"])
	}
	oauthPayload := payload["oauth"].(map[string]any)
	if oauthPayload["linuxdo_enabled"] != true {
		t.Fatalf("expected linuxdo enabled")
	}
	if oauthPayload["linuxdo_login_url"] != "/auth/linuxdo/login" {
		t.Fatalf("unexpected login url: %#v", oauthPayload["linuxdo_login_url"])
	}
}

func TestStartLinuxDOLoginReturns503WhenDisabled(t *testing.T) {
	cfg := testConfig(false)
	cfg.LinuxDO.ClientID = ""
	service := newTestService(t, cfg, nil)
	e := newAuthTestEcho(service)

	request := httptest.NewRequest(http.MethodGet, "/auth/linuxdo/login", nil)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"detail":"Linux.do OAuth is not configured."`) {
		t.Fatalf("unexpected body: %s", recorder.Body.String())
	}
}

func TestLinuxDOCallbackSuccessSetsSession(t *testing.T) {
	service := newTestService(t, testConfig(true), fakeOAuthTransport())
	e := newAuthTestEcho(service)

	cookieValue := sessionCookieValue(t, service, SessionState{
		OAuthState:        "expected-state",
		PostLoginRedirect: "/admin",
	})

	request := httptest.NewRequest(http.MethodGet, "/auth/linuxdo/callback?code=demo-code&state=expected-state", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: cookieValue})
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusFound {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	if got := recorder.Header().Get("Location"); got != "/admin?auth=linuxdo" {
		t.Fatalf("unexpected redirect location: %s", got)
	}

	authCookie := responseCookie(t, recorder, sessionCookieName)
	statusRequest := httptest.NewRequest(http.MethodGet, "/auth/status", nil)
	statusRequest.AddCookie(authCookie)
	statusRecorder := httptest.NewRecorder()
	e.ServeHTTP(statusRecorder, statusRequest)

	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected status response code: %d", statusRecorder.Code)
	}
	if !strings.Contains(statusRecorder.Body.String(), `"authenticated":true`) {
		t.Fatalf("unexpected status body: %s", statusRecorder.Body.String())
	}
	if !strings.Contains(statusRecorder.Body.String(), `"username":"demo-user"`) {
		t.Fatalf("unexpected status body: %s", statusRecorder.Body.String())
	}
}

func TestLinuxDOCallbackRejectsInvalidState(t *testing.T) {
	service := newTestService(t, testConfig(true), fakeOAuthTransport())
	e := newAuthTestEcho(service)

	cookieValue := sessionCookieValue(t, service, SessionState{
		OAuthState:        "expected-state",
		PostLoginRedirect: "/",
	})

	request := httptest.NewRequest(http.MethodGet, "/auth/linuxdo/callback?code=demo-code&state=other-state", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: cookieValue})
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusFound {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	if got := recorder.Header().Get("Location"); got != "/?auth_error=invalid_oauth_state" {
		t.Fatalf("unexpected redirect location: %s", got)
	}
	authCookie := responseCookie(t, recorder, sessionCookieName)
	if authCookie.MaxAge != -1 {
		t.Fatalf("expected cleared auth cookie, got max-age=%d", authCookie.MaxAge)
	}
}

func TestRequireAPIKeyMiddleware(t *testing.T) {
	service := newTestService(t, testConfig(true), nil)
	seedAPIKey(t, service, "plain-api-key")

	e := echo.New()
	httpapi.InstallErrorHandler(e)
	e.GET("/protected", func(c echo.Context) error {
		record, ok := APIKeyRecordFromEcho(c)
		if !ok {
			t.Fatalf("missing api key record in context")
		}
		return c.JSON(http.StatusOK, record)
	}, service.RequireAPIKeyMiddleware)

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set(apiKeyHeader, "plain-api-key")
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"user_id":1`) {
		t.Fatalf("unexpected body: %s", recorder.Body.String())
	}
}

func TestAPIKeyCRUDRoutes(t *testing.T) {
	service := newTestService(t, testConfig(true), nil)
	seedUser(t, service, 1, "1001", "demo-user", "Demo User", 2)
	e := newAuthTestEcho(service)

	authCookie := sessionAuthCookie(t, service, 1, 1001, "demo-user", "Demo User", false)

	createRequest := httptest.NewRequest(http.MethodPost, "/me/api-keys", strings.NewReader(`{"name":"primary"}`))
	createRequest.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	createRequest.AddCookie(authCookie)
	createRecorder := httptest.NewRecorder()
	e.ServeHTTP(createRecorder, createRequest)

	if createRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected create status: %d body=%s", createRecorder.Code, createRecorder.Body.String())
	}
	if !strings.Contains(createRecorder.Body.String(), `"name":"primary"`) {
		t.Fatalf("unexpected create body: %s", createRecorder.Body.String())
	}
	if !strings.Contains(createRecorder.Body.String(), `"token":"tk_`) {
		t.Fatalf("expected token in create response: %s", createRecorder.Body.String())
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/me/api-keys", nil)
	listRequest.AddCookie(authCookie)
	listRecorder := httptest.NewRecorder()
	e.ServeHTTP(listRecorder, listRequest)

	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	if !strings.Contains(listRecorder.Body.String(), `"active":1`) {
		t.Fatalf("unexpected list body: %s", listRecorder.Body.String())
	}

	revokeRequest := httptest.NewRequest(http.MethodPost, "/me/api-keys/1/revoke", nil)
	revokeRequest.AddCookie(authCookie)
	revokeRecorder := httptest.NewRecorder()
	e.ServeHTTP(revokeRecorder, revokeRequest)

	if revokeRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected revoke status: %d body=%s", revokeRecorder.Code, revokeRecorder.Body.String())
	}
	if !strings.Contains(revokeRecorder.Body.String(), `"status":"revoked"`) {
		t.Fatalf("unexpected revoke body: %s", revokeRecorder.Body.String())
	}
}

func newAuthTestEcho(service *Service) *echo.Echo {
	e := echo.New()
	httpapi.InstallErrorHandler(e)
	service.RegisterRoutes(e)
	return e
}

func newTestService(t *testing.T, cfg config.Config, transport http.RoundTripper) *Service {
	t.Helper()

	store, err := database.Open(filepath.Join(t.TempDir(), "token_atlas.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init database: %v", err)
	}

	client := &http.Client{Transport: transport}
	if transport == nil {
		client = nil
	}
	return NewService(cfg, store, client, nil)
}

func testConfig(enabled bool) config.Config {
	cfg := config.Config{
		Session: config.SessionConfig{Secret: "test-session-secret"},
		LinuxDO: config.LinuxDOConfig{
			ClientID:      "client-id",
			ClientSecret:  "client-secret",
			RedirectURI:   "",
			Scope:         "read",
			MinTrustLevel: 1,
			AllowedIDs:    map[string]struct{}{},
		},
		APIKeys: config.APIKeyConfig{
			MaxPerUser:    5,
			RatePerMinute: 60,
			AdminIdentities: config.IdentitySet{
				IDs:       map[string]struct{}{"1001": {}},
				Usernames: map[string]struct{}{},
			},
		},
		Server: config.ServerConfig{},
	}
	if !enabled {
		cfg.LinuxDO.ClientID = ""
		cfg.LinuxDO.ClientSecret = ""
	}
	return cfg
}

func fakeOAuthTransport() http.RoundTripper {
	return roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.String() {
		case linuxDOTokenURL:
			return jsonResponse(http.StatusOK, `{"access_token":"oauth-access-token"}`), nil
		case linuxDOUserURL:
			return jsonResponse(http.StatusOK, `{"id":1001,"username":"demo-user","name":"Demo User","active":true,"trust_level":2,"silenced":false}`), nil
		default:
			return jsonResponse(http.StatusNotFound, `{"detail":"not found"}`), nil
		}
	})
}

func jsonResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func sessionCookieValue(t *testing.T, service *Service, state SessionState) string {
	t.Helper()

	value, err := service.sessions.encode(state)
	if err != nil {
		t.Fatalf("encode session: %v", err)
	}
	return value
}

func responseCookie(t *testing.T, recorder *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()

	response := recorder.Result()
	for _, cookie := range response.Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	t.Fatalf("missing cookie %s", name)
	return nil
}

func seedAPIKey(t *testing.T, service *Service, plainAPIKey string) {
	t.Helper()

	seedUser(t, service, 1, "1001", "demo-user", "Demo User", 2)
	if _, err := service.store.DB().Exec(`
		INSERT INTO api_keys (id, user_id, name, key_hash, key_prefix, key_value, status, created_at_ts)
		VALUES (1, 1, 'primary', ?, 'tk_abc', ?, 'active', 1000)
	`, hashAPIKey(plainAPIKey), plainAPIKey); err != nil {
		t.Fatalf("insert api key: %v", err)
	}
}

func seedUser(t *testing.T, service *Service, id int64, linuxDOUserID string, username string, name string, trustLevel int64) {
	t.Helper()

	if _, err := service.store.DB().Exec(`
		INSERT INTO users (id, linuxdo_user_id, linuxdo_username, linuxdo_name, trust_level, created_at_ts, last_login_at_ts)
		VALUES (?, ?, ?, ?, ?, 1000, 1000)
	`, id, linuxDOUserID, username, name, trustLevel); err != nil {
		t.Fatalf("insert user: %v", err)
	}
}

func sessionAuthCookie(t *testing.T, service *Service, dbUserID int64, linuxDOID int64, username string, name string, isAdmin bool) *http.Cookie {
	t.Helper()

	value := sessionCookieValue(t, service, SessionState{
		Auth: &SessionAuth{
			Method:     "linuxdo",
			UserID:     dbUserID,
			LoggedInAt: isoformatNow(),
			User: SessionUserSnapshot{
				ID:         linuxDOID,
				Username:   username,
				Name:       name,
				TrustLevel: 2,
				IsAdmin:    isAdmin,
			},
		},
	})
	return &http.Cookie{Name: sessionCookieName, Value: value}
}

type roundTripFunc func(request *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
