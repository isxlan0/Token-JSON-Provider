package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromPathUsesDefaultsAndAliasFallbacks(t *testing.T) {
	unsetKnownEnvKeys(t)

	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	envBody := "" +
		"TOKEN_INDEX_SESSION_SECRET=file-secret # comment\n" +
		"TOKEN_INDEX_LINUXDO_CLIENT_ID=file-client # comment\n" +
		"TOKEN_INDEX_LINUXDO_CLIENT_SECRET=file-client-secret # comment\n" +
		"TOKEN_INDEX_LINUXDO_REDIRECT_URI=http://127.0.0.1:8000/auth/linuxdo/callback # comment\n" +
		"TOKEN_INDEX_LINUXDO_SCOPE=\n" +
		"TOKEN_INDEX_LINUXDO_MIN_TRUST_LEVEL=-4\n" +
		"TOKEN_INDEX_LINUXDO_ALLOWED_IDS= 12, 34 \n" +
		"TOKEN_DB_PATH=\n" +
		"TOKEN_DB_MAX_OPEN_CONNS=11\n" +
		"TOKEN_DB_MAX_IDLE_CONNS=7\n" +
		"TOKEN_FILES_DIR=data/token-files\n" +
		"TOKEN_INDEX_ADMIN_IDS=123,@RootUser\n" +
		"TOKEN_CACHE_BACKEND=REDIS\n" +
		"TOKEN_CACHE_DEFAULT_TTL_SEC=0\n" +
		"TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE=bad-value\n" +
		"TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE=-3\n" +
		"TOKEN_UPLOAD_MAX_FILE_SIZE_BYTES=16\n" +
		"TOKEN_QUEUE_ENABLED=false\n" +
		"PORT=not-a-number\n"

	if err := os.WriteFile(envPath, []byte(envBody), 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	cfg, err := loadFromPath(envPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Session.Secret != "file-secret" {
		t.Fatalf("unexpected session secret: %q", cfg.Session.Secret)
	}
	if cfg.LinuxDO.ClientID != "file-client" {
		t.Fatalf("unexpected client id: %q", cfg.LinuxDO.ClientID)
	}
	if cfg.LinuxDO.ClientSecret != "file-client-secret" {
		t.Fatalf("unexpected client secret: %q", cfg.LinuxDO.ClientSecret)
	}
	if cfg.LinuxDO.RedirectURI != "http://127.0.0.1:8000/auth/linuxdo/callback" {
		t.Fatalf("unexpected redirect uri: %q", cfg.LinuxDO.RedirectURI)
	}
	if cfg.LinuxDO.Scope != "read" {
		t.Fatalf("unexpected scope: %q", cfg.LinuxDO.Scope)
	}
	if cfg.LinuxDO.MinTrustLevel != 0 {
		t.Fatalf("unexpected min trust level: %d", cfg.LinuxDO.MinTrustLevel)
	}
	if cfg.Database.Path != filepath.Join(dir, "token_atlas.db") {
		t.Fatalf("unexpected database path: %q", cfg.Database.Path)
	}
	if cfg.Database.MaxOpenConns != 11 {
		t.Fatalf("unexpected database max open conns: %d", cfg.Database.MaxOpenConns)
	}
	if cfg.Database.MaxIdleConns != 7 {
		t.Fatalf("unexpected database max idle conns: %d", cfg.Database.MaxIdleConns)
	}
	if cfg.Files.TokenDir != filepath.Join(dir, "data", "token-files") {
		t.Fatalf("unexpected token files dir: %q", cfg.Files.TokenDir)
	}
	if cfg.Cache.Backend != "redis" {
		t.Fatalf("unexpected cache backend: %q", cfg.Cache.Backend)
	}
	if cfg.Cache.DefaultTTL != 1 {
		t.Fatalf("unexpected cache default ttl: %d", cfg.Cache.DefaultTTL)
	}
	if cfg.Inventory.NonHealthyMaxClaimsScope != defaultNonHealthyScope {
		t.Fatalf("unexpected non healthy scope: %q", cfg.Inventory.NonHealthyMaxClaimsScope)
	}
	if cfg.APIKeys.RatePerMinute != 0 {
		t.Fatalf("unexpected api key rate limit: %d", cfg.APIKeys.RatePerMinute)
	}
	if cfg.Upload.MaxFileSizeBytes != 1024 {
		t.Fatalf("unexpected upload max file size: %d", cfg.Upload.MaxFileSizeBytes)
	}
	if cfg.Server.QueueEnabled {
		t.Fatalf("expected queue to be disabled from env file")
	}
	if cfg.Server.Port != 8000 {
		t.Fatalf("unexpected port: %d", cfg.Server.Port)
	}
	if _, ok := cfg.LinuxDO.AllowedIDs["12"]; !ok {
		t.Fatalf("missing allowed id 12")
	}
	if _, ok := cfg.APIKeys.AdminIdentities.IDs["123"]; !ok {
		t.Fatalf("missing admin id 123")
	}
	if _, ok := cfg.APIKeys.AdminIdentities.Usernames["rootuser"]; !ok {
		t.Fatalf("missing admin username rootuser")
	}
}

func TestLoadFromPathPrefersProcessEnv(t *testing.T) {
	unsetKnownEnvKeys(t)

	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	envBody := "" +
		"PORT=8000\n" +
		"TOKEN_INDEX_ADMIN_IDENTITIES=456,@file-user\n" +
		"TOKEN_FILES_DIR=file-token-dir\n" +
		"TOKEN_CACHE_BACKEND=memory\n" +
		"TOKEN_DB_MAX_OPEN_CONNS=6\n"

	if err := os.WriteFile(envPath, []byte(envBody), 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	t.Setenv(envPort, "9001")
	t.Setenv(envAdminIdentities, "@process-user")
	t.Setenv(envCacheBackend, "AUTO")
	t.Setenv(envTokenFilesDir, filepath.Join(dir, "process-token-dir"))
	t.Setenv(envDBMaxIdleConns, "5")
	t.Setenv(envQueueEnabled, "true")

	cfg, err := loadFromPath(envPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Server.Port != 9001 {
		t.Fatalf("unexpected port: %d", cfg.Server.Port)
	}
	if cfg.Cache.Backend != "auto" {
		t.Fatalf("unexpected cache backend: %q", cfg.Cache.Backend)
	}
	if cfg.Files.TokenDir != filepath.Join(dir, "process-token-dir") {
		t.Fatalf("unexpected token files dir: %q", cfg.Files.TokenDir)
	}
	if cfg.Database.MaxOpenConns != 6 {
		t.Fatalf("unexpected database max open conns: %d", cfg.Database.MaxOpenConns)
	}
	if cfg.Database.MaxIdleConns != 5 {
		t.Fatalf("unexpected database max idle conns: %d", cfg.Database.MaxIdleConns)
	}
	if !cfg.Server.QueueEnabled {
		t.Fatalf("expected process env to enable queue")
	}
	if len(cfg.APIKeys.AdminIdentities.IDs) != 0 {
		t.Fatalf("expected process env to replace file admin identities")
	}
	if _, ok := cfg.APIKeys.AdminIdentities.Usernames["process-user"]; !ok {
		t.Fatalf("missing process env admin username")
	}
}

func TestLoadFromPathUsesOneHourCacheDefaults(t *testing.T) {
	unsetKnownEnvKeys(t)

	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	if err := os.WriteFile(envPath, []byte(""), 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	cfg, err := loadFromPath(envPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Cache.DefaultTTL != 3600 {
		t.Fatalf("unexpected cache default ttl: %d", cfg.Cache.DefaultTTL)
	}
	if cfg.Cache.MeTTL != 3600 {
		t.Fatalf("unexpected me ttl: %d", cfg.Cache.MeTTL)
	}
	if cfg.Cache.ClaimsTTL != 3600 {
		t.Fatalf("unexpected claims ttl: %d", cfg.Cache.ClaimsTTL)
	}
	if cfg.Cache.AdminTTL != 3600 {
		t.Fatalf("unexpected admin ttl: %d", cfg.Cache.AdminTTL)
	}
	if cfg.Cache.DashboardTTL != 3600 {
		t.Fatalf("unexpected dashboard ttl: %d", cfg.Cache.DashboardTTL)
	}
	if cfg.Files.TokenDir != filepath.Join(dir, "token") {
		t.Fatalf("unexpected token files dir default: %q", cfg.Files.TokenDir)
	}
	if !cfg.Server.QueueEnabled {
		t.Fatalf("expected queue to be enabled by default")
	}
}

func unsetKnownEnvKeys(t *testing.T) {
	t.Helper()

	snapshot := make(map[string]*string, len(knownEnvKeys))
	for _, key := range knownEnvKeys {
		if value, ok := os.LookupEnv(key); ok {
			copied := value
			snapshot[key] = &copied
		} else {
			snapshot[key] = nil
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset env %s: %v", key, err)
		}
	}

	t.Cleanup(func() {
		for key, value := range snapshot {
			if value == nil {
				_ = os.Unsetenv(key)
				continue
			}
			_ = os.Setenv(key, *value)
		}
	})
}
