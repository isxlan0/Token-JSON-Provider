package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

const (
	envSessionSecret            = "TOKEN_INDEX_SESSION_SECRET"
	envLinuxDOClientID          = "TOKEN_INDEX_LINUXDO_CLIENT_ID"
	envLinuxDOClientSecret      = "TOKEN_INDEX_LINUXDO_CLIENT_SECRET"
	envLinuxDORedirectURI       = "TOKEN_INDEX_LINUXDO_REDIRECT_URI"
	envLinuxDOScope             = "TOKEN_INDEX_LINUXDO_SCOPE"
	envLinuxDOMinTrustLevel     = "TOKEN_INDEX_LINUXDO_MIN_TRUST_LEVEL"
	envLinuxDOAllowedIDs        = "TOKEN_INDEX_LINUXDO_ALLOWED_IDS"
	envDBPath                   = "TOKEN_DB_PATH"
	envDBMaxOpenConns           = "TOKEN_DB_MAX_OPEN_CONNS"
	envDBMaxIdleConns           = "TOKEN_DB_MAX_IDLE_CONNS"
	envTokenFilesDir            = "TOKEN_FILES_DIR"
	envAdminIdentities          = "TOKEN_INDEX_ADMIN_IDENTITIES"
	envAdminIDsAlias            = "TOKEN_INDEX_ADMIN_IDS"
	envHealthyThreshold         = "TOKEN_HEALTHY_THRESHOLD"
	envWarningThreshold         = "TOKEN_WARNING_THRESHOLD"
	envCriticalThreshold        = "TOKEN_CRITICAL_THRESHOLD"
	envHourlyLimitHealthy       = "TOKEN_HOURLY_LIMIT_HEALTHY"
	envHourlyLimitWarning       = "TOKEN_HOURLY_LIMIT_WARNING"
	envHourlyLimitCritical      = "TOKEN_HOURLY_LIMIT_CRITICAL"
	envMaxClaimsHealthy         = "TOKEN_MAX_CLAIMS_HEALTHY"
	envMaxClaimsWarning         = "TOKEN_MAX_CLAIMS_WARNING"
	envMaxClaimsCritical        = "TOKEN_MAX_CLAIMS_CRITICAL"
	envNonHealthyMaxClaimsScope = "TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE"
	envAPIKeyMaxPerUser         = "TOKEN_APIKEY_MAX_PER_USER"
	envAPIKeyRatePerMinute      = "TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE"
	envProviderBaseURL          = "TOKEN_PROVIDER_BASE_URL"
	envCacheBackend             = "TOKEN_CACHE_BACKEND"
	envRedisURL                 = "TOKEN_REDIS_URL"
	envRedisUsername            = "TOKEN_REDIS_USERNAME"
	envRedisPassword            = "TOKEN_REDIS_PASSWORD"
	envRedisPrefix              = "TOKEN_REDIS_PREFIX"
	envCacheDefaultTTL          = "TOKEN_CACHE_DEFAULT_TTL_SEC"
	envCacheMeTTL               = "TOKEN_CACHE_ME_TTL_SEC"
	envCacheClaimsTTL           = "TOKEN_CACHE_CLAIMS_TTL_SEC"
	envCacheAdminTTL            = "TOKEN_CACHE_ADMIN_TTL_SEC"
	envCacheQueueTTL            = "TOKEN_CACHE_QUEUE_TTL_SEC"
	envCacheDashboardTTL        = "TOKEN_CACHE_DASHBOARD_TTL_SEC"
	envProbeDelaySec            = "TOKEN_CODEX_PROBE_DELAY_SEC"
	envProbeTimeoutSec          = "TOKEN_CODEX_PROBE_TIMEOUT_SEC"
	envProbeReserveSec          = "TOKEN_CODEX_PROBE_RESERVE_SEC"
	envUploadMaxFiles           = "TOKEN_UPLOAD_MAX_FILES_PER_REQUEST"
	envUploadMaxFileSize        = "TOKEN_UPLOAD_MAX_FILE_SIZE_BYTES"
	envUploadMaxSuccessPerHour  = "TOKEN_UPLOAD_MAX_SUCCESS_PER_HOUR"
	envQueueEnabled             = "TOKEN_QUEUE_ENABLED"
	envLogLevel                 = "TOKEN_LOG_LEVEL"
	envClaimTrace               = "TOKEN_CLAIM_TRACE"
	envPort                     = "PORT"

	defaultNonHealthyScope = "all_unfinished"
)

var knownEnvKeys = []string{
	envSessionSecret,
	envLinuxDOClientID,
	envLinuxDOClientSecret,
	envLinuxDORedirectURI,
	envLinuxDOScope,
	envLinuxDOMinTrustLevel,
	envLinuxDOAllowedIDs,
	envDBPath,
	envDBMaxOpenConns,
	envDBMaxIdleConns,
	envTokenFilesDir,
	envAdminIdentities,
	envAdminIDsAlias,
	envHealthyThreshold,
	envWarningThreshold,
	envCriticalThreshold,
	envHourlyLimitHealthy,
	envHourlyLimitWarning,
	envHourlyLimitCritical,
	envMaxClaimsHealthy,
	envMaxClaimsWarning,
	envMaxClaimsCritical,
	envNonHealthyMaxClaimsScope,
	envAPIKeyMaxPerUser,
	envAPIKeyRatePerMinute,
	envProviderBaseURL,
	envCacheBackend,
	envRedisURL,
	envRedisUsername,
	envRedisPassword,
	envRedisPrefix,
	envCacheDefaultTTL,
	envCacheMeTTL,
	envCacheClaimsTTL,
	envCacheAdminTTL,
	envCacheQueueTTL,
	envCacheDashboardTTL,
	envProbeDelaySec,
	envProbeTimeoutSec,
	envProbeReserveSec,
	envUploadMaxFiles,
	envUploadMaxFileSize,
	envUploadMaxSuccessPerHour,
	envQueueEnabled,
	envLogLevel,
	envClaimTrace,
	envPort,
}

type Config struct {
	Session   SessionConfig
	LinuxDO   LinuxDOConfig
	Database  DatabaseConfig
	Files     FilesConfig
	Cache     CacheConfig
	Inventory InventoryConfig
	APIKeys   APIKeyConfig
	Probe     ProbeConfig
	Upload    UploadConfig
	Server    ServerConfig
	Logging   LoggingConfig
}

type SessionConfig struct {
	Secret string
}

type LinuxDOConfig struct {
	ClientID      string
	ClientSecret  string
	RedirectURI   string
	Scope         string
	MinTrustLevel int
	AllowedIDs    map[string]struct{}
}

type DatabaseConfig struct {
	Path         string
	MaxOpenConns int
	MaxIdleConns int
}

type FilesConfig struct {
	TokenDir string
}

type CacheConfig struct {
	Backend       string
	RedisURL      string
	RedisUsername string
	RedisPassword string
	RedisPrefix   string
	DefaultTTL    int
	MeTTL         int
	ClaimsTTL     int
	AdminTTL      int
	QueueTTL      int
	DashboardTTL  int
}

type InventoryConfig struct {
	Thresholds               ThresholdConfig
	Limits                   InventoryLimitConfig
	NonHealthyMaxClaimsScope string
}

type ThresholdConfig struct {
	Healthy  int
	Warning  int
	Critical int
}

type InventoryLimitConfig struct {
	Healthy  InventoryStatusLimit
	Warning  InventoryStatusLimit
	Critical InventoryStatusLimit
}

type InventoryStatusLimit struct {
	Hourly    int
	MaxClaims int
}

type APIKeyConfig struct {
	MaxPerUser      int
	RatePerMinute   int
	AdminIdentities IdentitySet
}

type IdentitySet struct {
	IDs       map[string]struct{}
	Usernames map[string]struct{}
}

type ProbeConfig struct {
	DelaySec   float64
	TimeoutSec float64
	ReserveSec int
}

type UploadConfig struct {
	MaxFilesPerRequest int
	MaxFileSizeBytes   int
	MaxSuccessPerHour  int
}

type ServerConfig struct {
	ProviderBaseURL string
	QueueEnabled    bool
	Port            int
}

type LoggingConfig struct {
	Level      string
	ClaimTrace bool
}

type envSource struct {
	fileValues map[string]string
}

func Load() (Config, error) {
	return loadFromPath(".env")
}

func loadFromPath(path string) (Config, error) {
	fileValues, err := godotenv.Read(path)
	if err != nil {
		return Config{}, fmt.Errorf("read env file %q: %w", path, err)
	}

	baseDir := filepath.Dir(path)
	if baseDir == "." {
		baseDir, err = os.Getwd()
		if err != nil {
			return Config{}, fmt.Errorf("resolve working directory: %w", err)
		}
	}

	source := envSource{fileValues: fileValues}

	cfg := Config{
		Session: SessionConfig{
			Secret: source.rawTrimmed(envSessionSecret, "change-me-session-secret"),
		},
		LinuxDO: LinuxDOConfig{
			ClientID:      source.rawTrimmed(envLinuxDOClientID, ""),
			ClientSecret:  source.rawTrimmed(envLinuxDOClientSecret, ""),
			RedirectURI:   source.rawTrimmed(envLinuxDORedirectURI, ""),
			Scope:         source.nonEmptyRawTrimmed(envLinuxDOScope, "read"),
			MinTrustLevel: maxInt(0, source.intFromRaw(envLinuxDOMinTrustLevel, 0)),
			AllowedIDs:    splitToSet(source.rawTrimmed(envLinuxDOAllowedIDs, "")),
		},
		Database: DatabaseConfig{
			Path:         source.databasePath(baseDir),
			MaxOpenConns: maxInt(1, source.cleanedInt(envDBMaxOpenConns, 8)),
			MaxIdleConns: maxInt(1, source.cleanedInt(envDBMaxIdleConns, 8)),
		},
		Files: FilesConfig{
			TokenDir: source.tokenFilesDir(baseDir),
		},
		Cache: CacheConfig{
			Backend:       strings.ToLower(source.cleanedString(envCacheBackend, "auto")),
			RedisURL:      source.cleanedString(envRedisURL, ""),
			RedisUsername: source.cleanedString(envRedisUsername, ""),
			RedisPassword: source.cleanedString(envRedisPassword, ""),
			RedisPrefix:   source.cleanedString(envRedisPrefix, "token_index:"),
			DefaultTTL:    maxInt(1, source.cleanedInt(envCacheDefaultTTL, 3600)),
			MeTTL:         maxInt(1, source.cleanedInt(envCacheMeTTL, 3600)),
			ClaimsTTL:     maxInt(1, source.cleanedInt(envCacheClaimsTTL, 3600)),
			AdminTTL:      maxInt(1, source.cleanedInt(envCacheAdminTTL, 3600)),
			QueueTTL:      maxInt(1, source.cleanedInt(envCacheQueueTTL, 5)),
			DashboardTTL:  maxInt(1, source.cleanedInt(envCacheDashboardTTL, 3600)),
		},
		Inventory: InventoryConfig{
			Thresholds: ThresholdConfig{
				Healthy:  maxInt(1, source.cleanedInt(envHealthyThreshold, 1000)),
				Warning:  maxInt(1, source.cleanedInt(envWarningThreshold, 500)),
				Critical: maxInt(1, source.cleanedInt(envCriticalThreshold, 100)),
			},
			Limits: InventoryLimitConfig{
				Healthy: InventoryStatusLimit{
					Hourly:    maxInt(1, source.cleanedInt(envHourlyLimitHealthy, 30)),
					MaxClaims: maxInt(1, source.cleanedInt(envMaxClaimsHealthy, 1)),
				},
				Warning: InventoryStatusLimit{
					Hourly:    maxInt(1, source.cleanedInt(envHourlyLimitWarning, 20)),
					MaxClaims: maxInt(1, source.cleanedInt(envMaxClaimsWarning, 2)),
				},
				Critical: InventoryStatusLimit{
					Hourly:    maxInt(1, source.cleanedInt(envHourlyLimitCritical, 15)),
					MaxClaims: maxInt(1, source.cleanedInt(envMaxClaimsCritical, 3)),
				},
			},
			NonHealthyMaxClaimsScope: normalizeNonHealthyScope(source.cleanedString(envNonHealthyMaxClaimsScope, defaultNonHealthyScope)),
		},
		APIKeys: APIKeyConfig{
			MaxPerUser:      maxInt(1, source.cleanedInt(envAPIKeyMaxPerUser, 5)),
			RatePerMinute:   maxInt(0, source.cleanedInt(envAPIKeyRatePerMinute, 60)),
			AdminIdentities: parseAdminIdentities(source.adminIdentityRaw()),
		},
		Probe: ProbeConfig{
			DelaySec:   maxFloat(0, source.cleanedFloat(envProbeDelaySec, 0.1)),
			TimeoutSec: maxFloat(1, source.cleanedFloat(envProbeTimeoutSec, 20.0)),
			ReserveSec: maxInt(5, int(source.cleanedFloat(envProbeReserveSec, 30.0))),
		},
		Upload: UploadConfig{
			MaxFilesPerRequest: maxInt(1, source.cleanedInt(envUploadMaxFiles, 10)),
			MaxFileSizeBytes:   maxInt(1024, source.cleanedInt(envUploadMaxFileSize, 10*1024)),
			MaxSuccessPerHour:  maxInt(1, source.cleanedInt(envUploadMaxSuccessPerHour, 20)),
		},
		Server: ServerConfig{
			ProviderBaseURL: strings.TrimRight(source.rawTrimmed(envProviderBaseURL, ""), "/"),
			QueueEnabled:    source.cleanedBool(envQueueEnabled, true),
			Port:            source.cleanedInt(envPort, 8000),
		},
		Logging: LoggingConfig{
			Level:      normalizeLogLevel(source.cleanedString(envLogLevel, "info")),
			ClaimTrace: source.cleanedBool(envClaimTrace, false),
		},
	}

	return cfg, nil
}

func normalizeLogLevel(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug", "info", "warn", "error":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "info"
	}
}

func (s envSource) lookup(name string) (string, bool) {
	if value, ok := os.LookupEnv(name); ok {
		return value, true
	}
	value, ok := s.fileValues[name]
	return value, ok
}

func (s envSource) rawTrimmed(name string, defaultValue string) string {
	value, ok := s.lookup(name)
	if !ok {
		return defaultValue
	}
	cleaned := strings.TrimSpace(strings.SplitN(value, "#", 2)[0])
	if cleaned == "" {
		return defaultValue
	}
	return cleaned
}

func (s envSource) nonEmptyRawTrimmed(name string, defaultValue string) string {
	value := s.rawTrimmed(name, "")
	if value == "" {
		return defaultValue
	}
	return value
}

func (s envSource) cleanedString(name string, defaultValue string) string {
	value, ok := s.lookup(name)
	if !ok {
		return defaultValue
	}
	cleaned := strings.TrimSpace(strings.SplitN(value, "#", 2)[0])
	if cleaned == "" {
		return defaultValue
	}
	return cleaned
}

func (s envSource) cleanedInt(name string, defaultValue int) int {
	value := s.cleanedString(name, "")
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func (s envSource) intFromRaw(name string, defaultValue int) int {
	value := s.rawTrimmed(name, "")
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func (s envSource) cleanedFloat(name string, defaultValue float64) float64 {
	value := s.cleanedString(name, "")
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func (s envSource) cleanedBool(name string, defaultValue bool) bool {
	value := strings.ToLower(s.cleanedString(name, ""))
	if value == "" {
		return defaultValue
	}
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}

func (s envSource) databasePath(baseDir string) string {
	value := s.rawTrimmed(envDBPath, "")
	if value == "" {
		return filepath.Join(baseDir, "token_atlas.db")
	}
	return value
}

func (s envSource) tokenFilesDir(baseDir string) string {
	value := s.rawTrimmed(envTokenFilesDir, "")
	if value == "" {
		return filepath.Join(baseDir, "token")
	}
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Clean(filepath.Join(baseDir, value))
}

func (s envSource) adminIdentityRaw() string {
	if value := s.cleanedString(envAdminIdentities, ""); value != "" {
		return value
	}
	return s.cleanedString(envAdminIDsAlias, "")
}

func parseAdminIdentities(raw string) IdentitySet {
	identities := IdentitySet{
		IDs:       map[string]struct{}{},
		Usernames: map[string]struct{}{},
	}

	for _, part := range strings.Split(raw, ",") {
		candidate := strings.TrimSpace(part)
		if candidate == "" {
			continue
		}
		if strings.HasPrefix(candidate, "@") || !isDigits(candidate) {
			normalized := normalizeUsername(candidate)
			if normalized != "" {
				identities.Usernames[normalized] = struct{}{}
			}
			continue
		}
		identities.IDs[candidate] = struct{}{}
	}

	return identities
}

func splitToSet(raw string) map[string]struct{} {
	values := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		candidate := strings.TrimSpace(part)
		if candidate == "" {
			continue
		}
		values[candidate] = struct{}{}
	}
	return values
}

func normalizeUsername(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "@")
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeNonHealthyScope(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "all_unfinished", "new_only", "unclaimed_only":
		return value
	default:
		return defaultNonHealthyScope
	}
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func maxInt(minimum int, value int) int {
	if value < minimum {
		return minimum
	}
	return value
}

func maxFloat(minimum float64, value float64) float64 {
	if value < minimum {
		return minimum
	}
	return value
}
