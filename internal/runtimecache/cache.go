package runtimecache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"token-atlas/internal/config"
)

const (
	localFrontCacheTTL       = 3 * time.Second
	redisOperationTimeout    = 100 * time.Millisecond
	redisBreakerThreshold    = 5
	redisBreakerOpenDuration = 30 * time.Second
)

type backend interface {
	getText(key string) (string, bool)
	setText(key string, value string, ttl time.Duration) error
	delete(key string) error
	incr(key string) (int64, error)
}

type memoryBackend struct {
	mu      sync.RWMutex
	entries map[string]memoryEntry
}

type memoryEntry struct {
	value     string
	expiresAt time.Time
}

func newMemoryBackend() *memoryBackend {
	return &memoryBackend{
		entries: make(map[string]memoryEntry),
	}
}

func (b *memoryBackend) getText(key string) (string, bool) {
	now := time.Now()

	b.mu.RLock()
	entry, ok := b.entries[key]
	b.mu.RUnlock()
	if !ok {
		return "", false
	}
	if !entry.expiresAt.IsZero() && !entry.expiresAt.After(now) {
		b.mu.Lock()
		delete(b.entries, key)
		b.mu.Unlock()
		return "", false
	}
	return entry.value, true
}

func (b *memoryBackend) setText(key string, value string, ttl time.Duration) error {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	b.mu.Lock()
	b.entries[key] = memoryEntry{
		value:     value,
		expiresAt: expiresAt,
	}
	b.mu.Unlock()
	return nil
}

func (b *memoryBackend) delete(key string) error {
	b.mu.Lock()
	delete(b.entries, key)
	b.mu.Unlock()
	return nil
}

func (b *memoryBackend) incr(key string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	current := int64(0)
	if entry, ok := b.entries[key]; ok {
		parsed, err := strconv.ParseInt(strings.TrimSpace(entry.value), 10, 64)
		if err == nil {
			current = parsed
		}
	}
	current++
	b.entries[key] = memoryEntry{value: strconv.FormatInt(current, 10)}
	return current, nil
}

type redisBackend struct {
	prefix string
	client *redis.Client

	breakerMu           sync.Mutex
	consecutiveFailures int
	openUntil           time.Time
}

type cacheMetrics struct {
	localHits         int64
	backendHits       int64
	misses            int64
	sets              int64
	deletes           int64
	scopeVersionReads int64
	scopeBumps        int64
}

type StatsSnapshot struct {
	BackendName       string `json:"backend_name"`
	LocalHits         int64  `json:"local_hits"`
	BackendHits       int64  `json:"backend_hits"`
	Misses            int64  `json:"misses"`
	Sets              int64  `json:"sets"`
	Deletes           int64  `json:"deletes"`
	ScopeVersionReads int64  `json:"scope_version_reads"`
	ScopeBumps        int64  `json:"scope_bumps"`
}

func newRedisBackend(ctx context.Context, cfg config.CacheConfig) (*redisBackend, error) {
	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	if trimmed := strings.TrimSpace(cfg.RedisUsername); trimmed != "" {
		options.Username = trimmed
	}
	if trimmed := strings.TrimSpace(cfg.RedisPassword); trimmed != "" {
		options.Password = trimmed
	}

	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &redisBackend{
		prefix: cfg.RedisPrefix,
		client: client,
	}, nil
}

func (b *redisBackend) key(value string) string {
	return b.prefix + value
}

func (b *redisBackend) allowRequest() bool {
	b.breakerMu.Lock()
	defer b.breakerMu.Unlock()

	now := time.Now()
	if !b.openUntil.IsZero() && now.Before(b.openUntil) {
		return false
	}
	if !b.openUntil.IsZero() && !now.Before(b.openUntil) {
		b.openUntil = time.Time{}
	}
	return true
}

func (b *redisBackend) recordOutcome(err error) {
	b.breakerMu.Lock()
	defer b.breakerMu.Unlock()

	if err == nil || err == redis.Nil {
		b.consecutiveFailures = 0
		return
	}

	b.consecutiveFailures++
	if b.consecutiveFailures >= redisBreakerThreshold {
		b.openUntil = time.Now().Add(redisBreakerOpenDuration)
		b.consecutiveFailures = 0
	}
}

func (b *redisBackend) operationContext() (context.Context, context.CancelFunc, bool) {
	if !b.allowRequest() {
		return nil, nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisOperationTimeout)
	return ctx, cancel, true
}

func (b *redisBackend) getText(key string) (string, bool) {
	ctx, cancel, ok := b.operationContext()
	if !ok {
		return "", false
	}
	defer cancel()

	value, err := b.client.Get(ctx, b.key(key)).Result()
	b.recordOutcome(err)
	if err == redis.Nil {
		return "", false
	}
	if err != nil {
		return "", false
	}
	return value, true
}

func (b *redisBackend) setText(key string, value string, ttl time.Duration) error {
	ctx, cancel, ok := b.operationContext()
	if !ok {
		return nil
	}
	defer cancel()

	var err error
	if ttl <= 0 {
		err = b.client.Set(ctx, b.key(key), value, 0).Err()
		b.recordOutcome(err)
		return err
	}
	err = b.client.SetEx(ctx, b.key(key), value, ttl).Err()
	b.recordOutcome(err)
	return err
}

func (b *redisBackend) delete(key string) error {
	ctx, cancel, ok := b.operationContext()
	if !ok {
		return nil
	}
	defer cancel()

	err := b.client.Del(ctx, b.key(key)).Err()
	b.recordOutcome(err)
	return err
}

func (b *redisBackend) incr(key string) (int64, error) {
	ctx, cancel, ok := b.operationContext()
	if !ok {
		return 0, fmt.Errorf("redis circuit breaker open")
	}
	defer cancel()

	value, err := b.client.Incr(ctx, b.key(key)).Result()
	b.recordOutcome(err)
	return value, err
}

type AppCache struct {
	mu          sync.RWMutex
	backendName string
	backend     backend
	frontCache  *memoryBackend

	flightMu sync.Mutex
	flights  map[string]*cacheFlight

	metrics cacheMetrics
}

type cacheFlight struct {
	done   chan struct{}
	result any
	err    error
}

type CacheState string

const (
	CacheStateMemoryHit    CacheState = "memory_hit"
	CacheStateFlightShared CacheState = "flight_shared"
	CacheStateMiss         CacheState = "miss"
)

func New(ctx context.Context, cfg config.CacheConfig, logger *slog.Logger) *AppCache {
	if logger == nil {
		logger = slog.Default()
	}

	cache := &AppCache{
		backendName: "memory",
		backend:     newMemoryBackend(),
		flights:     make(map[string]*cacheFlight),
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Backend))
	if mode == "" {
		mode = "auto"
	}

	logMessage := "[cache] using in-memory cache backend"
	if (mode == "auto" || mode == "redis") && strings.TrimSpace(cfg.RedisURL) != "" {
		redisBackend, err := newRedisBackend(ctx, cfg)
		if err == nil {
			cache.backendName = "redis"
			cache.backend = redisBackend
			cache.frontCache = newMemoryBackend()
			logMessage = fmt.Sprintf(
				"[cache] Redis connected successfully: backend=redis url=%s prefix=%s username=%s",
				cfg.RedisURL,
				cfg.RedisPrefix,
				usernameLogValue(cfg.RedisUsername),
			)
		} else {
			logMessage = fmt.Sprintf(
				"[cache] Redis connection failed: backend=%s url=%s reason=%v. Falling back to in-memory cache.",
				mode,
				cfg.RedisURL,
				err,
			)
		}
	} else if mode == "redis" {
		logMessage = "[cache] Redis backend requested but TOKEN_REDIS_URL is empty. Falling back to in-memory cache."
	} else if mode == "memory" {
		logMessage = "[cache] using in-memory cache backend (forced by TOKEN_CACHE_BACKEND=memory)"
	}

	logger.Info(logMessage)
	return cache
}

func usernameLogValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "<empty>"
	}
	return "<set>"
}

func (c *AppCache) BackendName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.backendName
}

func (c *AppCache) StatsSnapshot() StatsSnapshot {
	c.mu.RLock()
	backendName := c.backendName
	c.mu.RUnlock()

	return StatsSnapshot{
		BackendName:       backendName,
		LocalHits:         atomic.LoadInt64(&c.metrics.localHits),
		BackendHits:       atomic.LoadInt64(&c.metrics.backendHits),
		Misses:            atomic.LoadInt64(&c.metrics.misses),
		Sets:              atomic.LoadInt64(&c.metrics.sets),
		Deletes:           atomic.LoadInt64(&c.metrics.deletes),
		ScopeVersionReads: atomic.LoadInt64(&c.metrics.scopeVersionReads),
		ScopeBumps:        atomic.LoadInt64(&c.metrics.scopeBumps),
	}
}

func (c *AppCache) shouldUseFrontCache(key string) bool {
	if c == nil || c.frontCache == nil {
		return false
	}

	trimmed := strings.TrimSpace(key)
	for _, prefix := range []string{
		"snapshot:user-profile:",
		"snapshot:user-runtime-snapshot:",
		"snapshot:user-quota:",
		"snapshot:dashboard-summary:",
	} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
}

func (c *AppCache) frontCacheTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 || ttl > localFrontCacheTTL {
		return localFrontCacheTTL
	}
	return ttl
}

func (c *AppCache) GetText(key string) (string, bool) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return "", false
	}

	c.mu.RLock()
	backend := c.backend
	frontCache := c.frontCache
	c.mu.RUnlock()

	if c.shouldUseFrontCache(trimmed) && frontCache != nil {
		if value, ok := frontCache.getText(trimmed); ok {
			atomic.AddInt64(&c.metrics.localHits, 1)
			return value, true
		}
	}

	value, ok := backend.getText(trimmed)
	if !ok {
		atomic.AddInt64(&c.metrics.misses, 1)
		return "", false
	}

	atomic.AddInt64(&c.metrics.backendHits, 1)
	if c.shouldUseFrontCache(trimmed) && frontCache != nil {
		_ = frontCache.setText(trimmed, value, localFrontCacheTTL)
	}
	return value, true
}

func (c *AppCache) SetText(key string, value string, ttlSec int) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return
	}
	c.mu.RLock()
	backend := c.backend
	frontCache := c.frontCache
	c.mu.RUnlock()

	ttl := secondsToDuration(ttlSec)
	if c.shouldUseFrontCache(trimmed) && frontCache != nil {
		_ = frontCache.setText(trimmed, value, c.frontCacheTTL(ttl))
	}
	atomic.AddInt64(&c.metrics.sets, 1)
	_ = backend.setText(trimmed, value, ttl)
}

func (c *AppCache) Delete(key string) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return
	}
	c.mu.RLock()
	backend := c.backend
	frontCache := c.frontCache
	c.mu.RUnlock()
	if frontCache != nil {
		_ = frontCache.delete(trimmed)
	}
	atomic.AddInt64(&c.metrics.deletes, 1)
	_ = backend.delete(trimmed)
}

func (c *AppCache) Incr(key string) int64 {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return 1
	}
	c.mu.RLock()
	backend := c.backend
	frontCache := c.frontCache
	c.mu.RUnlock()
	if frontCache != nil {
		_ = frontCache.delete(trimmed)
	}
	value, err := backend.incr(trimmed)
	if err != nil {
		return 1
	}
	return value
}

func (c *AppCache) beginFlight(key string) (*cacheFlight, bool) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return &cacheFlight{done: make(chan struct{})}, true
	}

	c.flightMu.Lock()
	defer c.flightMu.Unlock()

	if c.flights == nil {
		c.flights = make(map[string]*cacheFlight)
	}
	if flight, ok := c.flights[trimmed]; ok {
		return flight, false
	}

	flight := &cacheFlight{done: make(chan struct{})}
	c.flights[trimmed] = flight
	return flight, true
}

func (c *AppCache) finishFlight(key string, flight *cacheFlight, result any, err error) {
	if flight == nil {
		return
	}

	flight.result = result
	flight.err = err
	close(flight.done)

	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return
	}

	c.flightMu.Lock()
	if current, ok := c.flights[trimmed]; ok && current == flight {
		delete(c.flights, trimmed)
	}
	c.flightMu.Unlock()
}

func (c *AppCache) GetJSON(key string, target any) bool {
	raw, ok := c.GetText(key)
	if !ok {
		return false
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		c.Delete(key)
		return false
	}
	return true
}

func (c *AppCache) SetJSON(key string, value any, ttlSec int) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return
	}
	c.SetText(key, string(encoded), ttlSec)
}

func (c *AppCache) ScopeVersion(scope string, parts ...any) int64 {
	atomic.AddInt64(&c.metrics.scopeVersionReads, 1)
	key := scopeVersionKey(scope, parts...)
	raw, ok := c.GetText(key)
	if !ok {
		c.SetText(key, "1", 0)
		return 1
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || parsed < 1 {
		c.SetText(key, "1", 0)
		return 1
	}
	return parsed
}

func (c *AppCache) BumpScope(scope string, parts ...any) int64 {
	atomic.AddInt64(&c.metrics.scopeBumps, 1)
	return maxInt64(1, c.Incr(scopeVersionKey(scope, parts...)))
}

func scopeVersionKey(scope string, parts ...any) string {
	values := []string{"ns", strings.TrimSpace(scope)}
	for _, part := range parts {
		trimmed := strings.TrimSpace(fmt.Sprint(part))
		if trimmed == "" {
			continue
		}
		values = append(values, trimmed)
	}
	return strings.Join(values, ":")
}

func BuildCacheKey(prefix string, parts ...any) string {
	values := []string{strings.TrimSpace(prefix)}
	for _, part := range parts {
		values = append(values, fmt.Sprint(part))
	}
	return strings.Join(values, ":")
}

func BuildSnapshotCacheKey(prefix string, parts ...any) string {
	arguments := append([]any{prefix}, parts...)
	return BuildCacheKey("snapshot", arguments...)
}

func CacheJSONWithState[T any](cache *AppCache, key string, ttlSec int, loader func() (T, error)) (T, CacheState, error) {
	var zero T
	if cache != nil {
		var cached T
		if cache.GetJSON(key, &cached) {
			return cached, CacheStateMemoryHit, nil
		}

		flight, leader := cache.beginFlight(key)
		if !leader {
			<-flight.done
			if flight.err != nil {
				return zero, CacheStateFlightShared, flight.err
			}
			value, ok := flight.result.(T)
			if !ok {
				return zero, CacheStateFlightShared, fmt.Errorf("cache flight result type mismatch for key %q", key)
			}
			return value, CacheStateFlightShared, nil
		}
		defer func() {
			if r := recover(); r != nil {
				cache.finishFlight(key, flight, zero, fmt.Errorf("cache loader panic for key %q: %v", key, r))
				panic(r)
			}
		}()

		value, err := loader()
		if err != nil {
			cache.finishFlight(key, flight, zero, err)
			return zero, CacheStateMiss, err
		}
		cache.SetJSON(key, value, ttlSec)
		cache.finishFlight(key, flight, value, nil)
		return value, CacheStateMiss, nil
	}

	value, err := loader()
	if err != nil {
		return zero, CacheStateMiss, err
	}
	if cache != nil {
		cache.SetJSON(key, value, ttlSec)
	}
	return value, CacheStateMiss, nil
}

func CacheJSON[T any](cache *AppCache, key string, ttlSec int, loader func() (T, error)) (T, error) {
	value, _, err := CacheJSONWithState(cache, key, ttlSec, loader)
	return value, err
}

func secondsToDuration(value int) time.Duration {
	if value <= 0 {
		return 0
	}
	return time.Duration(value) * time.Second
}

func maxInt64(left int64, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
