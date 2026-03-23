package runtimecache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"token-atlas/internal/config"
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

func (b *redisBackend) getText(key string) (string, bool) {
	value, err := b.client.Get(context.Background(), b.key(key)).Result()
	if err == redis.Nil {
		return "", false
	}
	if err != nil {
		return "", false
	}
	return value, true
}

func (b *redisBackend) setText(key string, value string, ttl time.Duration) error {
	if ttl <= 0 {
		return b.client.Set(context.Background(), b.key(key), value, 0).Err()
	}
	return b.client.SetEx(context.Background(), b.key(key), value, ttl).Err()
}

func (b *redisBackend) delete(key string) error {
	return b.client.Del(context.Background(), b.key(key)).Err()
}

func (b *redisBackend) incr(key string) (int64, error) {
	return b.client.Incr(context.Background(), b.key(key)).Result()
}

type AppCache struct {
	mu          sync.RWMutex
	backendName string
	backend     backend

	flightMu sync.Mutex
	flights  map[string]*cacheFlight
}

type cacheFlight struct {
	done   chan struct{}
	result any
	err    error
}

type CacheState string

const (
	CacheStateMemoryHit   CacheState = "memory_hit"
	CacheStateFlightShared CacheState = "flight_shared"
	CacheStateMiss        CacheState = "miss"
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

func (c *AppCache) GetText(key string) (string, bool) {
	c.mu.RLock()
	backend := c.backend
	c.mu.RUnlock()
	return backend.getText(strings.TrimSpace(key))
}

func (c *AppCache) SetText(key string, value string, ttlSec int) {
	if strings.TrimSpace(key) == "" {
		return
	}
	c.mu.RLock()
	backend := c.backend
	c.mu.RUnlock()
	_ = backend.setText(strings.TrimSpace(key), value, secondsToDuration(ttlSec))
}

func (c *AppCache) Delete(key string) {
	if strings.TrimSpace(key) == "" {
		return
	}
	c.mu.RLock()
	backend := c.backend
	c.mu.RUnlock()
	_ = backend.delete(strings.TrimSpace(key))
}

func (c *AppCache) Incr(key string) int64 {
	if strings.TrimSpace(key) == "" {
		return 1
	}
	c.mu.RLock()
	backend := c.backend
	c.mu.RUnlock()
	value, err := backend.incr(strings.TrimSpace(key))
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
