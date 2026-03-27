package claim

import (
	"context"
	"testing"

	"token-atlas/internal/config"
	"token-atlas/internal/runtimecache"
)

func TestGetRuntimeSnapshotDeletesPreviousVersionedCacheKey(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cfg.Cache.MeTTL = 60
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory", MeTTL: 60}, nil)

	userID := insertTestUser(t, store, "9101", "runtime-cache-user")

	if _, err := service.GetRuntimeSnapshot(context.Background(), userID); err != nil {
		t.Fatalf("get first runtime snapshot: %v", err)
	}
	firstKey := service.userRuntimeSnapshotCacheKey(userID)

	var first runtimeSnapshotPayload
	if !service.cache.GetJSON(firstKey, &first) {
		t.Fatalf("expected first runtime snapshot cache key %q to exist", firstKey)
	}

	service.invalidateUserClaimsCache(userID)

	if _, err := service.GetRuntimeSnapshot(context.Background(), userID); err != nil {
		t.Fatalf("get second runtime snapshot: %v", err)
	}
	secondKey := service.userRuntimeSnapshotCacheKey(userID)
	if firstKey == secondKey {
		t.Fatalf("expected runtime snapshot key to rotate after invalidation")
	}

	var stale runtimeSnapshotPayload
	if service.cache.GetJSON(firstKey, &stale) {
		t.Fatalf("expected previous runtime snapshot cache key %q to be deleted", firstKey)
	}

	var second runtimeSnapshotPayload
	if !service.cache.GetJSON(secondKey, &second) {
		t.Fatalf("expected refreshed runtime snapshot cache key %q to exist", secondKey)
	}
}

func TestSetQueueStatusSnapshotDeletesPreviousVersionedCacheKey(t *testing.T) {
	service, _ := newClaimTestService(t)
	service.cfg.Cache.QueueTTL = 60
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory", QueueTTL: 60}, nil)

	userID := int64(9201)
	firstPayload := queueStatusPayload{
		Queued:          true,
		Status:          queueStatusQueuedWaiting,
		Position:        1,
		Requested:       1,
		Remaining:       1,
		TotalQueued:     1,
		AvailableTokens: 10,
	}
	service.setQueueStatusSnapshot(userID, firstPayload)
	firstKey := service.userQueueCacheKey(userID)

	var first queueStatusPayload
	if !service.cache.GetJSON(firstKey, &first) {
		t.Fatalf("expected first queue snapshot cache key %q to exist", firstKey)
	}

	secondPayload := firstPayload
	secondPayload.Position = 2
	secondPayload.Ahead = 1
	service.setQueueStatusSnapshot(userID, secondPayload)
	secondKey := service.userQueueCacheKey(userID)
	if firstKey == secondKey {
		t.Fatalf("expected queue snapshot key to rotate after payload change")
	}

	var previous queueStatusPayload
	if service.cache.GetJSON(firstKey, &previous) {
		t.Fatalf("expected previous queue snapshot cache key %q to be deleted", firstKey)
	}

	var current queueStatusPayload
	if !service.cache.GetJSON(secondKey, &current) {
		t.Fatalf("expected refreshed queue snapshot cache key %q to exist", secondKey)
	}
}

func TestSetClaimRealtimeSnapshotDeletesPreviousVersionedCacheKey(t *testing.T) {
	service, _ := newClaimTestService(t)
	service.cfg.Cache.QueueTTL = 60
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory", QueueTTL: 60}, nil)

	userID := int64(9301)
	firstSnapshot := claimRealtimeSnapshot{
		Requests: []claimRealtimeRequest{
			{
				RequestID: "req-1",
				Status:    queueStatusQueuedWaiting,
				Requested: 1,
				Granted:   0,
				Remaining: 1,
				Queued:    true,
				UpdatedAt: isoformatNow(),
			},
		},
		StreamRequired: true,
		Transport:      "sse",
	}
	service.setClaimRealtimeSnapshot(userID, firstSnapshot)
	firstKey := service.userClaimRealtimeCacheKey(userID)

	var first claimRealtimeSnapshot
	if !service.cache.GetJSON(firstKey, &first) {
		t.Fatalf("expected first realtime snapshot cache key %q to exist", firstKey)
	}

	secondSnapshot := firstSnapshot
	secondSnapshot.Requests = []claimRealtimeRequest{
		{
			RequestID: "req-1",
			Status:    claimStatusSucceeded,
			Requested: 1,
			Granted:   1,
			Remaining: 0,
			Queued:    false,
			UpdatedAt: isoformatNow(),
			Terminal:  true,
		},
	}
	service.setClaimRealtimeSnapshot(userID, secondSnapshot)
	secondKey := service.userClaimRealtimeCacheKey(userID)
	if firstKey == secondKey {
		t.Fatalf("expected realtime snapshot key to rotate after payload change")
	}

	var previous claimRealtimeSnapshot
	if service.cache.GetJSON(firstKey, &previous) {
		t.Fatalf("expected previous realtime snapshot cache key %q to be deleted", firstKey)
	}

	var current claimRealtimeSnapshot
	if !service.cache.GetJSON(secondKey, &current) {
		t.Fatalf("expected refreshed realtime snapshot cache key %q to exist", secondKey)
	}
}
