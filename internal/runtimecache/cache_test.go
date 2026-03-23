package runtimecache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheJSONSingleflightSharesConcurrentMiss(t *testing.T) {
	cache := &AppCache{
		backendName: "memory",
		backend:     newMemoryBackend(),
		flights:     make(map[string]*cacheFlight),
	}

	start := make(chan struct{})
	var calls atomic.Int32

	loader := func() (map[string]int, error) {
		calls.Add(1)
		<-start
		return map[string]int{"value": 7}, nil
	}

	type result struct {
		value map[string]int
		state CacheState
		err   error
	}

	results := make([]result, 2)
	var wg sync.WaitGroup
	for idx := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			value, state, err := CacheJSONWithState(cache, "shared-key", 60, loader)
			results[index] = result{value: value, state: state, err: err}
		}(idx)
	}

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("loader did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected loader to run once, got %d", got)
	}
	for idx, item := range results {
		if item.err != nil {
			t.Fatalf("result %d returned error: %v", idx, item.err)
		}
		if item.value["value"] != 7 {
			t.Fatalf("result %d returned unexpected payload: %#v", idx, item.value)
		}
		if item.state != CacheStateMiss && item.state != CacheStateFlightShared {
			t.Fatalf("result %d returned unexpected cache state: %q", idx, item.state)
		}
	}
}

func TestCacheJSONWithStateMemoryHit(t *testing.T) {
	cache := &AppCache{
		backendName: "memory",
		backend:     newMemoryBackend(),
		flights:     make(map[string]*cacheFlight),
	}
	cache.SetJSON("memory-hit", map[string]int{"value": 11}, 60)

	value, state, err := CacheJSONWithState(cache, "memory-hit", 60, func() (map[string]int, error) {
		t.Fatal("loader should not run on memory hit")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("memory hit returned error: %v", err)
	}
	if state != CacheStateMemoryHit {
		t.Fatalf("expected memory hit state, got %q", state)
	}
	if value["value"] != 11 {
		t.Fatalf("unexpected memory hit payload: %#v", value)
	}
}

func TestAppCacheUsesFrontCacheForHotSnapshotKeys(t *testing.T) {
	cache := &AppCache{
		backendName: "redis",
		backend:     newMemoryBackend(),
		frontCache:  newMemoryBackend(),
		flights:     make(map[string]*cacheFlight),
	}

	if err := cache.backend.setText("snapshot:user-runtime-snapshot:42", `{"value":19}`, time.Minute); err != nil {
		t.Fatalf("seed backend cache entry: %v", err)
	}

	raw, ok := cache.GetText("snapshot:user-runtime-snapshot:42")
	if !ok || raw != `{"value":19}` {
		t.Fatalf("expected backend hit to return seeded payload, got ok=%v raw=%q", ok, raw)
	}

	if _, ok := cache.frontCache.getText("snapshot:user-runtime-snapshot:42"); !ok {
		t.Fatal("expected backend read to populate front cache")
	}

	if _, ok := cache.GetText("snapshot:user-runtime-snapshot:42"); !ok {
		t.Fatal("expected front cache hit on second read")
	}

	stats := cache.StatsSnapshot()
	if stats.BackendHits != 1 {
		t.Fatalf("expected one backend hit, got %+v", stats)
	}
	if stats.LocalHits != 1 {
		t.Fatalf("expected one local hit, got %+v", stats)
	}
}
