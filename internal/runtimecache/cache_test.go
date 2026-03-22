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
		err   error
	}

	results := make([]result, 2)
	var wg sync.WaitGroup
	for idx := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			value, err := CacheJSON(cache, "shared-key", 60, loader)
			results[index] = result{value: value, err: err}
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
	}
}
