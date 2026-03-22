package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func TestShutdownRuntimeStopsWorkersAndClosesDatabaseAfterHTTPShutdownError(t *testing.T) {
	events := &sharedEventLog{}
	server := &stubShutdownServer{events: events, shutdownErr: context.DeadlineExceeded}
	workers := &stubWorkerStopper{events: events}
	store := &stubCloser{events: events}

	err := shutdownRuntime(slog.New(slog.NewTextHandler(io.Discard, nil)), server, workers, store, true)
	if err == nil {
		t.Fatal("expected shutdownRuntime to return shutdown error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	got := events.snapshot()
	want := []string{"shutdown", "close", "stop", "store-close"}
	if len(got) != len(want) {
		t.Fatalf("unexpected event count: got %v want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("unexpected event order: got %v want %v", got, want)
		}
	}
	if server.shutdownCtx == nil || workers.stopCtx == nil {
		t.Fatalf("expected both shutdown and stop contexts to be set")
	}
	if server.shutdownCtx == workers.stopCtx {
		t.Fatal("expected HTTP shutdown and worker stop to use different contexts")
	}
}

func TestShutdownRuntimeClosesDatabaseEvenWhenWorkerStopFails(t *testing.T) {
	events := &sharedEventLog{}
	server := &stubShutdownServer{events: events}
	workers := &stubWorkerStopper{
		events:  events,
		stopErr: errors.New("worker stop failed"),
	}
	store := &stubCloser{events: events}

	err := shutdownRuntime(slog.New(slog.NewTextHandler(io.Discard, nil)), server, workers, store, true)
	if err == nil {
		t.Fatal("expected shutdownRuntime to return stop error")
	}
	if !errors.Is(err, workers.stopErr) {
		t.Fatalf("expected stop error, got %v", err)
	}
	if store.closeCalls != 1 {
		t.Fatalf("expected store close to run once, got %d", store.closeCalls)
	}
}

type stubShutdownServer struct {
	events      *sharedEventLog
	shutdownErr error
	closeErr    error
	shutdownCtx context.Context
}

func (s *stubShutdownServer) Shutdown(ctx context.Context) error {
	s.events.append("shutdown")
	s.shutdownCtx = ctx
	return s.shutdownErr
}

func (s *stubShutdownServer) Close() error {
	s.events.append("close")
	return s.closeErr
}

type stubWorkerStopper struct {
	events  *sharedEventLog
	stopErr error
	stopCtx context.Context
}

func (s *stubWorkerStopper) Stop(ctx context.Context) error {
	s.events.append("stop")
	s.stopCtx = ctx
	return s.stopErr
}

type stubCloser struct {
	events     *sharedEventLog
	closeErr   error
	closeCalls int
}

func (s *stubCloser) Close() error {
	s.events.append("store-close")
	s.closeCalls++
	return s.closeErr
}

type sharedEventLog struct {
	items []string
}

func (l *sharedEventLog) append(value string) {
	l.items = append(l.items, value)
}

func (l *sharedEventLog) snapshot() []string {
	return append([]string{}, l.items...)
}
