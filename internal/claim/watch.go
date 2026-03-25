package claim

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type tokenFileStat struct{}

func (s *Service) tokenWatchLoop(ctx context.Context) {
	if err := s.ensureTokenDir(); err != nil {
		s.logger.Error("ensure token directory for watch", "error", err)
		return
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		s.logger.Error("create token file watcher", "error", err)
		s.runTokenPollingLoop(ctx, nil)
		return
	}
	defer watcher.Close()

	if err := watcher.Add(s.tokenDirPath()); err != nil {
		s.logger.Error("watch token directory", "error", err, "dir", s.tokenDirPath())
		s.runTokenPollingLoop(ctx, nil)
		return
	}

	snapshot := s.listTokenFileNamesSnapshot()
	pollTicker := time.NewTicker(5 * time.Second)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			s.handleTokenFileEvent(ctx, event)
			snapshot = s.listTokenFileNamesSnapshot()
		case err, ok := <-watcher.Errors:
			if ok {
				s.logger.Error("token watcher error", "error", err)
			}
		case <-pollTicker.C:
			snapshot = s.pollTokenDirectoryChanges(ctx, snapshot)
		}
	}
}

func (s *Service) runTokenPollingLoop(ctx context.Context, snapshot map[string]tokenFileStat) {
	if snapshot == nil {
		snapshot = s.listTokenFileNamesSnapshot()
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot = s.pollTokenDirectoryChanges(ctx, snapshot)
		}
	}
}

func (s *Service) handleTokenFileEvent(ctx context.Context, event fsnotify.Event) {
	fileName := filepath.Base(event.Name)
	if !isJSONFileName(fileName) {
		return
	}
	if s.shouldIgnoreInternalTokenWrite(fileName) {
		return
	}

	switch {
	case event.Has(fsnotify.Remove), event.Has(fsnotify.Rename):
		if _, err := s.deactivateTokenFile(ctx, fileName); err != nil {
			s.logger.Error("deactivate token file after watcher event", "error", err, "file_name", fileName)
		}
	case event.Has(fsnotify.Create):
		s.enqueueTokenImport(ctx, fileName, "watch")
	}
}

func (s *Service) pollTokenDirectoryChanges(ctx context.Context, previous map[string]tokenFileStat) map[string]tokenFileStat {
	current := s.listTokenFileNamesSnapshot()
	for fileName := range current {
		if _, ok := previous[fileName]; !ok {
			if !s.shouldIgnoreInternalTokenWrite(fileName) {
				s.enqueueTokenImport(ctx, fileName, "poll")
			}
		}
	}
	for fileName := range previous {
		if _, ok := current[fileName]; ok {
			continue
		}
		if _, err := s.deactivateTokenFile(ctx, fileName); err != nil {
			s.logger.Error("deactivate token file after polling", "error", err, "file_name", fileName)
		}
	}
	return current
}

func (s *Service) listTokenFileNamesSnapshot() map[string]tokenFileStat {
	names := s.listTokenFileNameSet()
	snapshot := make(map[string]tokenFileStat, len(names))
	for fileName := range names {
		snapshot[fileName] = tokenFileStat{}
	}
	return snapshot
}

func isJSONFileName(fileName string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(fileName)), ".json")
}
