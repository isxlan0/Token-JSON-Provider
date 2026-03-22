package claim

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type tokenFileStat struct {
	size    int64
	modTime int64
}

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

	snapshot := s.listTokenFileStats()
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
			snapshot = s.listTokenFileStats()
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
		snapshot = s.listTokenFileStats()
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
	case event.Has(fsnotify.Create), event.Has(fsnotify.Write), event.Has(fsnotify.Chmod):
		s.enqueueTokenImport(fileName, "watch")
	}
}

func (s *Service) pollTokenDirectoryChanges(ctx context.Context, previous map[string]tokenFileStat) map[string]tokenFileStat {
	current := s.listTokenFileStats()
	for fileName, stat := range current {
		old, ok := previous[fileName]
		if !ok || old != stat {
			if !s.shouldIgnoreInternalTokenWrite(fileName) {
				s.enqueueTokenImport(fileName, "poll")
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

func (s *Service) listTokenFileStats() map[string]tokenFileStat {
	entries, err := os.ReadDir(s.tokenDirPath())
	if err != nil {
		return map[string]tokenFileStat{}
	}

	stats := make(map[string]tokenFileStat, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isJSONFileName(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		stats[entry.Name()] = tokenFileStat{
			size:    info.Size(),
			modTime: info.ModTime().UnixNano(),
		}
	}
	return stats
}

func isJSONFileName(fileName string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(fileName)), ".json")
}
