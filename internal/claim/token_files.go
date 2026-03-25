package claim

import (
	"fmt"
	"os"
)

func (s *Service) listTokenFileNames() ([]string, error) {
	entries, err := os.ReadDir(s.tokenDirPath())
	if err != nil {
		return nil, fmt.Errorf("read token directory: %w", err)
	}

	fileNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isJSONFileName(entry.Name()) {
			continue
		}
		fileNames = append(fileNames, entry.Name())
	}
	return fileNames, nil
}

func (s *Service) listTokenFileNameSet() map[string]struct{} {
	fileNames, err := s.listTokenFileNames()
	if err != nil {
		return map[string]struct{}{}
	}

	names := make(map[string]struct{}, len(fileNames))
	for _, fileName := range fileNames {
		names[fileName] = struct{}{}
	}
	return names
}
