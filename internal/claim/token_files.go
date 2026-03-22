package claim

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

func (s *Service) listTokenDirEntries() ([]os.DirEntry, error) {
	entries, err := os.ReadDir(s.tokenDirPath())
	if err != nil {
		return nil, fmt.Errorf("read token directory: %w", err)
	}

	tokenEntries := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isJSONFileName(entry.Name()) {
			continue
		}
		tokenEntries = append(tokenEntries, entry)
	}

	sort.Slice(tokenEntries, func(i int, j int) bool {
		left := tokenEntries[i].Name()
		right := tokenEntries[j].Name()
		leftLower := strings.ToLower(left)
		rightLower := strings.ToLower(right)
		if leftLower == rightLower {
			return left < right
		}
		return leftLower < rightLower
	})
	return tokenEntries, nil
}
