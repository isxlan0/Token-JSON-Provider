package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func prepareRuntimeRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}

	executablePath, err := os.Executable()
	if err != nil {
		executablePath = ""
	}

	root := resolveRuntimeRoot(cwd, executablePath)
	if root == "" {
		root = cwd
	}

	if err := os.Chdir(root); err != nil {
		return "", fmt.Errorf("switch working directory to %q: %w", root, err)
	}

	return root, nil
}

func resolveRuntimeRoot(cwd string, executablePath string) string {
	candidates := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)

	appendCandidate := func(path string) {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			return
		}

		absolute, err := filepath.Abs(trimmed)
		if err != nil {
			return
		}

		normalized := filepath.Clean(absolute)
		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		candidates = append(candidates, normalized)
	}

	appendCandidate(cwd)
	if isBinDir(cwd) {
		appendCandidate(filepath.Dir(cwd))
	}

	if executablePath != "" {
		executableDir := filepath.Dir(executablePath)
		appendCandidate(executableDir)
		if isBinDir(executableDir) {
			appendCandidate(filepath.Dir(executableDir))
		}
	}

	for _, candidate := range candidates {
		if looksLikeRuntimeRoot(candidate) {
			return candidate
		}
	}

	return ""
}

func isBinDir(path string) bool {
	return strings.EqualFold(filepath.Base(filepath.Clean(path)), "bin")
}

func looksLikeRuntimeRoot(dir string) bool {
	if !isDir(filepath.Join(dir, "static")) {
		return false
	}

	return isDir(filepath.Join(dir, "token")) ||
		isDir(filepath.Join(dir, "cmd")) ||
		isFile(filepath.Join(dir, ".env")) ||
		isFile(filepath.Join(dir, ".env.example"))
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
