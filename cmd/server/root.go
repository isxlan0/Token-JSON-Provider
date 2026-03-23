package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const runtimeRootEnv = "TOKEN_ATLAS_RUNTIME_ROOT"

func prepareRuntimeRoot() (string, error) {
	if configured := strings.TrimSpace(os.Getenv(runtimeRootEnv)); configured != "" {
		root, err := filepath.Abs(configured)
		if err != nil {
			return "", fmt.Errorf("resolve %s=%q: %w", runtimeRootEnv, configured, err)
		}
		root = filepath.Clean(root)
		if !looksLikeRuntimeRoot(root) {
			return "", fmt.Errorf("%s=%q is not a valid project root", runtimeRootEnv, root)
		}
		if err := os.Chdir(root); err != nil {
			return "", fmt.Errorf("switch working directory to %q: %w", root, err)
		}
		return root, nil
	}

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
		return "", fmt.Errorf("resolve project root failed; start from the project root or set %s", runtimeRootEnv)
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

	if executablePath != "" {
		executableDir := filepath.Dir(executablePath)
		appendCandidate(executableDir)
		if isBinDir(executableDir) {
			appendCandidate(filepath.Dir(executableDir))
		}
	}

	appendCandidate(cwd)
	if isBinDir(cwd) {
		appendCandidate(filepath.Dir(cwd))
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

	return isFile(filepath.Join(dir, "go.mod")) ||
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
