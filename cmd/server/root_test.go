package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveRuntimeRootPrefersCurrentProjectRoot(t *testing.T) {
	root := makeRuntimeRoot(t)
	executablePath := filepath.Join(root, "bin", "token-atlas.exe")

	resolved := resolveRuntimeRoot(root, executablePath)
	if resolved != root {
		t.Fatalf("unexpected runtime root: got %q want %q", resolved, root)
	}
}

func TestResolveRuntimeRootUsesParentOfBinWorkingDirectory(t *testing.T) {
	root := makeRuntimeRoot(t)
	binDir := filepath.Join(root, "bin")
	executablePath := filepath.Join(binDir, "token-atlas.exe")

	resolved := resolveRuntimeRoot(binDir, executablePath)
	if resolved != root {
		t.Fatalf("unexpected runtime root from bin cwd: got %q want %q", resolved, root)
	}
}

func TestResolveRuntimeRootUsesExecutableParentWhenLaunchedElsewhere(t *testing.T) {
	root := makeRuntimeRoot(t)
	otherDir := t.TempDir()
	executablePath := filepath.Join(root, "bin", "token-atlas.exe")

	resolved := resolveRuntimeRoot(otherDir, executablePath)
	if resolved != root {
		t.Fatalf("unexpected runtime root from external cwd: got %q want %q", resolved, root)
	}
}

func makeRuntimeRoot(t *testing.T) string {
	t.Helper()

	root := t.TempDir()
	for _, dir := range []string{
		filepath.Join(root, "static"),
		filepath.Join(root, "token"),
		filepath.Join(root, "bin"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create dir %q: %v", dir, err)
		}
	}

	if err := os.WriteFile(filepath.Join(root, ".env.example"), []byte("PORT=8000\n"), 0o644); err != nil {
		t.Fatalf("write env example: %v", err)
	}

	return root
}
