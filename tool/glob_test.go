package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupGlobWorkspace(t *testing.T) (string, *Registry) {
	t.Helper()
	dir := t.TempDir()
	r := NewRegistry()
	RegisterGlobTool(r, dir)

	// Create test structure:
	// root/
	//   main.go
	//   config.yaml
	//   src/
	//     app.go
	//     app_test.go
	//     util/
	//       helper.go
	//   docs/
	//     readme.md
	//   .hidden/
	//     secret.go

	files := map[string]string{
		"main.go":            "package main",
		"config.yaml":        "key: value",
		"src/app.go":         "package src",
		"src/app_test.go":    "package src",
		"src/util/helper.go": "package util",
		"docs/readme.md":     "# Docs",
		".hidden/secret.go":  "package hidden",
	}

	for path, content := range files {
		full := filepath.Join(dir, path)
		os.MkdirAll(filepath.Dir(full), 0755)
		os.WriteFile(full, []byte(content), 0644)
	}

	return dir, r
}

func globExec(t *testing.T, r *Registry, params interface{}) (string, error) {
	t.Helper()
	input, _ := json.Marshal(params)
	return r.Execute(context.Background(), "glob", input)
}

func TestGlob_RecursiveAllGo(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "**/*.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(result, "main.go") {
		t.Errorf("expected main.go: %s", result)
	}
	if !strings.Contains(result, "app.go") {
		t.Errorf("expected app.go: %s", result)
	}
	if !strings.Contains(result, "helper.go") {
		t.Errorf("expected helper.go: %s", result)
	}
}

func TestGlob_RecursiveSkipsHidden(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "**/*.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(result, "secret.go") {
		t.Errorf("should skip hidden dirs: %s", result)
	}
}

func TestGlob_RecursiveSubdir(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "src/**/*.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(result, "app.go") {
		t.Errorf("expected app.go: %s", result)
	}
	if !strings.Contains(result, "helper.go") {
		t.Errorf("expected helper.go: %s", result)
	}
	if strings.Contains(result, "main.go") {
		t.Errorf("should not contain root main.go: %s", result)
	}
}

func TestGlob_NonRecursive(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "*.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(result, "main.go") {
		t.Errorf("expected main.go: %s", result)
	}
	// Should NOT include files in subdirs
	if strings.Contains(result, "app.go") {
		t.Errorf("should not contain src/app.go: %s", result)
	}
}

func TestGlob_NonRecursiveSubdir(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "src/*.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(result, "app.go") {
		t.Errorf("expected app.go: %s", result)
	}
	if !strings.Contains(result, "app_test.go") {
		t.Errorf("expected app_test.go: %s", result)
	}
	if strings.Contains(result, "helper.go") {
		t.Errorf("should not contain nested helper.go: %s", result)
	}
}

func TestGlob_YamlFiles(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "**/*.yaml",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(result, "config.yaml") {
		t.Errorf("expected config.yaml: %s", result)
	}
}

func TestGlob_NoMatch(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "**/*.rs",
	})
	if err != nil {
		t.Fatal(err)
	}

	if result != "No files found." {
		t.Errorf("expected no files message, got: %s", result)
	}
}

func TestGlob_WithPathParam(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "**/*.go",
		"path":    "src",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(result, "app.go") {
		t.Errorf("expected app.go: %s", result)
	}
	if strings.Contains(result, "main.go") {
		t.Errorf("should not contain root main.go: %s", result)
	}
}

func TestGlob_TestFiles(t *testing.T) {
	_, r := setupGlobWorkspace(t)

	result, err := globExec(t, r, map[string]string{
		"pattern": "**/*_test.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(result, "app_test.go") {
		t.Errorf("expected app_test.go: %s", result)
	}
}
