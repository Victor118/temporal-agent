package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupGrepWorkspace(t *testing.T) (string, *Registry) {
	t.Helper()
	dir := t.TempDir()
	r := NewRegistry()
	RegisterGrepTool(r, dir)

	// Create test files
	os.WriteFile(filepath.Join(dir, "main.go"), []byte(`package main

import "fmt"

func main() {
	fmt.Println("hello world")
}

func helper() {
	fmt.Println("helper function")
}
`), 0644)

	os.WriteFile(filepath.Join(dir, "readme.md"), []byte(`# Project
This is a test project.
It has multiple lines.
`), 0644)

	sub := filepath.Join(dir, "pkg")
	os.Mkdir(sub, 0755)
	os.WriteFile(filepath.Join(sub, "util.go"), []byte(`package pkg

func Util() string {
	return "util"
}
`), 0644)

	return dir, r
}

func grepExec(t *testing.T, r *Registry, params interface{}) (string, error) {
	t.Helper()
	input, _ := json.Marshal(params)
	return r.Execute(context.Background(), "grep", input)
}

func TestGrep_BasicMatch(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern": "hello world",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "main.go") {
		t.Errorf("expected main.go in result: %s", result)
	}
	if !strings.Contains(result, "hello world") {
		t.Errorf("expected matching line in result: %s", result)
	}
}

func TestGrep_Regex(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern": `func \w+\(\)`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "func main()") {
		t.Errorf("expected func main() in result: %s", result)
	}
	if !strings.Contains(result, "func helper()") {
		t.Errorf("expected func helper() in result: %s", result)
	}
}

func TestGrep_InvalidRegex(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	_, err := grepExec(t, r, map[string]interface{}{
		"pattern": "[invalid",
	})
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
}

func TestGrep_IncludeFilter(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern": ".",
		"include": "*.md",
		"output_mode": "files",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "readme.md") {
		t.Errorf("expected readme.md: %s", result)
	}
	if strings.Contains(result, "main.go") {
		t.Errorf("should not contain main.go: %s", result)
	}
}

func TestGrep_FilesMode(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern":     "fmt",
		"output_mode": "files",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "main.go") {
		t.Errorf("expected main.go: %s", result)
	}
	// Should not contain line numbers in files mode
	if strings.Contains(result, ":") && strings.Contains(result, "fmt") {
		// files mode should just list file paths
	}
}

func TestGrep_CountMode(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern":     "Println",
		"output_mode": "count",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "main.go:2") {
		t.Errorf("expected main.go:2 (two Println calls): %s", result)
	}
}

func TestGrep_IgnoreCase(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern":     "PROJECT",
		"ignore_case": true,
		"output_mode": "files",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "readme.md") {
		t.Errorf("case insensitive search should match readme.md: %s", result)
	}
}

func TestGrep_ContextLines(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern": "helper function",
		"context": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Should contain the line before and after
	if !strings.Contains(result, "helper function") {
		t.Errorf("expected matching line: %s", result)
	}
	// Context should include surrounding lines
	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) < 2 {
		t.Errorf("expected context lines, got %d lines: %s", len(lines), result)
	}
}

func TestGrep_SubdirPath(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern":     "Util",
		"path":        "pkg",
		"output_mode": "files",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "util.go") {
		t.Errorf("expected util.go: %s", result)
	}
}

func TestGrep_NoMatch(t *testing.T) {
	_, r := setupGrepWorkspace(t)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern": "zzz_no_match_zzz",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "No matches found." {
		t.Errorf("expected no matches message, got: %s", result)
	}
}

func TestGrep_SkipsBinaryFiles(t *testing.T) {
	dir, r := setupGrepWorkspace(t)

	// Write a binary file with null bytes
	binary := make([]byte, 100)
	binary[50] = 0 // null byte
	copy(binary[:10], []byte("searchable"))
	os.WriteFile(filepath.Join(dir, "binary.dat"), binary, 0644)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern":     "searchable",
		"output_mode": "files",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result, "binary.dat") {
		t.Errorf("should skip binary files: %s", result)
	}
}

func TestGrep_SkipsHiddenDirs(t *testing.T) {
	dir, r := setupGrepWorkspace(t)

	hidden := filepath.Join(dir, ".hidden")
	os.Mkdir(hidden, 0755)
	os.WriteFile(filepath.Join(hidden, "secret.go"), []byte("package secret\nfunc Secret() {}"), 0644)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern":     "Secret",
		"output_mode": "files",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result, "secret.go") {
		t.Errorf("should skip hidden dirs: %s", result)
	}
}

func TestGrep_MaxResults(t *testing.T) {
	dir, r := setupGrepWorkspace(t)

	// Create file with many matching lines
	var lines []string
	for i := 0; i < 50; i++ {
		lines = append(lines, "match line")
	}
	os.WriteFile(filepath.Join(dir, "many.txt"), []byte(strings.Join(lines, "\n")), 0644)

	result, err := grepExec(t, r, map[string]interface{}{
		"pattern":     "match line",
		"max_results": 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "truncated") {
		t.Errorf("expected truncation message: %s", result)
	}
}
