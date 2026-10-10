package skill

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseFrontmatter_RunsIsStrict(t *testing.T) {
	for value, want := range map[string]bool{
		"true":      true,
		" true ":    true,
		"True":      false,
		"yes":       false,
		`"true"`:    false,
		"1":         false,
		"true # ok": false,
		"":          false,
	} {
		fm, body := parseFrontmatter("---\nname: tdd\ndescription: Test first\nruns:" + value + "\n---\nRED, GREEN, REFACTOR.\n")
		if fm.runs != want || fm.name != "tdd" || fm.description != "Test first" || body != "RED, GREEN, REFACTOR." {
			t.Errorf("runs:%q: %+v %q, want runs %v", value, fm, body, want)
		}
	}
	if fm, _ := parseFrontmatter("---\nname: tdd\n---\nbody"); fm.runs {
		t.Error("no runs key: runs")
	}
}

func TestFileStore_ReadsRuns(t *testing.T) {
	dir := t.TempDir()
	write := func(folder, content string) {
		if err := os.MkdirAll(filepath.Join(dir, folder), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, folder, "SKILL.md"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("tdd", "---\nname: tdd\ndescription: Test first\nruns: true\n---\nRED.")
	write("code_review", "---\nname: code-review\ndescription: Review\n---\nLook.")
	skills, err := (&FileStore{Dir: dir}).LoadAll(context.Background())
	if err != nil || len(skills) != 2 {
		t.Fatalf("%+v %v", skills, err)
	}
	byName := map[string]Skill{}
	for _, s := range skills {
		byName[s.Name] = s
	}
	if !byName["tdd"].Runs || byName["code-review"].Runs || byName["tdd"].Content != "RED." {
		t.Errorf("%+v", byName)
	}
	if v := Version(context.Background(), &FileStore{Dir: dir}); v != "" {
		t.Errorf("a directory's version: %q", v)
	}
}
