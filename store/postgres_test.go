package store

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

// openTestStore connects to TEST_DATABASE_URL, a throwaway database: the test
// writes agents into it. Without it the integration tests are skipped.
func openTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	s, err := NewPostgresStore(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAgentLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const id = "zz-store-test"
	s.DeleteAgent(ctx, id) // leftover from an aborted run
	t.Cleanup(func() { s.DeleteAgent(ctx, id) })

	if err := s.CreateAgent(ctx, Agent{ID: id, Name: "Test"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAgent(ctx, Agent{ID: id, Name: "Again"}); !errors.Is(err, ErrAgentExists) {
		t.Errorf("second create: %v, want ErrAgentExists", err)
	}

	a, err := s.GetAgent(ctx, id)
	if err != nil || a == nil {
		t.Fatalf("get: %v %v", a, err)
	}
	// A nil allowlist is stored as [], which grants nothing.
	if a.Tools == nil || len(a.Tools) != 0 || a.Revision != 1 {
		t.Errorf("created %+v", a)
	}

	rev, err := s.UpdateAgent(ctx, Agent{ID: id, Name: "Renamed", Tools: []string{"read_file"}}, 1)
	if err != nil || rev != 2 {
		t.Fatalf("update: rev %d, %v", rev, err)
	}
	if _, err := s.UpdateAgent(ctx, Agent{ID: id, Name: "Stale"}, 1); !errors.Is(err, ErrAgentConflict) {
		t.Errorf("stale update: %v, want ErrAgentConflict", err)
	}
	if _, err := s.UpdateAgent(ctx, Agent{ID: "zz-nobody", Name: "X"}, 1); !errors.Is(err, ErrAgentNotFound) {
		t.Errorf("update of a missing agent: %v, want ErrAgentNotFound", err)
	}

	a, _ = s.GetAgent(ctx, id)
	if a.Name != "Renamed" || !reflect.DeepEqual(a.Tools, []string{"read_file"}) || a.UpdatedAt.Before(a.CreatedAt) {
		t.Errorf("after update %+v", a)
	}

	if err := s.DeleteAgent(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAgent(ctx, id); !errors.Is(err, ErrAgentNotFound) {
		t.Errorf("second delete: %v, want ErrAgentNotFound", err)
	}
}
