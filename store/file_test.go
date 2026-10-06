package store

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestFiles(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	clean := func() {
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-file-%'")
		s.db.Exec("DELETE FROM users WHERE id LIKE 'zz-file-%'")
	}
	clean()
	t.Cleanup(clean)
	if err := s.CreateUser(ctx, User{ID: "zz-file-alice", Email: "zz-file-alice@example.com", Role: UserRoleStandard, PasswordHash: "h"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"zz-file-s1", "zz-file-s2"} {
		if err := s.CreateSession(ctx, Session{SessionID: id, CreatedBy: "zz-file-alice", Channel: "web"}); err != nil {
			t.Fatal(err)
		}
	}

	content := []byte("a,b\n1,2\n")
	f := File{ID: "zz-file-1", SessionID: "zz-file-s1", TurnKey: "m7.jarvis", CallID: "toolu_1", AgentID: "jarvis", UserID: "zz-file-alice",
		Name: "data.csv", ContentType: "text/csv", Size: int64(len(content)), SHA256: "abc"}
	saved, err := s.SaveFile(ctx, f, content)
	if err != nil {
		t.Fatal(err)
	}
	if saved.CreatedAt.IsZero() {
		t.Error("no creation time")
	}
	other := File{ID: "zz-file-2", SessionID: "zz-file-s2", TurnKey: "m9.jarvis", Name: "x.txt", ContentType: "text/plain", SHA256: "def"}
	if _, err := s.SaveFile(ctx, other, []byte{}); err != nil {
		t.Fatal(err)
	}

	// The same call publishing the same name again (a retry) finds the
	// first file, content included; another call publishes a new one.
	again := f
	again.ID = "zz-file-again"
	if got, err := s.SaveFile(ctx, again, content); err != nil || got.ID != "zz-file-1" || !got.CreatedAt.Equal(saved.CreatedAt) {
		t.Errorf("same call again: %+v, %v", got, err)
	}
	if data, _ := s.ReadFileContent(ctx, "zz-file-1"); !bytes.Equal(data, content) {
		t.Errorf("content after a retry %q", data)
	}
	// A call's files: what a machine's directive published.
	if files, err := s.ListCallFiles(ctx, "zz-file-s1", "m7.jarvis", "toolu_1"); err != nil || len(files) != 1 || files[0].ID != "zz-file-1" {
		t.Errorf("the call's files: %+v %v", files, err)
	}
	if files, err := s.ListCallFiles(ctx, "zz-file-s1", "m7.jarvis", "toolu_2"); err != nil || len(files) != 0 {
		t.Errorf("another call's files: %+v %v", files, err)
	}

	// The same name with other content, from the same call (two paths with
	// one base name): refused, never the first file in its place.
	clash := f
	clash.ID, clash.SHA256 = "zz-file-clash", "other"
	if got, err := s.SaveFile(ctx, clash, []byte("other")); !errors.Is(err, ErrFileExists) {
		t.Errorf("same name, other content: %+v, %v, want ErrFileExists", got, err)
	}
	if f, _ := s.GetFile(ctx, "zz-file-again"); f != nil {
		t.Errorf("a retry stored a second file: %+v", f)
	}
	next := f
	next.ID, next.CallID = "zz-file-next", "toolu_2"
	if got, err := s.SaveFile(ctx, next, content); err != nil || got.ID != "zz-file-next" {
		t.Errorf("another call: %+v, %v", got, err)
	}

	got, err := s.GetFile(ctx, "zz-file-1")
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Name != "data.csv" || got.TurnKey != "m7.jarvis" || got.CallID != "toolu_1" || got.AgentID != "jarvis" || got.UserID != "zz-file-alice" || got.Size != 8 || got.SHA256 != "abc" {
		t.Errorf("got %+v", got)
	}
	if data, err := s.ReadFileContent(ctx, "zz-file-1"); err != nil || !bytes.Equal(data, content) {
		t.Errorf("content %q, %v", data, err)
	}
	if f, err := s.GetFile(ctx, "zz-file-none"); f != nil || err != nil {
		t.Errorf("unknown file: %v, %v", f, err)
	}
	if data, err := s.ReadFileContent(ctx, "zz-file-none"); data != nil || err != nil {
		t.Errorf("unknown content: %v, %v", data, err)
	}

	// A session lists its own files only.
	list, err := s.ListSessionFiles(ctx, "zz-file-s1")
	if err != nil || len(list) != 2 || list[0].ID != "zz-file-1" || list[1].ID != "zz-file-next" {
		t.Errorf("s1's files: %+v, %v", list, err)
	}

	// A session that does not exist takes no file.
	gone := f
	gone.ID, gone.SessionID = "zz-file-3", "zz-file-none"
	if _, err := s.SaveFile(ctx, gone, content); !errors.Is(err, ErrFileSessionGone) {
		t.Errorf("file of no session: %v, want ErrFileSessionGone", err)
	}

	// Deleting a session deletes its files, content included; the other
	// session keeps its own.
	if err := s.DeleteSession(ctx, "zz-file-s1"); err != nil {
		t.Fatal(err)
	}
	if f, _ := s.GetFile(ctx, "zz-file-1"); f != nil {
		t.Errorf("file left after its session's deletion: %+v", f)
	}
	var n int
	s.db.QueryRow("SELECT count(*) FROM file_contents WHERE file_id = 'zz-file-1'").Scan(&n)
	if n != 0 {
		t.Errorf("content left after its session's deletion")
	}
	if f, _ := s.GetFile(ctx, "zz-file-2"); f == nil {
		t.Error("another session's file went too")
	}
}
