package activity

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPNotifier_PresentsTheKey(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := NewHTTPNotifier(srv.URL, "k3y")
	if err := n.post("s1", SSEEvent{Type: "message", Data: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if got != "Bearer k3y" {
		t.Errorf("Authorization = %q", got)
	}
}

func TestHTTPNotifier_ReportsARefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	if err := NewHTTPNotifier(srv.URL, "").post("s1", SSEEvent{Type: "message"}); err == nil {
		t.Error("a refused notification was reported as sent")
	}
}
