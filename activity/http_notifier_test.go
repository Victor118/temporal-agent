package activity

import (
	"context"
	"errors"
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
	if err := n.Notify(context.Background(), Notification{SessionID: "s1", Event: SSEEvent{Type: "message", Data: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if got != "Bearer k3y" {
		t.Errorf("Authorization = %q", got)
	}
}

// A refused notification is the activity's failure, so that it is retried and
// seen, rather than a line in the worker's log.
func TestHTTPNotifier_ReportsARefusal(t *testing.T) {
	status := http.StatusUnauthorized
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	defer srv.Close()
	n := NewHTTPNotifier(srv.URL, "")

	if err := n.Notify(context.Background(), Notification{SessionID: "s1"}); !errors.Is(err, ErrNotifyKeyRefused) {
		t.Errorf("Notify = %v, want the key refused", err)
	}
	if err := n.Check(context.Background()); !errors.Is(err, ErrNotifyKeyRefused) {
		t.Errorf("Check = %v, want the key refused", err)
	}
	status = http.StatusBadGateway
	if err := n.Notify(context.Background(), Notification{SessionID: "s1"}); err == nil {
		t.Error("a failed notification was reported as sent")
	}
}

// An event that does not encode is the notifier's error, not an empty body
// the server would refuse.
func TestHTTPNotifier_ReportsAnEventThatDoesNotEncode(t *testing.T) {
	posted := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posted = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := NewHTTPNotifier(srv.URL, "k3y")
	err := n.Notify(context.Background(), Notification{SessionID: "s1", Event: SSEEvent{Type: "notification", Data: []byte("not json")}})
	if err == nil {
		t.Error("an event whose data is not JSON was reported as sent")
	}
	if posted {
		t.Error("the event was posted anyway")
	}
}
