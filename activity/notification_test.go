package activity

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/temporal"
)

type recordingNotifier struct{ got []Notification }

func (r *recordingNotifier) Notify(_ context.Context, n Notification) error {
	r.got = append(r.got, n)
	return nil
}

func TestNotifyStep_PicksTheChannel(t *testing.T) {
	web, tg := &recordingNotifier{}, &recordingNotifier{}
	a := &NotificationActivities{Notifiers: map[string]Notifier{ChannelWeb: web, "telegram": tg}}
	ctx := context.Background()

	for _, in := range []NotifyInput{
		{SessionID: "s1", Event: SSEEvent{Type: "message"}},
		{SessionID: "s1", Channel: "web", Event: SSEEvent{Type: "message"}},
		{SessionID: "s2", Channel: "telegram", ChannelID: "42", Event: SSEEvent{Type: "ask_user"}},
		{SessionID: "s3", Channel: "slack", Event: SSEEvent{Type: "message"}}, // none registered
	} {
		if err := a.NotifyStep(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	if len(web.got) != 2 || web.got[0].SessionID != "s1" {
		t.Errorf("web got %+v", web.got)
	}
	if len(tg.got) != 1 || tg.got[0].ChannelID != "42" || tg.got[0].Event.Type != "ask_user" {
		t.Errorf("telegram got %+v", tg.got)
	}
}

type failingNotifier struct{ err error }

func (f failingNotifier) Notify(context.Context, Notification) error { return f.err }

// A channel that delivered part of a notification must not get it again: the
// error stops the retries. Any other failure stays retryable.
func TestNotifyStep_NeverRetriesAPartialDelivery(t *testing.T) {
	partial := &PartialDelivery{Delivered: 1, Total: 3, Err: errors.New("timeout")}
	a := &NotificationActivities{Notifiers: map[string]Notifier{"telegram": failingNotifier{partial}}}
	err := a.NotifyStep(context.Background(), NotifyInput{Channel: "telegram"})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() || appErr.Type() != "PartialDelivery" {
		t.Errorf("error %v, want a non-retryable PartialDelivery", err)
	}

	a.Notifiers["telegram"] = failingNotifier{errors.New("connection refused")}
	err = a.NotifyStep(context.Background(), NotifyInput{Channel: "telegram"})
	if err == nil || errors.As(err, &appErr) {
		t.Errorf("error %v, want a plain, retryable one", err)
	}
}

type hubFunc func(string, SSEEvent)

func (f hubFunc) Publish(id string, ev SSEEvent) { f(id, ev) }

func TestHubNotifier_PublishesOnTheSession(t *testing.T) {
	var gotID string
	n := HubNotifier{Hub: hubFunc(func(id string, _ SSEEvent) { gotID = id })}
	n.Notify(context.Background(), Notification{SessionID: "s1", Event: SSEEvent{Type: "message"}})
	if gotID != "s1" {
		t.Errorf("published on %q", gotID)
	}
}
