package activity

import (
	"context"
	"testing"
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
