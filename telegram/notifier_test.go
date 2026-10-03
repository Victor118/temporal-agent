package telegram

import (
	"context"
	"testing"

	"github.com/victor/temporal-agent/activity"
)

type sent struct{ chat, text string }

type fakeSender struct{ sent []sent }

func (f *fakeSender) SendMessage(_ context.Context, chatID, text string) error {
	f.sent = append(f.sent, sent{chatID, text})
	return nil
}

func TestNotifier_AnswersAndQuestionsOnly(t *testing.T) {
	s := &fakeSender{}
	n := &Notifier{Client: s}
	for _, ev := range []activity.SSEEvent{
		{Type: "message", Data: []byte(`{"type":"message","content":"the answer"}`)},
		{Type: "ask_user", Data: []byte(`{"type":"ask_user","question":"ok?"}`)},
		{Type: "tool_calls", Data: []byte(`{"tool_calls":[]}`)},
		{Type: "message", Data: []byte(`{"content":""}`)},
	} {
		if err := n.Notify(context.Background(), activity.Notification{SessionID: "s1", ChannelID: "42", Event: ev}); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.sent) != 2 || s.sent[0] != (sent{"42", "the answer"}) || s.sent[1] != (sent{"42", "❓ ok?"}) {
		t.Errorf("sent %+v", s.sent)
	}
}

// Several agents answer in the session: a signed answer starts with its
// agent's name, so the reader knows who speaks.
func TestNotifier_SignsTheAnswer(t *testing.T) {
	s := &fakeSender{}
	n := &Notifier{Client: s}
	ev := activity.SSEEvent{Type: "message", Data: []byte(`{"type":"message","content":"Utile, oui.","agent":"Agent Smith"}`)}
	if err := n.Notify(context.Background(), activity.Notification{SessionID: "s1", ChannelID: "42", Event: ev}); err != nil {
		t.Fatal(err)
	}
	if len(s.sent) != 1 || s.sent[0] != (sent{"42", "Agent Smith :\nUtile, oui."}) {
		t.Errorf("sent %+v", s.sent)
	}
}
