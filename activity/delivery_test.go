package activity

import (
	"context"
	"errors"
	"testing"

	"github.com/victor/temporal-agent/store"
)

type appendedMessages struct{ keys []string }

func (a *appendedMessages) AppendMessage(_ context.Context, _, key string, _ store.Message) error {
	a.keys = append(a.keys, key)
	return nil
}

// A result that could not be shown live fails the delivery, so that it is
// retried; it is stored under the same key every time.
func TestDeliverResult_ReportsTheWebNotifier(t *testing.T) {
	st := &appendedMessages{}
	refused := errors.New("refused")
	a := &DeliveryActivities{Web: failingNotifier{refused}, Store: st}
	in := DeliverInput{UserID: "u1", Content: "done", ScheduleID: "sched-1", RunUnixMilli: 42}

	if err := a.DeliverResult(context.Background(), in); !errors.Is(err, refused) {
		t.Errorf("DeliverResult = %v, want the notifier's error", err)
	}
	a.Web = &recordingNotifier{}
	if err := a.DeliverResult(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if len(st.keys) != 2 || st.keys[0] != st.keys[1] {
		t.Errorf("stored under %v, want the same key twice", st.keys)
	}
	if got := a.Web.(*recordingNotifier).got; len(got) != 1 || got[0].SessionID != "notifications:u1" {
		t.Errorf("notified %+v", got)
	}
}
