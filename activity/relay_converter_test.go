package activity_test

import (
	"context"
	"encoding/json"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/workflow"
)

// startRecorder keeps the arguments of a SignalWithStart.
type startRecorder struct {
	signal any
	args   []any
}

func (r *startRecorder) SignalWithStartWorkflow(_ context.Context, _, _ string, signalArg interface{}, _ client.StartWorkflowOptions, _ interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	r.signal, r.args = signalArg, workflowArgs
	return nil, nil
}

// The relay's arguments cross Temporal's default data converter twice: the
// relaying workflow encodes RelayInput for the activity, and the client
// encodes the signal and the start input it is handed. The raw JSON goes
// through both as JSON, and decodes into the participant's own types.
func TestRelay_ThroughTheDataConverter(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	sent := workflow.ParticipantMessage{MessageID: 3, UserID: "u-alice", UserName: "Alice", EarlierTurns: []string{"m3.jarvis"},
		SignReply: true, Next: []workflow.AddressedAgent{{ID: "bob", Name: "Bob", Mention: "bob"}}}
	message, _ := json.Marshal(sent)
	start, _ := json.Marshal(workflow.ParticipantInput{SessionID: "s1", AgentID: "smith", Channel: "telegram", ChannelID: "42"})
	payload, err := dc.ToPayload(activity.RelayInput{WorkflowID: "s1:p:smith", WorkflowType: "ParticipantWorkflow", TaskQueue: "agent",
		Signal: workflow.SignalMessage, Message: message, Start: start})
	if err != nil {
		t.Fatal(err)
	}
	var in activity.RelayInput
	if err := dc.FromPayload(payload, &in); err != nil {
		t.Fatal(err)
	}

	r := &startRecorder{}
	if err := (&activity.RelayActivities{Client: r}).Relay(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	signal, err := dc.ToPayload(r.signal)
	if err != nil {
		t.Fatal(err)
	}
	if enc := string(signal.Metadata[converter.MetadataEncoding]); enc != converter.MetadataEncodingJSON {
		t.Errorf("the signal encoded as %q, want JSON", enc)
	}
	var got workflow.ParticipantMessage
	if err := dc.FromPayload(signal, &got); err != nil {
		t.Fatal(err)
	}
	if got.MessageID != 3 || got.UserName != "Alice" || !got.SignReply || len(got.EarlierTurns) != 1 || len(got.Next) != 1 || got.Next[0].Mention != "bob" {
		t.Errorf("the signal decodes as %+v, want %+v", got, sent)
	}
	args, err := dc.ToPayloads(r.args...)
	if err != nil {
		t.Fatal(err)
	}
	var input workflow.ParticipantInput
	if err := dc.FromPayloads(args, &input); err != nil || input.SessionID != "s1" || input.AgentID != "smith" || input.ChannelID != "42" {
		t.Errorf("the start input decodes as %+v (%v)", input, err)
	}
}
