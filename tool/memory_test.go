package tool

import (
	"encoding/json"
	"testing"

	"github.com/victor/temporal-agent/store"
)

// What the agent saves about a user is shown to nobody in the transcript: in a
// shared session, every member sees the tool calls.
func TestDisplayInput_HidesMemory(t *testing.T) {
	r := NewRegistry()
	RegisterMemoryTools(r, nil)
	memory, _ := r.Get("save_user_memory")
	if !memory.PrivateInput {
		t.Fatal("save_user_memory is not marked private")
	}

	private := PrivateSetOf([]store.ToolRecord{{Name: "save_user_memory", PrivateInput: true}, {Name: "web_fetch"}})
	in := json.RawMessage(`{"content":"Alice likes tea"}`)
	if got := string(DisplayInput(private.PrivateInput("save_user_memory"), in)); got != `{"content":"(private)"}` {
		t.Errorf("memory shown: %s", got)
	}
	if got := string(DisplayInput(private.PrivateInput("web_fetch"), json.RawMessage(`{"url":"x"}`))); got != `{"url":"x"}` {
		t.Errorf("other tool hidden: %s", got)
	}
}

func TestWithCallContext(t *testing.T) {
	cc := CallContext{AgentChain: []string{"default", "analyst"}, Channel: "telegram", ChannelID: "42"}
	got, err := WithCallContext(json.RawMessage(`{"question":"ok?","channel":"forged","agent":"forged"}`), cc)
	if err != nil {
		t.Fatal(err)
	}
	var in struct {
		Question string `json:"question"`
		CallContext
	}
	json.Unmarshal(got, &in)
	if in.Question != "ok?" || in.Channel != "telegram" || in.ChannelID != "42" || len(in.AgentChain) != 2 || in.Agent != "" {
		t.Errorf("input %s", got)
	}
	if _, err := WithCallContext(json.RawMessage(`null`), cc); err != nil {
		t.Errorf("null input: %v", err)
	}
	if _, err := WithCallContext(json.RawMessage(`[1]`), cc); err == nil {
		t.Error("an array became an input object")
	}
}

func TestDecodeResult(t *testing.T) {
	for raw, want := range map[string]struct {
		content string
		isError bool
	}{
		`{"content":"done"}`:                 {"done", false},
		`{"content":"no","is_error":true}`:   {"no", true},
		`{"content":"","report":"x"}`:        {"", false},
		`"yes"`:                              {"yes", false},
		`{"other":1}`:                        {`{"other":1}`, false},
		`{"report":"r","content":"summary"}`: {"summary", false},
	} {
		c, e := DecodeResult(json.RawMessage(raw))
		if c != want.content || e != want.isError {
			t.Errorf("DecodeResult(%s) = %q, %v", raw, c, e)
		}
	}
}

// The properties of a tool are part of its contract: two workers disagreeing
// on them disagree on the tool.
func TestSchemaHash_CoversMetadata(t *testing.T) {
	a := &Tool{Name: "x", Description: "d", Kind: ToolKindActivity}
	b := *a
	b.Sensitive = true
	if a.SchemaHash() == b.SchemaHash() {
		t.Error("Sensitive does not change the hash")
	}
}
