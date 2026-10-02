package tool

import (
	"encoding/json"
	"testing"
)

// What the agent saves about a user is shown to nobody in the transcript: in a
// shared session, every member sees the tool calls.
func TestDisplayInput_HidesMemory(t *testing.T) {
	in := json.RawMessage(`{"content":"Alice likes tea"}`)
	if got := string(DisplayInput("save_user_memory", in)); got != `{"content":"(private)"}` {
		t.Errorf("memory shown: %s", got)
	}
	if got := string(DisplayInput("web_fetch", json.RawMessage(`{"url":"x"}`))); got != `{"url":"x"}` {
		t.Errorf("other tool hidden: %s", got)
	}
}
