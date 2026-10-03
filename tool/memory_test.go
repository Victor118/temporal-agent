package tool

import (
	"context"
	"encoding/json"
	"strings"
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
	got, err := WithCallContext(json.RawMessage(`{"question":"ok?","channel":"forged","agent":"forged","memory_version":99}`), cc)
	if err != nil {
		t.Fatal(err)
	}
	var in struct {
		Question string `json:"question"`
		CallContext
	}
	json.Unmarshal(got, &in)
	if in.Question != "ok?" || in.Channel != "telegram" || in.ChannelID != "42" || len(in.AgentChain) != 2 || in.Agent != "" || in.MemoryVersion != nil {
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

// fakeSaver is a user's memory, saved only over the version expected.
type fakeSaver struct {
	memory store.Memory
	saves  int
}

func (s *fakeSaver) SaveMemory(_ context.Context, _ store.MemoryScope, _ string, content string, expected int64) (int64, error) {
	s.saves++
	if s.memory.Version != expected {
		return 0, &store.MemoryConflict{Current: s.memory}
	}
	s.memory = store.Memory{Content: content, Version: expected + 1}
	return s.memory.Version, nil
}

// save_user_memory replaces the version the model read, and only it: a save
// from another, or from none, is refused with what the model needs to do.
func TestSaveUserMemory(t *testing.T) {
	saver := &fakeSaver{memory: store.Memory{Content: "likes tea; lives in Lyon", Version: 4}}
	r := NewRegistry()
	RegisterMemoryTools(r, saver)
	memory, _ := r.Get("save_user_memory")
	if !memory.NeedsCallContext || !memory.PrivateInput {
		t.Fatalf("save_user_memory: NeedsCallContext %v, PrivateInput %v", memory.NeedsCallContext, memory.PrivateInput)
	}
	save := func(ctx context.Context) (string, error) {
		return memory.Execute(ctx, json.RawMessage(`{"content":"likes tea and coffee"}`))
	}
	alice := WithUserID(context.Background(), "u-alice")
	read := func(v int64) context.Context { return WithCall(alice, CallContext{MemoryVersion: &v}) }

	// Read at 3, changed since by another session: nothing saved, and the
	// model is given the memory to merge into.
	_, err := save(read(3))
	if err == nil || !strings.Contains(err.Error(), "changed elsewhere") ||
		!strings.Contains(err.Error(), "likes tea; lives in Lyon") || !strings.Contains(err.Error(), "call save_user_memory again") {
		t.Errorf("conflict: %v", err)
	}
	if saver.memory.Version != 4 {
		t.Errorf("a conflicting save wrote: %+v", saver.memory)
	}

	// No version: the prompt held no memory (a sub-agent), or no LLM call
	// was made. Refused before the store.
	saves := saver.saves
	for name, ctx := range map[string]context.Context{
		"no call context": alice,
		"no version":      WithCall(alice, CallContext{Channel: "web"}),
	} {
		if _, err := save(ctx); err == nil || !strings.Contains(err.Error(), "not in your prompt") {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := save(WithCall(context.Background(), CallContext{MemoryVersion: new(int64)})); err == nil || !strings.Contains(err.Error(), "user not identified") {
		t.Errorf("no user: %v", err)
	}
	if saver.saves != saves {
		t.Error("a blind save reached the store")
	}

	if got, err := save(read(4)); err != nil || got != "Memory saved." || saver.memory != (store.Memory{Content: "likes tea and coffee", Version: 5}) {
		t.Errorf("save from the current version: %q, %v; memory %+v", got, err, saver.memory)
	}
}

// The result of a private tool is shown as its input is: a conflict carries
// the memory.
func TestDisplayResult_HidesAPrivateResult(t *testing.T) {
	if got := DisplayResult(true, "Current version: Alice likes tea"); got != "(private)" {
		t.Errorf("private result shown: %q", got)
	}
	if got := DisplayResult(false, "page"); got != "page" {
		t.Errorf("result hidden: %q", got)
	}
}
