package gateway

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// A call to the model is never sent again nor carried on at a hello: its
// request is gone. Lost, whatever the machine says of it; what it lists is
// cancelled, or its result dropped.
func TestReconcile_LLM(t *testing.T) {
	open := []store.Directive{
		{ID: "llm-running", Kind: machine.KindLLM, State: store.DirectiveRunning, SentConn: "c-old"},
		{ID: "llm-finished", Kind: machine.KindLLM, State: store.DirectiveRunning, SentConn: "c-old"},
		{ID: "llm-unsent", Kind: machine.KindLLM, State: store.DirectiveRunning},
		{ID: "run", Kind: machine.KindAnalyzeRepo, State: store.DirectiveRunning, SentConn: "c-old"},
	}
	p := reconcile(open, []string{"llm-running", "run"}, []string{"llm-finished"}, "c-now")
	if !slices.Equal(ids(p.lost), []string{"llm-running", "llm-finished", "llm-unsent"}) || len(p.send) != 0 ||
		!slices.Equal(ids(p.attach), []string{"run"}) || !slices.Equal(p.cancel, []string{"llm-running"}) || !slices.Equal(p.ack, []string{"llm-finished"}) {
		t.Errorf("plan %+v", p)
	}
}

// An answer that breaks the rules becomes a failure the gateway writes in
// its place; a sound one passes as it is.
func TestCheckLLMResult(t *testing.T) {
	good := machine.Message{Type: machine.TypeResult, ID: "d", Status: machine.StatusOK,
		Output: json.RawMessage(`{"content":"hi","stop_reason":"end_turn"}`)}
	if got := checkLLMResult(good); string(got.Output) != string(good.Output) || got.Status != machine.StatusOK {
		t.Errorf("good: %+v", got)
	}
	for name, m := range map[string]machine.Message{
		"bad answer":         {ID: "d", Status: machine.StatusOK, Output: json.RawMessage(`{"stop_reason":"whatever"}`)},
		"unreadable failure": {ID: "d", Status: machine.StatusError, Output: json.RawMessage(`[1]`)},
		"huge failure":       {ID: "d", Status: machine.StatusError, Output: json.RawMessage(`"` + strings.Repeat("x", machine.MaxMessageBytes) + `"`)},
	} {
		got := checkLLMResult(m)
		var f machine.LLMFailure
		if got.Status != machine.StatusError || json.Unmarshal(got.Output, &f) != nil || f.Type != machine.LLMFailBadResult ||
			!strings.Contains(got.Error, "refused") || got.Type != machine.TypeResult || got.ID != "d" {
			t.Errorf("%s: %+v", name, got)
		}
	}
}

// What the workflow reads of a call to the model: the answer with the
// prompt's memory from the database, or a typed error.
func TestLLMCompletion(t *testing.T) {
	d := store.Directive{ID: "d", Kind: machine.KindLLM, Input: json.RawMessage(`{"memory_version":7}`)}
	res, err := llmCompletion(d, machine.Message{Status: machine.StatusOK,
		Output: json.RawMessage(`{"content":"hi","stop_reason":"end_turn","model":"m","usage":{"input_tokens":5,"output_tokens":1}}`)})
	r, ok := res.(machine.LLMResult)
	if err != nil || !ok || r.Content != "hi" || r.Model != "m" || r.Usage == nil || r.Usage.InputTokens != 5 ||
		r.MemoryVersion == nil || *r.MemoryVersion != 7 {
		t.Fatalf("ok: %+v %v", res, err)
	}
	if _, err := llmCompletion(d, machine.Message{Status: machine.StatusCanceled}); !temporal.IsCanceledError(err) {
		t.Errorf("canceled: %v", err)
	}
	fail := func(f machine.LLMFailure) machine.Message {
		out, _ := json.Marshal(f)
		return machine.Message{Status: machine.StatusError, Error: "boom", Output: out}
	}
	for name, c := range map[string]struct {
		m   machine.Message
		typ string
	}{
		"retry after":   {fail(machine.LLMFailure{Type: machine.LLMFailRetryAfter, RetryAfterMS: 30000}), machine.ErrTypeRetryAfter},
		"too long":      {fail(machine.LLMFailure{Type: machine.LLMFailContextTooLong}), machine.ErrTypeContextTooLong},
		"permanent":     {fail(machine.LLMFailure{Type: machine.LLMFailPermanent}), machine.ErrTypePermanentAPI},
		"credentials":   {fail(machine.LLMFailure{Type: machine.LLMFailCredentials}), machine.ErrTypePermanentAPI},
		"bad result":    {fail(machine.LLMFailure{Type: machine.LLMFailBadResult}), machine.ErrTypeBadResult},
		"other failure": {machine.Message{Status: machine.StatusError, Error: "network"}, machine.ErrTypeFailed},
		"stopping":      {machine.Message{Status: machine.StatusStopping}, machine.ErrTypeStopping},
		"refused":       {machine.Message{Status: machine.StatusRefused, Error: "full"}, machine.ErrTypeRefused},
	} {
		_, err := llmCompletion(d, c.m)
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || appErr.Type() != c.typ {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if c.typ == machine.ErrTypeRetryAfter {
			var f machine.LLMFailure
			if appErr.Details(&f) != nil || f.RetryAfter() != 30*time.Second {
				t.Errorf("retry after: details %+v", f)
			}
		}
	}
}
