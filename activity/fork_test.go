package activity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

func text(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestBuildTranscript(t *testing.T) {
	long := strings.Repeat("x", 5000)
	got, truncated := buildTranscript([]store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: text("earlier summary")}},
		{ID: 2, Message: store.Message{Role: store.RoleUser, Content: text("hello"), Author: "Alice"}},
		{ID: 3, Message: store.Message{Role: store.RoleAssistant, Content: text("let me look"),
			ToolCalls: []store.ToolCall{{Name: "read_file", Input: json.RawMessage(`{"path":"` + long + `"}`)}}}},
		{ID: 4, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{Content: long}}},
		{ID: 5, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{Content: "boom", IsError: true}}},
		{ID: 6, Message: store.Message{Role: store.RoleAssistant,
			ToolCalls: []store.ToolCall{{Name: "save_user_memory", Input: json.RawMessage(`{"content":"Alice's secret"}`)}}}},
	})
	if truncated {
		t.Error("a short conversation reported truncated")
	}
	for _, want := range []string{
		"[Summary of an earlier conversation this one continued]\nearlier summary",
		"User (Alice): hello",
		"Assistant: let me look\nAssistant called read_file {\"path\":\"xxx",
		"Tool error: boom",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("transcript lacks %q", want)
		}
	}
	// A user's memory is private: the summary may go to another user's fork.
	if strings.Contains(got, "Alice's secret") {
		t.Error("the transcript carries a user's memory")
	}
	// Tool inputs and results are clipped: they are most of a transcript and
	// little of what a summary needs.
	if strings.Count(got, "x") > maxSummaryToolInputBytes+maxSummaryToolResultBytes {
		t.Errorf("tool content not clipped: %d bytes of it", strings.Count(got, "x"))
	}
}

func TestBuildTranscript_KeepsTheEnd(t *testing.T) {
	var msgs []store.MessageWithID
	turn := strings.Repeat("y", 1000)
	for i := 0; i < 1000; i++ { // 1MB, over the cap
		msgs = append(msgs, store.MessageWithID{ID: int64(i + 1), Message: store.Message{Role: store.RoleUser, Content: text(turn)}})
	}
	msgs = append(msgs, store.MessageWithID{ID: 1001, Message: store.Message{Role: store.RoleUser, Content: text("the last word")}})

	got, truncated := buildTranscript(msgs)
	if !truncated || !strings.HasPrefix(got, "[The beginning of the conversation is omitted") {
		t.Error("an oversized conversation must say its beginning is cut")
	}
	if !strings.HasSuffix(got, "User: the last word") {
		t.Error("the end of the conversation must be kept")
	}
	if len(got) > maxSummaryTranscriptBytes+200 {
		t.Errorf("transcript of %d bytes, over the cap", len(got))
	}
}

type forkStore struct {
	msgs []store.MessageWithID
}

func (f *forkStore) LoadMessagesUpTo(_ context.Context, _ string, lastID int64) ([]store.MessageWithID, error) {
	var out []store.MessageWithID
	for _, m := range f.msgs {
		if m.ID <= lastID {
			out = append(out, m)
		}
	}
	return out, nil
}

type fakeLLM struct {
	reply string
	seen  provider.ChatRequest
}

func (f *fakeLLM) Chat(_ context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	f.seen = req
	return provider.ChatResponse{Content: f.reply}, nil
}

func TestSummarizeConversation(t *testing.T) {
	st := &forkStore{msgs: []store.MessageWithID{
		{ID: 10, Message: store.Message{Role: store.RoleUser, Content: text("first question")}},
		{ID: 11, Message: store.Message{Role: store.RoleAssistant, Content: text("first answer")}},
		{ID: 12, Message: store.Message{Role: store.RoleUser, Content: text("after the fork point")}},
	}}
	llm := &fakeLLM{reply: "  The summary.  "}
	a := &ForkActivities{Store: st, LLM: llm}

	out, err := a.SummarizeConversation(context.Background(), SummarizeConversationInput{SessionID: "p", UpToMessageID: 11, Model: "small"})
	if err != nil || out.Summary != "The summary." {
		t.Fatalf("summary %q, %v", out.Summary, err)
	}
	var sent string
	json.Unmarshal(llm.seen.Messages[0].Content, &sent)
	// Up to the fork's message, not past it.
	if !strings.Contains(sent, "first answer") || strings.Contains(sent, "after the fork point") || llm.seen.Model != "small" {
		t.Errorf("sent %q to model %q", sent, llm.seen.Model)
	}

	// A message that is not in the session is a permanent failure: retrying
	// will not make it appear.
	_, err = a.SummarizeConversation(context.Background(), SummarizeConversationInput{SessionID: "p", UpToMessageID: 99})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Errorf("missing message: %v", err)
	}

	llm.reply = "   "
	if _, err := a.SummarizeConversation(context.Background(), SummarizeConversationInput{SessionID: "p", UpToMessageID: 11}); err == nil {
		t.Error("an empty summary must be an error")
	}
}
