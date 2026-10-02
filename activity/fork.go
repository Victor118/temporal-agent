package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// Bounds on what a summary reads. Tool calls and results are the bulk of a
// transcript and the least of what matters for a summary, so they are cut
// short; the whole is capped below what the model can read in one request.
const (
	maxSummaryTranscriptBytes = 300_000
	maxSummaryToolInputBytes  = 300
	maxSummaryToolResultBytes = 600
	maxSummaryTokens          = 4096
)

const summarySystemPrompt = `You summarize a conversation between users and an AI assistant, so that a new conversation can continue from where this one stops.

Write the summary in the language of the conversation. Keep:
- the goals pursued, and where each stands;
- the decisions taken, and why;
- the facts established, results obtained and conclusions reached;
- the questions still open, and what was about to happen next;
- every concrete reference: repositories, branches, files, URLs, names, figures, commands;
- the preferences the users expressed about how to work.

Drop greetings, small talk and dead ends that led nowhere. Name who said what when several users take part. Never add anything the conversation does not contain. Write a structured note, not a narrative; no preamble.`

// TranscriptReader reads a conversation up to one of its messages.
type TranscriptReader interface {
	LoadMessagesUpTo(ctx context.Context, sessionID string, lastID int64) ([]store.MessageWithID, error)
}

// ForkActivities produce the summary a forked session starts from.
type ForkActivities struct {
	Store TranscriptReader
	LLM   provider.LLMProvider
	// Private tells which tool inputs stay out of the summary: it may go to
	// another user's fork. Without it, every tool input does.
	Private tool.PrivateInputs
}

type SummarizeConversationInput struct {
	SessionID     string `json:"session_id"`
	UpToMessageID int64  `json:"up_to_message_id"`
	Model         string `json:"model,omitempty"` // empty = the worker's default
}

type SummarizeConversationOutput struct {
	Summary string `json:"summary"`
	// Truncated: the conversation was too long, and its beginning is not in
	// the summary.
	Truncated bool `json:"truncated,omitempty"`
}

// SummarizeConversation summarizes a session up to a message. It loads the
// conversation itself: a transcript can weigh hundreds of kilobytes, and
// passing it through the workflow would put it in its history.
func (a *ForkActivities) SummarizeConversation(ctx context.Context, in SummarizeConversationInput) (SummarizeConversationOutput, error) {
	msgs, err := a.Store.LoadMessagesUpTo(ctx, in.SessionID, in.UpToMessageID)
	if err != nil {
		return SummarizeConversationOutput{}, err
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].ID != in.UpToMessageID {
		return SummarizeConversationOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("message %d not found in session %s", in.UpToMessageID, in.SessionID), "MessageNotFound", nil)
	}

	transcript, truncated := buildTranscript(msgs, a.Private)
	content, _ := json.Marshal("Conversation to summarize:\n\n" + transcript)
	resp, err := a.LLM.Chat(ctx, provider.ChatRequest{
		Model:     in.Model,
		System:    summarySystemPrompt,
		Messages:  []provider.ChatMessage{{Role: "user", Content: content}},
		MaxTokens: maxSummaryTokens,
	})
	if err != nil {
		var permErr *provider.PermanentAPIError
		if errors.As(err, &permErr) {
			return SummarizeConversationOutput{}, temporal.NewNonRetryableApplicationError(err.Error(), "PermanentAPIError", err)
		}
		return SummarizeConversationOutput{}, err
	}
	summary := strings.TrimSpace(resp.Content)
	if summary == "" {
		return SummarizeConversationOutput{}, errors.New("the model returned an empty summary")
	}
	return SummarizeConversationOutput{Summary: summary, Truncated: truncated}, nil
}

// buildTranscript renders messages as plain text for the summarizer, keeping
// the end when the whole does not fit: the latest turns are what the fork
// continues from.
func buildTranscript(msgs []store.MessageWithID, private tool.PrivateInputs) (string, bool) {
	var entries []string
	for _, m := range msgs {
		if e := transcriptEntry(m.Message, private); e != "" {
			entries = append(entries, e)
		}
	}

	const sep = "\n\n"
	size, start := 0, len(entries)
	for start > 0 && size+len(entries[start-1])+len(sep) <= maxSummaryTranscriptBytes {
		start--
		size += len(entries[start]) + len(sep)
	}
	truncated := start > 0
	if truncated {
		entries = append([]string{"[The beginning of the conversation is omitted: too long.]"}, entries[start:]...)
	}
	return strings.Join(entries, sep), truncated
}

func transcriptEntry(m store.Message, private tool.PrivateInputs) string {
	text := decodeText(m.Content)
	switch {
	case m.Kind == store.KindForkSummary:
		// The parent was itself a fork: its starting summary is context too.
		return "[Summary of an earlier conversation this one continued]\n" + text
	case m.Role == store.RoleUser && m.Author != "":
		return "User (" + m.Author + "): " + text
	case m.Role == store.RoleUser:
		return "User: " + text
	case m.Role == store.RoleAssistant:
		var parts []string
		if text != "" {
			parts = append(parts, "Assistant: "+text)
		}
		for _, tc := range m.ToolCalls {
			// Shown as the session's members see it: a user's memory stays out
			// of the summary, which may go to someone else's fork.
			input := tool.DisplayInput(private == nil || private.PrivateInput(tc.Name), tc.Input)
			parts = append(parts, "Assistant called "+tc.Name+" "+clip(string(input), maxSummaryToolInputBytes))
		}
		return strings.Join(parts, "\n")
	case m.ToolResult != nil:
		label := "Tool result"
		if m.ToolResult.IsError {
			label = "Tool error"
		}
		return label + ": " + clip(m.ToolResult.Content, maxSummaryToolResultBytes)
	}
	return ""
}

// decodeText returns a message's content as text: it is stored as a JSON
// string, or as raw JSON for anything else.
func decodeText(content string) string {
	var s string
	if json.Unmarshal([]byte(content), &s) == nil {
		return s
	}
	return content
}

// clip shortens s to at most n bytes, on a rune boundary.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
