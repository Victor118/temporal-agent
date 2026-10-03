package activity

import (
	"cmp"
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

const summarySystemPrompt = `You summarize a conversation between users and one or more AI assistants, so that a new conversation can continue from where this one stops.

Write the summary in the language of the conversation. Keep:
- the goals pursued, and where each stands;
- the decisions taken, and why;
- the facts established, results obtained and conclusions reached;
- the questions still open, and what was about to happen next;
- every concrete reference: repositories, branches, files, URLs, names, figures, commands;
- the preferences the users expressed about how to work.

Drop greetings, small talk and dead ends that led nowhere. Name who said what when several users or assistants take part. Never add anything the conversation does not contain. Write a structured note, not a narrative; no preamble.`

// summaryPurposePrompt is added to summarySystemPrompt when the fork has a
// purpose. The purpose itself goes with the conversation, in the request: it
// is a member's words, not instructions.
const summaryPurposePrompt = `

The new conversation has a goal, stated before the conversation. Summarize for that goal: keep in full what bears on it — the specifications, requirements, decisions and constraints that apply to it, and the questions still open about it — and quote them word for word wherever the wording matters (names, formats, interfaces, figures, acceptance criteria). Cover the rest of the conversation briefly, as context.`

// TranscriptReader reads a conversation up to one of its messages.
type TranscriptReader interface {
	LoadMessagesUpTo(ctx context.Context, sessionID string, lastID int64) ([]store.MessageWithID, error)
}

// ForkActivities produce the summary a forked session starts from.
type ForkActivities struct {
	Store TranscriptReader
	LLM   provider.LLMProvider
	// Private tells which tool inputs, and their results, stay out of the
	// summary: it may go to another user's fork. Without it, every one does.
	Private tool.PrivateInputs
}

type SummarizeConversationInput struct {
	SessionID     string `json:"session_id"`
	UpToMessageID int64  `json:"up_to_message_id"`
	// Purpose is what the fork is for, in a member's words: the summary
	// keeps what matters for it. Empty = a general summary.
	Purpose string `json:"purpose,omitempty"`
	Model   string `json:"model,omitempty"` // empty = the worker's default
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
	system, request := summarySystemPrompt, "Conversation to summarize:\n\n"+transcript
	if in.Purpose != "" {
		system += summaryPurposePrompt
		request = "Goal of the new conversation: " + in.Purpose + "\n\n" + request
	}
	summary, err := a.summarize(ctx, in.Model, system, request)
	if err != nil {
		return SummarizeConversationOutput{}, err
	}
	return SummarizeConversationOutput{Summary: summary, Truncated: truncated}, nil
}

// summarize has the model write what system asks of request. A request the
// API refuses for good is not retried.
func (a *ForkActivities) summarize(ctx context.Context, model, system, request string) (string, error) {
	content, _ := json.Marshal(request)
	resp, err := a.LLM.Chat(ctx, provider.ChatRequest{
		Model:     model,
		System:    system,
		Messages:  []provider.ChatMessage{{Role: "user", Content: content}},
		MaxTokens: maxSummaryTokens,
	})
	if err != nil {
		var permErr *provider.PermanentAPIError
		if errors.As(err, &permErr) {
			return "", temporal.NewNonRetryableApplicationError(err.Error(), "PermanentAPIError", err)
		}
		return "", err
	}
	summary := strings.TrimSpace(resp.Content)
	if summary == "" {
		return "", errors.New("the model returned an empty summary")
	}
	return summary, nil
}

// buildTranscript renders messages as plain text for the summarizer, keeping
// the end when the whole does not fit: the latest turns are what the fork
// continues from.
func buildTranscript(msgs []store.MessageWithID, private tool.PrivateInputs) (string, bool) {
	// Shown as the session's members see them: a user's memory stays out of
	// the summary, which may go to someone else's fork. Unknown, every tool
	// is private. A result is the call's: found by its ID, the call before it;
	// a result whose call is not in the transcript is private too.
	isPrivate := func(name string) bool { return private == nil || private.PrivateInput(name) }
	callTools := map[string]string{}
	var entries []string
	for _, m := range msgs {
		for _, tc := range m.ToolCalls {
			callTools[tc.ID] = tc.Name
		}
		privateResult := false
		if m.ToolResult != nil {
			name, found := callTools[m.ToolResult.ToolCallID]
			privateResult = !found || isPrivate(name)
		}
		if e := transcriptEntry(m.Message, isPrivate, privateResult); e != "" {
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

// transcriptEntry is m as the summarizer reads it. isPrivate tells which
// tools' inputs are hidden; privateResult, whether m's result is.
func transcriptEntry(m store.Message, isPrivate func(tool string) bool, privateResult bool) string {
	text := decodeText(m.Content)
	switch {
	case m.Kind == store.KindTurnError:
		return ""
	case m.Kind == store.KindForkSummary:
		// The parent was itself a fork: its starting summary is context too.
		return "[Summary of an earlier conversation this one continued]\n" + text
	case m.Kind == store.KindForkReport:
		title := "untitled"
		if m.Fork != nil && m.Fork.Title != "" {
			title = m.Fork.Title
		}
		return "[Report from fork « " + title + " », posted by " + cmp.Or(m.Author, "a member") + "]\n" + text
	case m.Role == store.RoleUser && m.Author != "":
		return "User (" + m.Author + "): " + text
	case m.Role == store.RoleUser:
		return "User: " + text
	case m.Role == store.RoleAssistant:
		// Several agents may answer in a session: each is named.
		who := "Assistant"
		if m.Author != "" {
			who += " (" + m.Author + ")"
		}
		var parts []string
		if text != "" {
			parts = append(parts, who+": "+text)
		}
		for _, tc := range m.ToolCalls {
			input := tool.DisplayInput(isPrivate(tc.Name), tc.Input)
			parts = append(parts, who+" called "+tc.Name+" "+clip(string(input), maxSummaryToolInputBytes))
		}
		return strings.Join(parts, "\n")
	case m.ToolResult != nil:
		label := "Tool result"
		if m.ToolResult.IsError {
			label = "Tool error"
		}
		return label + ": " + clip(tool.DisplayResult(privateResult, m.ToolResult.Content), maxSummaryToolResultBytes)
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
