// Package conversation turns a session's stored messages into what an agent's
// model reads: in an order the LLM API accepts, other agents' turns as text
// under their names, people's messages under theirs.
package conversation

import (
	"cmp"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// Label is how an agent is named to another one: its name, and the mention
// that calls it.
type Label struct {
	Name    string `json:"name"`
	Mention string `json:"mention"`
}

// View is what an agent needs to read the session's history: who it is, how
// to name the other agents, and which tool inputs their calls hide.
type View struct {
	Self    string
	Agents  map[string]Label   // the catalog's agents, by ID
	Private tool.PrivateInputs // tools whose input the members do not see; nil = none
}

// Bounds on what another agent's tool calls bring into the history: its
// answer carries what matters, its calls only show how it got there.
const (
	maxOtherToolInputBytes  = 500
	maxOtherToolResultBytes = 1500
)

// other reports whether m is the turn of another agent than the one reading.
// An assistant message without an agent predates agents' signatures: it is
// read as the reader's own.
func (v View) other(m store.Message) bool {
	return m.Role == store.RoleAssistant && m.AgentID != "" && m.AgentID != v.Self
}

// label names m's agent as the history shows it: "agent Jarvis (@jarvis)".
// An agent gone from the catalog keeps the name it signed with.
func (v View) label(m store.Message) string {
	if a, ok := v.Agents[m.AgentID]; ok {
		return AgentLabel(a.Name, a.Mention)
	}
	return AgentLabel(cmp.Or(m.Author, m.AgentID), "")
}

func (v View) privateInput(toolName string) bool {
	return v.Private != nil && v.Private.PrivateInput(toolName)
}

// AgentLabel is how an agent is named to another one, in the history and in
// the part note: by its name and the mention that calls it.
func AgentLabel(name, mention string) string {
	if mention == "" {
		return "agent " + name
	}
	return "agent " + name + " (@" + mention + ")"
}

// otherCall is a tool call another agent made: its result is shown with it.
type otherCall struct {
	tool, agent string
}

// Convert turns the conversation, in the order Order gives, into what the
// model of the agent view.Self reads.
//
// Another agent's turn is text the model reads, in a user message: its
// answer, its tool calls and their results, each under that agent's name.
// Kept as assistant messages, they would be the model's own words; their
// tool blocks would be rejected by an API request that defines no tool (an
// agent without any), and a conversation ending on them would have the model
// continue the other agent's answer instead of giving its own. The agent's
// own messages, and those signed by no agent, keep their tool blocks.
func Convert(messages []store.Message, view View) []provider.ChatMessage {
	// Tool results carry no agent: the others' are found by their calls.
	others := map[string]otherCall{}
	for _, m := range messages {
		if view.other(m) {
			for _, tc := range m.ToolCalls {
				others[tc.ID] = otherCall{tool: tc.Name, agent: view.label(m)}
			}
		}
	}

	result := make([]provider.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		if msg.Kind == store.KindTurnError {
			continue // for the members: the model is not told about its failures
		}
		if view.other(msg) {
			result = appendUserText(result, view.otherTurn(msg))
			continue
		}
		if msg.ToolResult != nil {
			if call, ok := others[msg.ToolResult.ToolCallID]; ok {
				result = appendUserText(result, otherResult(call, msg.ToolResult))
				continue
			}
		}
		content := json.RawMessage(msg.Content)
		if len(content) == 0 {
			content = nil
		}
		switch {
		case msg.Kind == store.KindForkSummary:
			content = asForkContext(content)
		case msg.Role == store.RoleUser && msg.Author != "":
			content = withAuthor(content, msg.Author)
		}
		cm := provider.ChatMessage{
			Role:    string(msg.Role),
			Content: content,
		}
		for _, tc := range msg.ToolCalls {
			cm.ToolCalls = append(cm.ToolCalls, provider.ToolCallInfo{
				ID:    tc.ID,
				Name:  tc.Name,
				Input: tc.Input,
			})
		}
		if msg.ToolResult != nil {
			cm.ToolResult = &provider.ToolResultInfo{
				ToolCallID: msg.ToolResult.ToolCallID,
				Content:    msg.ToolResult.Content,
				IsError:    msg.ToolResult.IsError,
			}
		}
		if text, ok := plainUserText(cm); ok {
			result = appendUserText(result, text)
			continue
		}
		result = append(result, cm)
	}
	return result
}

// otherTurn is another agent's assistant message as text: its answer, then
// each tool call, its input clipped, or hidden as the members see it.
func (v View) otherTurn(m store.Message) string {
	who := v.label(m)
	var lines []string
	if text := messageText(m.Content); text != "" {
		lines = append(lines, "["+who+"] "+text)
	}
	for _, tc := range m.ToolCalls {
		input := tool.DisplayInput(v.privateInput(tc.Name), tc.Input)
		lines = append(lines, fmt.Sprintf("[%s called %s %s]", who, tc.Name, Clip(cmp.Or(string(input), "{}"), maxOtherToolInputBytes)))
	}
	return strings.Join(lines, "\n")
}

// otherResult is the result of another agent's tool call as text, clipped.
func otherResult(call otherCall, r *store.ToolResult) string {
	what := "result of"
	if r.IsError {
		what = "error from"
	}
	return fmt.Sprintf("[%s %s, called by %s] %s", what, call.tool, call.agent, Clip(r.Content, maxOtherToolResultBytes))
}

// plainUserText returns the text of a user message that is text alone.
func plainUserText(cm provider.ChatMessage) (string, bool) {
	if cm.Role != string(store.RoleUser) || cm.ToolResult != nil || len(cm.ToolCalls) > 0 {
		return "", false
	}
	var text string
	if json.Unmarshal(cm.Content, &text) != nil {
		return "", false
	}
	return text, true
}

// appendUserText adds text as a user message, joined to the previous one when
// that is user text too: messages from several people and agents follow one
// another, and the API expects the roles to alternate.
func appendUserText(result []provider.ChatMessage, text string) []provider.ChatMessage {
	if text == "" {
		return result
	}
	if n := len(result); n > 0 {
		if prev, ok := plainUserText(result[n-1]); ok {
			text = prev + "\n\n" + text
			result = result[:n-1]
		}
	}
	content, _ := json.Marshal(text)
	return append(result, provider.ChatMessage{Role: string(store.RoleUser), Content: content})
}

// messageText returns a stored message's content as text: a JSON string, or
// the raw content for anything else.
func messageText(content string) string {
	var s string
	if json.Unmarshal([]byte(content), &s) == nil {
		return s
	}
	return content
}

// Clip shortens s to at most n bytes, on a rune boundary.
func Clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

// asForkContext presents a fork's starting summary to the model for what it
// is: context carried over, not something a user just said.
func asForkContext(content json.RawMessage) json.RawMessage {
	var text string
	if json.Unmarshal(content, &text) != nil {
		return content
	}
	framed, _ := json.Marshal("[Context carried over from an earlier conversation this one was forked from. A summary, not a message from the user.]\n\n" + text)
	return framed
}

// withAuthor prefixes a person's message with their name, so the model knows
// who speaks when a session has several users. Only the text sent to the
// model changes: the stored message keeps the author in its own field.
func withAuthor(content json.RawMessage, author string) json.RawMessage {
	var text string
	if json.Unmarshal(content, &text) != nil || text == "" {
		return content // not plain text, or none: leave it alone
	}
	prefixed, _ := json.Marshal("[" + author + "] " + text)
	return prefixed
}
