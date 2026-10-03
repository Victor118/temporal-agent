package store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type ToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type ToolResult struct {
	ToolCallID string `json:"tool_call_id"`
	Content    string `json:"content"`
	IsError    bool   `json:"is_error,omitempty"`
}

type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content,omitempty"`
	// UserID identifies who wrote a user message, AgentID which agent wrote
	// an assistant message (or the turn a KindTurnError ended): a session is
	// shared by several users, and several agents answer in it. Author is
	// the writer's name at the time of writing, user or agent; the interface
	// shows the agent's current name, Author only once the agent is gone.
	UserID  string `json:"user_id,omitempty"`
	AgentID string `json:"agent_id,omitempty"`
	Author  string `json:"author,omitempty"`
	// Kind marks a message the system wrote: KindForkSummary is the summary
	// a fork starts from, KindTurnError why a turn failed. Empty for an
	// ordinary message.
	Kind       string      `json:"kind,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
	ToolResult *ToolResult `json:"tool_result,omitempty"`
}

// KindForkSummary marks the first message of a fork: the summary of the
// parent session up to the message the fork started from.
const KindForkSummary = "fork_summary"

// KindTurnError marks why a turn failed, written after what the turn produced.
// It is for the session's members: the model never sees it.
const KindTurnError = "turn_error"

type MessageWithID struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	// Key is the message's idempotency key (msg_key): it tells which turn
	// wrote it (TurnOf). Never shown.
	Key string `json:"-"`
	Message
}

// The keys of a session's messages, by writer: a person ("msg:"), a
// scheduled task's result ("sched:"), and a turn ("{turn key}:{index}").

const (
	humanKeyPrefix     = "msg:"
	scheduledKeyPrefix = "sched:"
)

// HumanMessageKey is the idempotency key of a message a person wrote, id
// being unique.
func HumanMessageKey(id string) string { return humanKeyPrefix + id }

// TurnGroupKey names the turns answering one message: id is unique to the
// message, upTo the last message of the session they read besides their own
// (the snapshot they started from). A message stored after upTo while they
// ran is read after them (see TurnSnapshot).
func TurnGroupKey(id string, upTo int64) string {
	return fmt.Sprintf("%s@%d", id, upTo)
}

// TurnKey names the turn of the agent-th agent answering a message, in the
// group of turns that answer it (TurnGroupKey): they are read as one block.
func TurnKey(group string, agent int) string {
	return fmt.Sprintf("%s.%d", group, agent)
}

// TurnOf returns the key of the turn that wrote the message stored under
// msgKey; false for a message no turn wrote, a person's or a task result.
func TurnOf(msgKey string) (string, bool) {
	if strings.HasPrefix(msgKey, humanKeyPrefix) || strings.HasPrefix(msgKey, scheduledKeyPrefix) {
		return "", false
	}
	i := strings.LastIndexByte(msgKey, ':')
	if i <= 0 {
		return "", false
	}
	return msgKey[:i], true
}

// TurnGroup returns the group of turns a turn belongs to (see TurnKey): a
// turn key made otherwise is its own group.
func TurnGroup(turnKey string) string {
	if i := strings.LastIndexByte(turnKey, '.'); i > 0 {
		return turnKey[:i]
	}
	return turnKey
}

// TurnSnapshot returns the last message the turns of group read besides their
// own (see TurnGroupKey); false for a group named otherwise.
func TurnSnapshot(group string) (int64, bool) {
	i := strings.LastIndexByte(group, '@')
	if i < 0 {
		return 0, false
	}
	upTo, err := strconv.ParseInt(group[i+1:], 10, 64)
	return upTo, err == nil
}

// TurnMessageKey is the idempotency key of the index-th message produced by a
// conversation turn. turnKey identifies the turn globally, not just within one
// workflow run: a session whose workflow timed out is resumed under the same
// session ID with its turn counter back to zero, so a counter alone would make
// the new turn 1 collide with the old one and drop its messages.
func TurnMessageKey(turnKey string, index int) string {
	return fmt.Sprintf("%s:%d", turnKey, index)
}

// ScheduledMessageKey is the idempotency key of a scheduled task result. A cron
// schedule fires repeatedly under the same ID, so the run timestamp is part of
// the key: retries of one run dedupe, successive runs do not.
func ScheduledMessageKey(scheduleID string, runUnixMilli int64) string {
	return fmt.Sprintf("sched:%s:%d", scheduleID, runUnixMilli)
}
