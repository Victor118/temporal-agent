package store

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
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
	// an assistant message (or the turn a KindTurnEnd ended): a session is
	// shared by several users, and several agents answer in it. Author is
	// the writer's name at the time of writing, user or agent; the interface
	// shows the agent's current name, Author only once the agent is gone.
	// On an assistant message, UserID is the user the turn answered, not an
	// author: the agent's turn for another user hides its private tool
	// blocks (conversation.Convert). Empty on messages written before it was
	// set: read as today, in full.
	UserID  string `json:"user_id,omitempty"`
	AgentID string `json:"agent_id,omitempty"`
	Author  string `json:"author,omitempty"`
	// Kind marks a message the system wrote: KindForkSummary is the summary
	// a fork starts from, KindTurnEnd a turn's end, KindForkReport a
	// fork's report to its parent. Empty for an ordinary message.
	Kind       string      `json:"kind,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
	ToolResult *ToolResult `json:"tool_result,omitempty"`
	// Fork is the fork a KindForkReport comes from.
	Fork *ForkRef `json:"fork,omitempty"`
	// On an assistant message: the model that wrote it, what its call took
	// (Usage), and the machine that called it, when the turn's model ran on
	// its author's machine (docs/design/machine-llm.md); empty: the server's
	// key, or a message written before they were kept.
	Model     string `json:"model,omitempty"`
	Usage     *Usage `json:"usage,omitempty"`
	MachineID string `json:"machine_id,omitempty"`
	// Machine is that machine's name when it answered, for the thread.
	Machine string `json:"machine,omitempty"`
}

// Usage is what a call to the model took, in tokens (provider.Usage).
type Usage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens,omitempty"`
}

// ForkRef names the fork a report comes from: its session, its title when it
// reported, and the last of its messages the report covers.
type ForkRef struct {
	SessionID     string `json:"session_id"`
	Title         string `json:"title"`
	UpToMessageID int64  `json:"up_to_message_id"`
}

// KindForkSummary marks the first message of a fork: the summary of the
// parent session up to the message the fork started from.
const KindForkSummary = "fork_summary"

// KindTurnEnd marks a turn's end, written under TurnEndKey after what the
// turn produced, however it ended: done, failed, stopped, or before it
// wrote anything. Its content is why the turn failed, "" when it did not
// (TurnEndError). It tells the other participants the turn is whole
// (TurnReads) and a message delivered twice that it was answered. The
// members see it only when it carries an error; the model never does.
const KindTurnEnd = "turn_end"

// maxTurnErrorBytes bounds the error a turn's end keeps: an API error can
// carry a whole response body.
const maxTurnErrorBytes = 2000

// TurnEnd is the end of agentID's turn, failed for reason; "" for a turn
// that did not fail.
func TurnEnd(agentID, reason string) Message {
	m := Message{Role: RoleAssistant, Kind: KindTurnEnd, AgentID: agentID}
	if reason != "" {
		if len(reason) > maxTurnErrorBytes {
			cut := maxTurnErrorBytes
			for cut > 0 && !utf8.RuneStart(reason[cut]) {
				cut--
			}
			reason = reason[:cut] + "…"
		}
		b, _ := json.Marshal(reason)
		m.Content = string(b)
	}
	return m
}

// TurnEndError is why the turn m ends failed, "" when it did not, or when m
// is no turn's end.
func TurnEndError(m Message) string {
	var reason string
	if m.Kind != KindTurnEnd || json.Unmarshal([]byte(m.Content), &reason) != nil {
		return ""
	}
	return reason
}

// KindForkReport marks a fork's report, posted into its parent session by a
// member of both (UserID, Author): what the fork did, decided, changed from
// the plan, and left open. It calls no agent; it is read as any message is.
const KindForkReport = "fork_report"

type MessageWithID struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	// Key is the message's idempotency key (msg_key): it tells which turn
	// wrote it (TurnOf). Never shown.
	Key string `json:"-"`
	Message
}

// The keys of a session's messages, by writer: a person ("msg:"), a
// scheduled task's result ("sched:"), a fork's report ("report:"), a fork's
// summary (ForkSummaryKey), and a turn: "{turn key}:{index}" for what it
// wrote, "{turn key}:end" for its end (TurnEndKey).

const (
	humanKeyPrefix     = "msg:"
	scheduledKeyPrefix = "sched:"
	reportKeyPrefix    = "report:"
	turnEndSuffix      = "end"
)

// HumanMessageKey is the idempotency key of a message a person wrote, id
// being unique.
func HumanMessageKey(id string) string { return humanKeyPrefix + id }

// ForkReportKey is the idempotency key of a fork's report in its parent: the
// fork and the range of its messages the report covers, after from up to
// upTo. A retried post writes nothing more.
func ForkReportKey(forkSessionID string, from, upTo int64) string {
	return fmt.Sprintf("%s%s:%d-%d", reportKeyPrefix, forkSessionID, from, upTo)
}

// TurnKey names the turn of a participant answering a message:
// "m<message id>.<participant>". The message is the turn's anchor: the last
// message no turn wrote that it reads (TurnReads), and the one the
// conversation shows it after (conversation.Order). participant is the
// agent's ID, "i=<instance>" for an instance, "<agent>~btw" for an aside:
// none holds a dot or a colon. A participant answers a message once: its
// turn key is unique in the session, which is what deduplicates a message
// delivered twice (its end, TurnEndKey).
func TurnKey(messageID int64, participant string) string {
	return fmt.Sprintf("m%d.%s", messageID, participant)
}

// parseTurn reads a turn key: the anchor after "m", the participant after
// the dot, the only one. False for any other string.
func parseTurn(turn string) (anchor int64, participant string, ok bool) {
	rest, found := strings.CutPrefix(turn, "m")
	digitsOf, participant, dot := strings.Cut(rest, ".")
	if !found || !dot || !digits(digitsOf) {
		return 0, "", false
	}
	anchor, err := strconv.ParseInt(digitsOf, 10, 64)
	if err != nil || participant == "" || strings.ContainsAny(participant, ".:") {
		return 0, "", false
	}
	return anchor, participant, true
}

func digits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

// TurnOf returns the key of the turn that wrote the message stored under
// msgKey, its end included; false for a message no turn wrote: a person's, a
// task result, a fork's report or summary.
func TurnOf(msgKey string) (string, bool) {
	i := strings.LastIndexByte(msgKey, ':')
	if i <= 0 {
		return "", false
	}
	if suffix := msgKey[i+1:]; suffix != turnEndSuffix && !digits(suffix) {
		return "", false
	}
	turn := msgKey[:i]
	if _, _, ok := parseTurn(turn); !ok {
		return "", false
	}
	return turn, true
}

// TurnAnchor returns the message a turn answers (see TurnKey); false for a
// string that is no turn key.
func TurnAnchor(turnKey string) (int64, bool) {
	anchor, _, ok := parseTurn(turnKey)
	return anchor, ok
}

// TurnParticipant returns the participant whose turn turnKey is (see
// TurnKey); "" for a string that is no turn key.
func TurnParticipant(turnKey string) string {
	_, participant, _ := parseTurn(turnKey)
	return participant
}

// TurnEndKey is the key of a turn's end (KindTurnEnd): out of the indexes
// of its messages, which a write ignores once taken (AppendMessages), so an
// end written after a turn that stored message 0 is never absorbed by it.
func TurnEndKey(turnKey string) string { return turnKey + ":" + turnEndSuffix }

// IsTurnEnd reports whether msgKey is a turn's end.
func IsTurnEnd(msgKey string) bool {
	turn, ok := TurnOf(msgKey)
	return ok && msgKey == TurnEndKey(turn)
}

// TurnScope is what a turn is: the message it answers, its participant, and
// the turns it reads whole whatever their state: its own and, on a relay,
// those that answered the message before it (EarlierTurns).
type TurnScope struct {
	UpTo        int64
	Participant string
	Turns       []string
}

// ScopeOf is the scope of the turn turnKey, which reads earlier: the turns
// that answered its message before it (a relay).
func ScopeOf(turnKey string, earlier []string) TurnScope {
	anchor, participant, _ := parseTurn(turnKey)
	return TurnScope{UpTo: anchor, Participant: participant, Turns: append(slices.Clone(earlier), turnKey)}
}

// TurnReads reports whether a turn of scope reads the message stored as id
// under key, ends being the ID of each turn's end (TurnEndIDs). Participants
// answer in parallel, each its messages one at a time; a turn reads:
//  1. a message no turn wrote, up to the one it answers (UpTo): a person's,
//     the fork's summary, a report, a task result. One stored after it gets
//     its own turn;
//  2. its own turn and those of scope.Turns (the relay), whole;
//  3. its participant's turns anchored up to UpTo, whole, even written after
//     it: by anchor, not by the order they ran in, since a relay can bring
//     an older message after a newer one was answered;
//  4. another participant's turns whole or not at all: those that ended
//     (their end's ID) by UpTo. One still running, or that never ended, is
//     never read half written: a tool call without its result is refused by
//     the LLM API.
//
// A turn's end is never read: it is for the members.
//
// Known race, left alone: IDs are not in commit order. A message stored a
// few milliseconds before M can take its ID before M's and commit after M's
// turn began: the turn's next call reads it, before the turn's own
// messages (the prefix changes, the cache misses). Rare, and no request
// the API refuses; it would take an order by commit.
func TurnReads(id int64, key string, scope TurnScope, ends map[string]int64) bool {
	turn, ok := TurnOf(key)
	switch {
	case !ok:
		return id <= scope.UpTo
	case IsTurnEnd(key):
		return false
	case slices.Contains(scope.Turns, turn):
		return true
	}
	anchor, participant, _ := parseTurn(turn)
	if anchor > scope.UpTo {
		return false
	}
	if participant == scope.Participant {
		return true
	}
	end, ended := ends[turn]
	return ended && end <= scope.UpTo
}

// TurnEndIDs returns the ID of each turn's end among msgs, by turn: what
// TurnReads needs to know of the turns it does not read whole.
func TurnEndIDs(msgs []MessageWithID) map[string]int64 {
	ends := map[string]int64{}
	for _, m := range msgs {
		if IsTurnEnd(m.Key) {
			turn, _ := TurnOf(m.Key)
			ends[turn] = m.ID
		}
	}
	return ends
}

// TurnMessageKey is the idempotency key of the index-th message produced by a
// turn (TurnKey): a rewrite of the same message writes nothing more.
func TurnMessageKey(turnKey string, index int) string {
	return fmt.Sprintf("%s:%d", turnKey, index)
}

// IsScheduledResult reports whether msgKey is a scheduled task's result
// (ScheduledMessageKey).
func IsScheduledResult(msgKey string) bool { return strings.HasPrefix(msgKey, scheduledKeyPrefix) }

// ScheduledMessageKey is the idempotency key of a scheduled task result. A cron
// schedule fires repeatedly under the same ID, so the run timestamp is part of
// the key: retries of one run dedupe, successive runs do not.
func ScheduledMessageKey(scheduleID string, runUnixMilli int64) string {
	return fmt.Sprintf("%s%s:%d", scheduledKeyPrefix, scheduleID, runUnixMilli)
}
