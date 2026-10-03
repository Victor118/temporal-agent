package store

import (
	"errors"
	"time"
)

// Roles a user can hold. (Role and RoleUser name message roles.)
const (
	UserRoleAdmin    = "admin"
	UserRoleStandard = "user"
)

var (
	ErrUserExists   = errors.New("a user with this email already exists")
	ErrUserNotFound = errors.New("user not found")
)

type User struct {
	ID           string     `json:"id"`
	Email        string     `json:"email"`
	DisplayName  string     `json:"display_name"`
	PasswordHash string     `json:"-"`
	Role         string     `json:"role"`
	TelegramID   *int64     `json:"telegram_id,omitempty"`
	DisabledAt   *time.Time `json:"disabled_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

// Name is how the user is shown to others and to the agent: the display name,
// or the email when there is none.
func (u *User) Name() string {
	if u.DisplayName != "" {
		return u.DisplayName
	}
	return u.Email
}

func (u *User) IsAdmin() bool { return u.Role == UserRoleAdmin }

type Session struct {
	SessionID string    `json:"session_id"`
	CreatedBy string    `json:"created_by"` // the user who opened it
	Title     string    `json:"title"`
	AgentID   string    `json:"agent_id"`
	Channel   string    `json:"channel"`    // "web", "telegram", "whatsapp"
	ChannelID string    `json:"channel_id"` // chat_id telegram, phone whatsapp, "" pour web
	CreatedAt time.Time `json:"created_at"`

	// A fork: the session and the message it started from, and who forked it.
	// Empty for a session that is not a fork, or whose parent was deleted.
	// AgentMode: when a human message calls the agent (AgentMode* values).
	AgentMode string `json:"agent_mode"`

	ParentSessionID   string `json:"parent_session_id,omitempty"`
	ForkedAtMessageID int64  `json:"forked_at_message_id,omitempty"`
	ForkedBy          string `json:"forked_by,omitempty"`
	// ForkPurpose is what the fork was opened for, in its creator's words;
	// empty when they gave none. It steers the fork's summary.
	ForkPurpose string `json:"fork_purpose,omitempty"`
	// SummaryMessageID is a fork's summary, its first message; 0 until it
	// is posted (AppendForkSummary).
	SummaryMessageID int64 `json:"summary_message_id,omitempty"`
	// A fork's latest report to its parent: the last of the fork's messages
	// it covers (the next one starts after it; 0 = none yet), its message
	// in the parent, and when it was posted.
	LastReportedMessageID int64      `json:"last_reported_message_id,omitempty"`
	LastReportID          int64      `json:"last_report_id,omitempty"`
	LastReportedAt        *time.Time `json:"last_reported_at,omitempty"`
}

// When a human message calls a session's agent.
const (
	// AgentModeAuto: every message while one user is alone in the session,
	// only on @agent once several share it.
	AgentModeAuto    = "auto"
	AgentModeAlways  = "always"
	AgentModeMention = "mention"
)

// SessionMember is a user of a session.
type SessionMember struct {
	UserID      string    `json:"user_id"`
	Email       string    `json:"email"`
	DisplayName string    `json:"display_name"`
	AddedBy     string    `json:"added_by"`
	AddedAt     time.Time `json:"added_at"`
}
