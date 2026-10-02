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
}

// SessionMember is a user of a session.
type SessionMember struct {
	UserID      string    `json:"user_id"`
	Email       string    `json:"email"`
	DisplayName string    `json:"display_name"`
	AddedBy     string    `json:"added_by"`
	AddedAt     time.Time `json:"added_at"`
}
