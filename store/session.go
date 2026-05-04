package store

import "time"

type User struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	TelegramID *int64 `json:"telegram_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type Session struct {
	SessionID string    `json:"session_id"`
	UserID    string    `json:"user_id"`
	Title     string    `json:"title"`
	TaskQueue string    `json:"task_queue"`
	Channel   string    `json:"channel"`    // "web", "telegram", "whatsapp"
	ChannelID string    `json:"channel_id"` // chat_id telegram, phone whatsapp, "" pour web
	CreatedAt time.Time `json:"created_at"`
}
