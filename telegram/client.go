package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	token  string
	client *http.Client
}

func NewClient(token string) *Client {
	return &Client{
		token:  token,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

type SendMessageRequest struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode,omitempty"`
}

func (c *Client) SendMessage(chatID, text string) error {
	// Telegram limits messages to 4096 characters
	const maxLen = 4096
	for len(text) > 0 {
		chunk := text
		if len(chunk) > maxLen {
			chunk = text[:maxLen]
			text = text[maxLen:]
		} else {
			text = ""
		}

		payload, _ := json.Marshal(SendMessageRequest{
			ChatID:    chatID,
			Text:      chunk,
			ParseMode: "Markdown",
		})

		resp, err := c.client.Post(
			fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.token),
			"application/json",
			bytes.NewReader(payload),
		)
		if err != nil {
			return fmt.Errorf("telegram send: %w", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("telegram send: status %d", resp.StatusCode)
		}
	}
	return nil
}

// Update represents an incoming Telegram update.
type Update struct {
	UpdateID int      `json:"update_id"`
	Message  *TGMessage `json:"message,omitempty"`
}

type TGMessage struct {
	MessageID int    `json:"message_id"`
	Chat      Chat   `json:"chat"`
	From      *User  `json:"from,omitempty"`
	Text      string `json:"text"`
}

type Chat struct {
	ID int64 `json:"id"`
}

type User struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
}
