package telegram

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf16"
)

// Channel names Telegram among the channels a session reaches its user on.
const Channel = "telegram"

// maxMessageUnits is Telegram's limit on a message: 4096 characters, which it
// counts in UTF-16 code units.
const maxMessageUnits = 4096

type Client struct {
	token   string
	baseURL string
	client  *http.Client
}

func NewClient(token string) *Client {
	return &Client{
		token:   token,
		baseURL: "https://api.telegram.org",
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

type SendMessageRequest struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode,omitempty"`
}

// SendMessage sends text as plain text, in as many messages as it takes.
//
// No parse_mode: in Telegram's Markdown, an unpaired _, * or ` — common in
// code and identifiers — gets the whole message refused, and the agent's
// answer never arrives. Plain text always goes through.
func (c *Client) SendMessage(chatID, text string) error {
	for _, chunk := range splitMessage(text, maxMessageUnits) {
		if err := c.send(chatID, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) send(chatID, text string) error {
	payload, _ := json.Marshal(SendMessageRequest{ChatID: chatID, Text: text})
	resp, err := c.client.Post(
		fmt.Sprintf("%s/bot%s/sendMessage", c.baseURL, c.token),
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		// The URL holds the token: report the error without it.
		return fmt.Errorf("telegram send: %w", unwrapURLError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Telegram says why in the body ("message is too long", "chat not
		// found"): the status alone does not tell.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("telegram send: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// unwrapURLError drops the URL from a request error, keeping its cause.
func unwrapURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// splitMessage cuts text into chunks of at most limit UTF-16 code units, on a
// line break when there is one in the second half of the chunk, and never
// inside a character.
func splitMessage(text string, limit int) []string {
	var chunks []string
	for text != "" {
		cut, units, lastBreak := len(text), 0, -1
		for i, r := range text {
			n := utf16.RuneLen(r)
			if n < 0 {
				n = 1 // invalid UTF-8: Telegram gets U+FFFD, one unit
			}
			if units+n > limit {
				cut = i
				break
			}
			units += n
			if r == '\n' && units > limit/2 {
				lastBreak = i + 1
			}
		}
		if cut < len(text) && lastBreak > 0 {
			cut = lastBreak
		}
		chunks = append(chunks, text[:cut])
		text = text[cut:]
	}
	return chunks
}

// Update represents an incoming Telegram update.
type Update struct {
	UpdateID int        `json:"update_id"`
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
