package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/victor/temporal-agent/activity"
)

// Channel names Telegram among the channels a session reaches its user on.
const Channel = "telegram"

// maxMessageUnits is Telegram's limit on a message: 4096 characters, which it
// counts in UTF-16 code units.
const maxMessageUnits = 4096

// sendAttempts is how many times one message is tried before the send gives
// up, and retryDelay the wait before the second try (doubled for the next).
// They are retried here rather than by the activity: a retried activity
// would send again the parts of a long answer that went through already.
const sendAttempts = 3

var retryDelay = time.Second

type Client struct {
	token   string
	baseURL string
	client  *http.Client
}

// NewClient returns a client whose requests each time out after 5 seconds: a
// long answer is several requests, and they all fit in the notification's
// activity together with their retries.
func NewClient(token string) *Client {
	return &Client{
		token:   token,
		baseURL: "https://api.telegram.org",
		client:  &http.Client{Timeout: 5 * time.Second},
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
//
// Each message is retried on its own. Once one has gone out, a failure is an
// *activity.PartialDelivery, which nothing retries: Telegram cannot tell a
// message it already has, and the user would read the beginning again.
//
// For the same reason, a message after the first is only started if ctx
// leaves it the time to finish, retries included (worstSend): an activity
// that times out with the beginning sent is retried, beginning included,
// whatever it returns afterwards. Without that time, the rest is given up as
// a PartialDelivery.
func (c *Client) SendMessage(ctx context.Context, chatID, text string) error {
	chunks := splitMessage(text, maxMessageUnits)
	for i, chunk := range chunks {
		if i > 0 && !c.timeFor(ctx) {
			return &activity.PartialDelivery{Delivered: i, Total: len(chunks), Err: errNoTimeLeft}
		}
		if err := c.sendRetrying(ctx, chatID, chunk); err != nil {
			if i == 0 {
				return err
			}
			return &activity.PartialDelivery{Delivered: i, Total: len(chunks), Err: err}
		}
	}
	return nil
}

// errNoTimeLeft is the rest of an answer given up for lack of time.
var errNoTimeLeft = errors.New("telegram send: not enough time left to send the rest before the activity's timeout")

// sendMargin is kept between the end of the last send and ctx's deadline:
// the activity still has to report its result.
const sendMargin = 2 * time.Second

// worstSend is the longest one message can take: every attempt up to the
// request timeout, and the waits between them.
func (c *Client) worstSend() time.Duration {
	worst, delay := time.Duration(0), retryDelay
	for attempt := 1; attempt <= sendAttempts; attempt++ {
		worst += c.client.Timeout
		if attempt < sendAttempts {
			worst += delay
			delay *= 2
		}
	}
	return worst
}

// timeFor reports whether ctx leaves one more message the time to finish.
func (c *Client) timeFor(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) >= c.worstSend()+sendMargin
}

// sendRetrying sends one message, trying again after a failure that may pass:
// the network, a rate limit, Telegram's own errors. A refusal (a chat that
// does not exist) is final.
func (c *Client) sendRetrying(ctx context.Context, chatID, text string) error {
	delay := retryDelay
	for attempt := 1; ; attempt++ {
		err := c.send(ctx, chatID, text)
		var refused *refusedError
		if err == nil || errors.As(err, &refused) || attempt == sendAttempts {
			return err
		}
		select {
		case <-time.After(delay):
			delay *= 2
		case <-ctx.Done():
			return err
		}
	}
}

// refusedError is Telegram declining a message for a reason a retry does not
// change.
type refusedError struct{ status int }

func (e *refusedError) Error() string { return fmt.Sprintf("status %d", e.status) }

func (c *Client) send(ctx context.Context, chatID, text string) error {
	payload, _ := json.Marshal(SendMessageRequest{ChatID: chatID, Text: text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/bot%s/sendMessage", c.baseURL, c.token), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("telegram send: %w", unwrapURLError(err))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		// The URL holds the token: report the error without it.
		return fmt.Errorf("telegram send: %w", unwrapURLError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Telegram says why in the body ("message is too long", "chat not
		// found"): the status alone does not tell.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		reason := strings.TrimSpace(string(body))
		if resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("telegram send: %w: %s", &refusedError{resp.StatusCode}, reason)
		}
		return fmt.Errorf("telegram send: status %d: %s", resp.StatusCode, reason)
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
