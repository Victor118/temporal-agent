package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// ErrNotifyKeyRefused is the server refusing the worker's INTERNAL_API_KEY.
var ErrNotifyKeyRefused = errors.New("the server refused this worker's INTERNAL_API_KEY")

// HTTPNotifier is the web channel of a worker running apart from the server:
// it POSTs each event to the server's internal endpoint, which publishes it
// on its SSE hub. Unlike the hub in a dev process, a POST can fail, and the
// failure is the activity's, so the notification's retry policy applies.
type HTTPNotifier struct {
	BaseURL string
	// APIKey is the INTERNAL_API_KEY shared with the server, which refuses a
	// notification without it.
	APIKey string
	Client *http.Client
}

func NewHTTPNotifier(baseURL, apiKey string) *HTTPNotifier {
	return &HTTPNotifier{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (n *HTTPNotifier) Notify(ctx context.Context, note Notification) error {
	if err := n.post(ctx, NotifyInput{SessionID: note.SessionID, Event: note.Event}); err != nil {
		return fmt.Errorf("notify server: %w", err)
	}
	return nil
}

// Check asks the server whether it accepts this worker's key, with an event
// for no session, which the server drops. A refused key otherwise shows only
// as notifications that never arrive.
func (n *HTTPNotifier) Check(ctx context.Context) error {
	return n.post(ctx, NotifyInput{})
}

func (n *HTTPNotifier) post(ctx context.Context, input NotifyInput) error {
	payload, _ := json.Marshal(input)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.BaseURL+"/internal/notify", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+n.APIKey)
	resp, err := n.Client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return ErrNotifyKeyRefused
	case resp.StatusCode/100 != 2:
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
