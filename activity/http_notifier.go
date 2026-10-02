package activity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// HTTPNotifier implements SSEHub by POSTing events to the server's internal endpoint.
// Used by the worker process when running separately from the server.
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

func (n *HTTPNotifier) Publish(sessionID string, event SSEEvent) {
	if err := n.post(sessionID, event); err != nil {
		log.Printf("Warning: failed to notify server: %v", err)
	}
}

func (n *HTTPNotifier) post(sessionID string, event SSEEvent) error {
	payload, _ := json.Marshal(NotifyInput{SessionID: sessionID, Event: event})
	req, err := http.NewRequest(http.MethodPost, n.BaseURL+"/internal/notify", bytes.NewReader(payload))
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
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
