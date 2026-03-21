package activity

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// HTTPNotifier implements SSEHub by POSTing events to the server's internal endpoint.
// Used by the worker process when running separately from the server.
type HTTPNotifier struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPNotifier(baseURL string) *HTTPNotifier {
	return &HTTPNotifier{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (n *HTTPNotifier) Publish(sessionID string, event SSEEvent) {
	payload, _ := json.Marshal(NotifyInput{SessionID: sessionID, Event: event})
	resp, err := n.Client.Post(n.BaseURL+"/internal/notify", "application/json", bytes.NewReader(payload))
	if err != nil {
		log.Printf("Warning: failed to notify server: %v", err)
		return
	}
	resp.Body.Close()
}
