package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/sse"
)

// maxWebhookBytes bounds a webhook payload. GitHub caps its own at 25 MB, but
// a push event that matters here is a few kilobytes.
const maxWebhookBytes = 1 << 20

// maxNotifyBytes bounds a notification from a worker. An event carries one
// agent message at most, far below this.
const maxNotifyBytes = 4 << 20

// handleInternalNotify receives SSE events from workers and publishes them to
// the local hub. A worker proves itself with the shared INTERNAL_API_KEY: the
// endpoint can put any event in any session, a fake question or a fake answer
// of the agent included, so without a key configured it refuses everything.
func handleInternalNotify(hub *sse.Hub, apiKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !validBearer(r, apiKey) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		var input activity.NotifyInput
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxNotifyBytes)).Decode(&input); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		// No session: a worker checking its key at startup.
		if input.SessionID != "" {
			hub.Publish(input.SessionID, input.Event)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// validBearer reports whether the request carries "Authorization: Bearer
// <key>". An empty key matches nothing.
func validBearer(r *http.Request, key string) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && secretEqual(got, key)
}

// secretEqual compares a presented secret with the expected one in constant
// time. An empty expected secret matches nothing: an unset secret closes the
// route rather than opening it.
func secretEqual(got, want string) bool {
	return want != "" && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// skillsVersionBumper moves the version that makes the server and the workers
// reload the skills.
type skillsVersionBumper interface {
	IncrementSkillsVersion(ctx context.Context) (int64, error)
}

// skillsWebhook handles GitHub webhook pushes to increment the skills version.
// Workers detect the change via DB polling and reload from git. Every request
// must be signed with SKILLS_WEBHOOK_SECRET: each one makes the server and
// every worker clone the skills repo again.
type skillsWebhook struct {
	secret string
	store  skillsVersionBumper
}

func (s *skillsWebhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBytes))
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	if !verifyGitHubSignature(body, r.Header.Get("X-Hub-Signature-256"), s.secret) {
		http.Error(w, "Invalid signature", http.StatusUnauthorized)
		return
	}

	newVersion, err := s.store.IncrementSkillsVersion(r.Context())
	if err != nil {
		log.Printf("Error incrementing skills version: %v", err)
		http.Error(w, "Failed to update skills version", http.StatusInternalServerError)
		return
	}

	log.Printf("Skills webhook: version incremented to %d", newVersion)
	w.WriteHeader(http.StatusNoContent)
}

// verifyGitHubSignature checks the HMAC-SHA256 signature sent by GitHub.
// The header format is "sha256=<hex digest>".
func verifyGitHubSignature(payload []byte, signature, secret string) bool {
	if secret == "" || !strings.HasPrefix(signature, "sha256=") {
		return false
	}

	got, err := hex.DecodeString(signature[len("sha256="):])
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	expected := mac.Sum(nil)

	return hmac.Equal(got, expected)
}
