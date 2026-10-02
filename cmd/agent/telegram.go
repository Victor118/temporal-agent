package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/telegram"
	"github.com/victor/temporal-agent/workflow"
)

// telegramSecretHeader carries the secret_token given to setWebhook. It is the
// only proof that an update comes from Telegram: the chat ID in the body is
// guessable, and it decides whose agent runs and whose questions get answered.
const telegramSecretHeader = "X-Telegram-Bot-Api-Secret-Token"

func (h *handler) handleTelegramWebhook(w http.ResponseWriter, r *http.Request) {
	if !secretEqual(r.Header.Get(telegramSecretHeader), h.cfg.TelegramWebhookSecret) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var update telegram.Update
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxWebhookBytes)).Decode(&update); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if update.Message == nil || update.Message.Text == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	chatID := update.Message.Chat.ID
	text := update.Message.Text

	// A photo, sticker or voice note has no text. Acknowledge it so Telegram
	// stops retrying, but never turn it into an empty user message.
	if strings.TrimSpace(text) == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Lookup user by telegram_id
	user, err := h.store.GetUserByTelegramID(r.Context(), chatID)
	if err != nil {
		log.Printf("Telegram webhook: error looking up user: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if user == nil || user.DisabledAt != nil {
		log.Printf("Telegram webhook: unknown or disabled telegram_id %d", chatID)
		w.WriteHeader(http.StatusOK) // Return 200 to Telegram so it doesn't retry
		return
	}

	channelID := strconv.FormatInt(chatID, 10)

	// Telegram sessions use the default agent
	agentID, err := h.resolveAgentID(r.Context(), "")
	if err != nil {
		log.Printf("Telegram webhook: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// Handle /new command: create a fresh session
	if text == "/new" {
		sessionID := newUUID()

		_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
			ID:        "session-" + sessionID,
			TaskQueue: h.cfg.WorkflowQueue,
		}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
			SessionID: sessionID,
			AgentID:   agentID,
			Channel:   "telegram",
			ChannelID: channelID,
		})
		if err != nil {
			log.Printf("Telegram webhook: failed to create session: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		if err := h.store.CreateSession(r.Context(), store.Session{
			SessionID: sessionID,
			CreatedBy: user.ID,
			AgentID:   agentID,
			Channel:   "telegram",
			ChannelID: channelID,
		}); err != nil {
			log.Printf("Telegram webhook: failed to persist session: %v", err)
		}

		log.Printf("Telegram: new session %s for user %s", sessionID, user.ID)
		w.WriteHeader(http.StatusOK)
		return
	}

	// Find or create session for this user+channel
	session, err := h.store.GetActiveSessionByChannel(r.Context(), user.ID, "telegram", channelID)
	if err != nil {
		log.Printf("Telegram webhook: error finding session: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if session == nil {
		// Create a new session
		sessionID := newUUID()

		_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
			ID:        "session-" + sessionID,
			TaskQueue: h.cfg.WorkflowQueue,
		}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
			SessionID: sessionID,
			AgentID:   agentID,
			Channel:   "telegram",
			ChannelID: channelID,
		})
		if err != nil {
			log.Printf("Telegram webhook: failed to create session: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		if err := h.store.CreateSession(r.Context(), store.Session{
			SessionID: sessionID,
			CreatedBy: user.ID,
			AgentID:   agentID,
			Channel:   "telegram",
			ChannelID: channelID,
		}); err != nil {
			log.Printf("Telegram webhook: failed to persist session: %v", err)
		}

		session = &store.Session{SessionID: sessionID, AgentID: agentID}
		log.Printf("Telegram: auto-created session %s for user %s", sessionID, user.ID)
	}

	// Check if there's a pending ask_user workflow waiting for an answer
	if answered := h.tryAnswerAskUser(r.Context(), session.SessionID, text); answered {
		w.WriteHeader(http.StatusOK)
		return
	}

	// The full record: an auto-created session above is only partly filled.
	full, err := h.store.GetSession(r.Context(), session.SessionID)
	if err != nil || full == nil {
		log.Printf("Telegram webhook: load session %s: %v", session.SessionID, err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if _, err := h.deliverMessage(r.Context(), full, user, text); err != nil {
		log.Printf("Telegram webhook: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// tryAnswerAskUser checks for a running ask_user child workflow for this session
// and signals it with the user's answer. Returns true if an ask_user was answered.
func (h *handler) tryAnswerAskUser(ctx context.Context, sessionID, answer string) bool {
	resp, err := h.temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: h.cfg.TemporalNamespace,
		Query:     fmt.Sprintf("WorkflowId STARTS_WITH '%s-tool-ask_user' AND ExecutionStatus = 'Running'", sessionID),
		PageSize:  1,
	})
	if err != nil || len(resp.Executions) == 0 {
		return false
	}

	askWfID := resp.Executions[0].Execution.WorkflowId
	if err := h.temporalClient.SignalWorkflow(ctx, askWfID, "", workflow.SignalUserAnswer, answer); err != nil {
		log.Printf("Telegram: failed to signal ask_user workflow %s: %v", askWfID, err)
		return false
	}

	log.Printf("Telegram: routed answer to ask_user workflow %s", askWfID)
	return true
}
