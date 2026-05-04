package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/telegram"
	"github.com/victor/temporal-agent/workflow"
)

func (h *handler) handleTelegramWebhook(w http.ResponseWriter, r *http.Request) {
	var update telegram.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if update.Message == nil || update.Message.Text == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	chatID := update.Message.Chat.ID
	text := update.Message.Text

	// Lookup user by telegram_id
	user, err := h.store.GetUserByTelegramID(r.Context(), chatID)
	if err != nil {
		log.Printf("Telegram webhook: error looking up user: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if user == nil {
		log.Printf("Telegram webhook: unknown telegram_id %d", chatID)
		w.WriteHeader(http.StatusOK) // Return 200 to Telegram so it doesn't retry
		return
	}

	channelID := strconv.FormatInt(chatID, 10)

	// Handle /new command: create a fresh session
	if text == "/new" {
		sessionID := newUUID()
		taskQueue := h.cfg.PrimaryTaskQueue()

		_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
			ID:        "session-" + sessionID,
			TaskQueue: taskQueue,
		}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
			SessionID: sessionID,
			UserID:    user.ID,
			Model:     h.cfg.LLMModel,
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
			UserID:    user.ID,
			TaskQueue: taskQueue,
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
		taskQueue := h.cfg.PrimaryTaskQueue()

		_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
			ID:        "session-" + sessionID,
			TaskQueue: taskQueue,
		}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
			SessionID: sessionID,
			UserID:    user.ID,
			Model:     h.cfg.LLMModel,
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
			UserID:    user.ID,
			TaskQueue: taskQueue,
			Channel:   "telegram",
			ChannelID: channelID,
		}); err != nil {
			log.Printf("Telegram webhook: failed to persist session: %v", err)
		}

		session = &store.Session{SessionID: sessionID}
		log.Printf("Telegram: auto-created session %s for user %s", sessionID, user.ID)
	}

	// Find active workflow or restart
	workflowID := h.findActiveWorkflowID(r.Context(), session.SessionID)
	if workflowID == "" {
		newWorkflowID := fmt.Sprintf("session-%s-%d", session.SessionID, time.Now().Unix())
		_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
			ID:        newWorkflowID,
			TaskQueue: h.cfg.PrimaryTaskQueue(),
		}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
			SessionID: session.SessionID,
			UserID:    user.ID,
			Model:     h.cfg.LLMModel,
			Channel:   "telegram",
			ChannelID: channelID,
		})
		if err != nil {
			log.Printf("Telegram webhook: failed to resume session: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		workflowID = newWorkflowID
		log.Printf("Telegram: resumed session %s with workflow %s", session.SessionID, workflowID)
	}

	// Check if there's a pending ask_user workflow waiting for an answer
	if answered := h.tryAnswerAskUser(r.Context(), session.SessionID, text); answered {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Signal the workflow with the message
	if err := h.temporalClient.SignalWorkflow(r.Context(), workflowID, "", workflow.SignalUserMessage, text); err != nil {
		log.Printf("Telegram webhook: failed to signal workflow: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// Set title from first message
	go func() {
		title := text
		if len(title) > 80 {
			title = title[:80] + "..."
		}
		h.store.UpdateSessionTitle(r.Context(), session.SessionID, title)
	}()

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
