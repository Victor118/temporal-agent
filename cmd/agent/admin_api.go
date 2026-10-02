package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/store"
)

// activityQueueStore is the activity → task queue routing, and the tools that
// tell which queues exist.
type activityQueueStore interface {
	ListTools(ctx context.Context) ([]store.ToolRecord, error)
	ListActivityQueues(ctx context.Context) ([]store.ActivityQueueEntry, error)
	SetActivityQueue(ctx context.Context, activityName, taskQueue string) error
	DeleteActivityQueue(ctx context.Context, activityName string) error
}

// activityQueuesAPI is the admin JSON behind the chat's activity queue panel.
type activityQueuesAPI struct {
	store         activityQueueStore
	workflowQueue string
}

// listKnownQueues lists the workflow queue and the queues serving tools.
func (a *activityQueuesAPI) listKnownQueues(w http.ResponseWriter, r *http.Request) {
	tools, err := a.store.ListTools(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list tools: %v", err), http.StatusInternalServerError)
		return
	}
	queues := []string{a.workflowQueue}
	for _, t := range tools {
		if !slices.Contains(queues, t.TaskQueue) {
			queues = append(queues, t.TaskQueue)
		}
	}
	writeJSON(w, http.StatusOK, queues)
}

func (a *activityQueuesAPI) listActivityQueues(w http.ResponseWriter, r *http.Request) {
	entries, err := a.store.ListActivityQueues(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list activity queues: %v", err), http.StatusInternalServerError)
		return
	}
	if entries == nil {
		entries = []store.ActivityQueueEntry{}
	}
	writeJSON(w, http.StatusOK, entries)
}

func (a *activityQueuesAPI) setActivityQueue(w http.ResponseWriter, r *http.Request) {
	var req store.ActivityQueueEntry
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.ActivityName == "" || req.TaskQueue == "" {
		http.Error(w, "activity_name and task_queue are required", http.StatusBadRequest)
		return
	}
	if err := a.store.SetActivityQueue(r.Context(), req.ActivityName, req.TaskQueue); err != nil {
		http.Error(w, fmt.Sprintf("Failed to set activity queue: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *activityQueuesAPI) deleteActivityQueue(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteActivityQueue(r.Context(), chi.URLParam(r, "activityName")); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete activity queue: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
