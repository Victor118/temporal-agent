package main

import (
	"context"
	"time"

	"github.com/victor/temporal-agent/store"
)

// What the route tests never reach, routeStore answers empty: it implements
// the whole serverStore, so a route calling something new fails to compile
// rather than panicking on a nil method.

func (f *routeStore) GetActiveSessionByChannel(context.Context, string, string, string) (*store.Session, error) {
	return nil, nil
}
func (f *routeStore) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	if id == "default" {
		return &store.Agent{ID: "default", Name: "Default"}, nil
	}
	return nil, nil
}
func (f *routeStore) ListSessionsByUser(context.Context, string) ([]store.Session, error) {
	return []store.Session{f.session}, nil
}
func (f *routeStore) ListSessionStats(context.Context, string) (map[string]store.SessionStats, error) {
	return map[string]store.SessionStats{}, nil
}
func (f *routeStore) LoadMessagesWithID(_ context.Context, sessionID string) ([]store.MessageWithID, error) {
	f.countLoad(sessionID)
	return f.messages[sessionID], nil
}
func (f *routeStore) DeleteMessage(context.Context, string, int64) error    { return nil }
func (f *routeStore) DeleteMessagesBySession(context.Context, string) error { return nil }
func (f *routeStore) DeleteLoginSession(_ context.Context, tokenHash string) error {
	delete(f.logins, tokenHash)
	return nil
}
func (f *routeStore) GetUserByTelegramID(context.Context, int64) (*store.User, error) {
	return nil, nil
}
func (f *routeStore) IncrementSkillsVersion(context.Context) (int64, error) { return 1, nil }
func (f *routeStore) ListTools(context.Context) ([]store.ToolRecord, error) { return f.tools, nil }
func (f *routeStore) ListActivityQueues(context.Context) ([]store.ActivityQueueEntry, error) {
	return nil, nil
}
func (f *routeStore) SetActivityQueue(context.Context, string, string) error { return nil }
func (f *routeStore) DeleteActivityQueue(context.Context, string) error      { return nil }
func (f *routeStore) GetFile(_ context.Context, id string) (*store.File, error) {
	for _, file := range f.files {
		if file.ID == id {
			return &file, nil
		}
	}
	return nil, nil
}
func (f *routeStore) ReadFileContent(_ context.Context, id string) ([]byte, error) {
	return f.contents[id], nil
}
func (f *routeStore) ListSessionFiles(_ context.Context, sessionID string) ([]store.File, error) {
	f.fileLists++
	var out []store.File
	for _, file := range f.files {
		if file.SessionID == sessionID {
			out = append(out, file)
		}
	}
	return out, nil
}

func (f *routeStore) ListRunningTasks(_ context.Context, sessionID, participant string) ([]store.BackgroundTask, error) {
	var out []store.BackgroundTask
	for _, t := range f.tasks {
		if t.SessionID == sessionID && (participant == "" || t.Participant == participant) && t.State == store.BackgroundRunning {
			out = append(out, t)
		}
	}
	return out, nil
}
func (f *routeStore) GetTask(_ context.Context, id string) (*store.BackgroundTask, error) {
	for _, t := range f.tasks {
		if t.ID == id {
			return &t, nil
		}
	}
	return nil, nil
}
func (f *routeStore) SetTaskCancelledBy(_ context.Context, id, name string) error {
	if f.cancelledBy == nil {
		f.cancelledBy = map[string]string{}
	}
	f.cancelledBy[id] = name
	return nil
}
func (f *routeStore) ListTasksRunningSince(context.Context, time.Time) ([]store.BackgroundTask, error) {
	return nil, nil
}
func (f *routeStore) EndTask(context.Context, string, string, string, func(store.BackgroundTask) store.Message) (store.TaskEnding, error) {
	return store.TaskEnding{Gone: true}, nil
}
