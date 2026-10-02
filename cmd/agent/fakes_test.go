package main

import (
	"context"

	"github.com/victor/temporal-agent/store"
)

// What the route tests never reach, routeStore answers empty: it implements
// the whole serverStore, so a route calling something new fails to compile
// rather than panicking on a nil method.

func (f *routeStore) GetActiveSessionByChannel(context.Context, string, string, string) (*store.Session, error) {
	return nil, nil
}
func (f *routeStore) DeleteSession(context.Context, string) error { return nil }
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
func (f *routeStore) ListTools(context.Context) ([]store.ToolRecord, error) { return nil, nil }
func (f *routeStore) ListActivityQueues(context.Context) ([]store.ActivityQueueEntry, error) {
	return nil, nil
}
func (f *routeStore) SetActivityQueue(context.Context, string, string) error { return nil }
func (f *routeStore) DeleteActivityQueue(context.Context, string) error      { return nil }
