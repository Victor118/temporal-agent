package tool

import "context"

type contextKey string

const (
	sessionIDKey contextKey = "session_id"
	agentIDKey   contextKey = "agent_id"
)

// WithSessionID injects the session ID into the context.
func WithSessionID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, sessionIDKey, id)
}

// SessionIDFromContext retrieves the session ID from the context.
func SessionIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(sessionIDKey).(string)
	return v
}

// WithAgentID injects the ID of the agent calling the tool into the context.
func WithAgentID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, agentIDKey, id)
}

// AgentIDFromContext retrieves the calling agent's ID from the context.
func AgentIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(agentIDKey).(string)
	return v
}
