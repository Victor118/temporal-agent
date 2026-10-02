package tool

import "context"

type contextKey string

const (
	sessionIDKey contextKey = "session_id"
	agentIDKey   contextKey = "agent_id"
	userIDKey    contextKey = "user_id"
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

// WithUserID injects the user the tool acts for: the author of the message the
// agent is answering. In a shared session, that is not every member.
func WithUserID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, userIDKey, id)
}

// UserIDFromContext retrieves the user the tool acts for.
func UserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(userIDKey).(string)
	return v
}
