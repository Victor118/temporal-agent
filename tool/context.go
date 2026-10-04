package tool

import (
	"context"
	"time"
)

type contextKey string

const (
	sessionIDKey contextKey = "session_id"
	agentIDKey   contextKey = "agent_id"
	userIDKey    contextKey = "user_id"
	callKey      contextKey = "call"
	callTimeKey  contextKey = "call_time"
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

// WithCall injects the caller's context, given to an activity tool flagged
// NeedsCallContext: a workflow tool reads it from its input instead.
func WithCall(ctx context.Context, cc CallContext) context.Context {
	return context.WithValue(ctx, callKey, cc)
}

// CallFromContext retrieves the caller's context; false when the tool was not
// given one.
func CallFromContext(ctx context.Context) (CallContext, bool) {
	cc, ok := ctx.Value(callKey).(CallContext)
	return cc, ok
}

// WithCallTime injects when the call was scheduled: the same for every
// attempt of it, so that a retry dates what it makes as the first attempt
// did (a rendered document, which it publishes again under the call's ID).
func WithCallTime(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, callTimeKey, t)
}

// CallTimeFromContext retrieves when the call was scheduled; the zero time
// when it was not given.
func CallTimeFromContext(ctx context.Context) time.Time {
	t, _ := ctx.Value(callTimeKey).(time.Time)
	return t
}
