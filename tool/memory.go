package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/victor/temporal-agent/store"
)

// MemorySaver writes a scope's memory if it is still at the version expected
// (store.PostgresStore.SaveMemory), or fails with store.ErrMemoryConflict.
type MemorySaver interface {
	SaveMemory(ctx context.Context, scope store.MemoryScope, scopeID string, content string, expected int64) (int64, error)
}

// A save replaces the memory whole: it is refused from a call whose prompt
// held no memory, the model would replace what it never read. Either the
// memory could not be read for this step (the next LLM call reads it again),
// or the run is given none (a sub-agent: its parent has the memory). A call
// without its context does not say which version the model read: refused
// too, without blaming the run.
const (
	memoryUnread    = "Cannot save memory: the user's memory could not be read for this step, and a save replaces it whole. Nothing was saved. Try again at your next step."
	memoryAbsent    = "Cannot save memory: the user's memory is not in your prompt (a sub-agent is given none), and a save replaces it whole. Nothing was saved. If you are working for another agent, put what is worth remembering in your answer instead."
	memoryNoContext = "Cannot save memory: this call does not say which version of the user's memory you read, and a save replaces it whole. Nothing was saved."
	memoryEmpty     = "Cannot save memory: content is empty. A save replaces the whole memory: give everything worth keeping about the user."
)

// memoryConflict tells the model its save came after another one. It does
// not repeat the memory: this result stays in the session's history, which
// the agent reads when it answers another member, and the memory may hold
// what the user said elsewhere. The next LLM call reads the memory again, at
// its new version, so the merged save succeeds.
const memoryConflict = "The memory was changed since you read it (a parallel save, another session or a fork). Nothing was saved. Your system prompt now holds the current version: merge your change into it and call save_user_memory again."

// RegisterMemoryTools registers tools that let the LLM persist user memory.
func RegisterMemoryTools(registry *Registry, st MemorySaver) {
	registry.Register(&Tool{
		Name: "save_user_memory",
		Description: `Save or update your memory about the user who wrote the message you are answering. Use this to remember important information across sessions: preferences, role, expertise, ongoing projects, communication style, etc.
The content you save will be loaded automatically at the start of every future session with this user.
The memory is private to that one user. In a session several people share, record only what concerns the author of the current message, never anything about the other participants: it would follow this user into sessions the others are not part of.
Write the memory as a concise, structured note. Each call REPLACES the previous memory — include everything you want to remember.
Only works when the user's memory is in your system prompt; a sub-agent has none.`,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"content": {
					"type": "string",
					"description": "The full memory content to save. This replaces any previous memory. Use structured text (markdown) for clarity."
				}
			},
			"required": ["content"]
		}`),
		Kind: ToolKindActivity,
		// Everyone in a shared session sees the tool calls, and a user's
		// memory is theirs alone.
		PrivateInput: true,
		// The version of the memory the model read (CallContext.MemoryVersion):
		// the save replaces that one only.
		NeedsCallContext: true,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Content string `json:"content"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("parse input: %w", err)
			}
			if strings.TrimSpace(params.Content) == "" {
				return "", errors.New(memoryEmpty)
			}

			// The author of the message being answered: in a shared session,
			// what the agent learns about one member is not saved for another.
			userID := UserIDFromContext(ctx)
			if userID == "" {
				return "", errors.New("Cannot save memory: user not identified")
			}
			call, ok := CallFromContext(ctx)
			if !ok {
				return "", errors.New(memoryNoContext)
			}
			if call.MemoryVersion == nil {
				if call.MemoryUnread {
					return "", errors.New(memoryUnread)
				}
				return "", errors.New(memoryAbsent)
			}

			_, err := st.SaveMemory(ctx, store.MemoryScopeUser, userID, params.Content, *call.MemoryVersion)
			if errors.Is(err, store.ErrMemoryConflict) {
				return "", errors.New(memoryConflict)
			}
			if err != nil {
				return "", fmt.Errorf("Failed to save memory: %w", err)
			}
			return "Memory saved.", nil
		},
	})
}
