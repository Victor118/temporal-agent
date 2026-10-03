package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/victor/temporal-agent/store"
)

// MemorySaver writes a scope's memory if it is still at the version expected
// (store.PostgresStore.SaveMemory), or fails with a *store.MemoryConflict.
type MemorySaver interface {
	SaveMemory(ctx context.Context, scope store.MemoryScope, scopeID string, content string, expected int64) (int64, error)
}

// memoryUnread refuses a save from a call whose prompt held no memory: the
// save replaces the memory whole, and the model would replace what it never
// read. A sub-agent is given none.
const memoryUnread = "Cannot save memory: the user's current memory is not in your prompt, and a save replaces it whole. If you are working for another agent, put what is worth remembering in your answer instead."

// memoryConflict tells the model its save came after another one, from
// another session or fork, and what the memory is now: the next LLM call
// reads it with its new version, so the merged save succeeds.
func memoryConflict(current string) string {
	return "The memory was changed elsewhere since you read it (another session or fork). Nothing was saved. Current version:\n\n" +
		current + "\n\nMerge your change into it and call save_user_memory again."
}

// RegisterMemoryTools registers tools that let the LLM persist user memory.
func RegisterMemoryTools(registry *Registry, st MemorySaver) {
	registry.Register(&Tool{
		Name: "save_user_memory",
		Description: `Save or update your memory about the user who wrote the message you are answering. Use this to remember important information across sessions: preferences, role, expertise, ongoing projects, communication style, etc.
The content you save will be loaded automatically at the start of every future session with this user.
The memory is private to that one user. In a session several people share, record only what concerns the author of the current message, never anything about the other participants: it would follow this user into sessions the others are not part of.
Write the memory as a concise, structured note. Each call REPLACES the previous memory — include everything you want to remember.`,
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
		// memory is theirs alone; a conflict answers with that memory.
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

			// The author of the message being answered: in a shared session,
			// what the agent learns about one member is not saved for another.
			userID := UserIDFromContext(ctx)
			if userID == "" {
				return "", errors.New("Cannot save memory: user not identified")
			}
			call, _ := CallFromContext(ctx)
			if call.MemoryVersion == nil {
				return "", errors.New(memoryUnread)
			}

			_, err := st.SaveMemory(ctx, store.MemoryScopeUser, userID, params.Content, *call.MemoryVersion)
			var conflict *store.MemoryConflict
			if errors.As(err, &conflict) {
				return "", errors.New(memoryConflict(conflict.Current.Content))
			}
			if err != nil {
				return "", fmt.Errorf("Failed to save memory: %w", err)
			}
			return "Memory saved.", nil
		},
	})
}
