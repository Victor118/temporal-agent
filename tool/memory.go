package tool

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/victor/temporal-agent/store"
)

// privateInputTools are the tools whose input is not shown in the session's
// transcript: everyone in a shared session sees the tool calls, and a user's
// memory is theirs alone.
var privateInputTools = map[string]bool{"save_user_memory": true}

// DisplayInput is the input of a tool call as the session's members see it.
func DisplayInput(toolName string, input json.RawMessage) json.RawMessage {
	if privateInputTools[toolName] {
		return json.RawMessage(`{"content":"(private)"}`)
	}
	return input
}

// RegisterMemoryTools registers tools that let the LLM persist user memory.
func RegisterMemoryTools(registry *Registry, st store.Store) {
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
				return "Cannot save memory: user not identified", nil
			}

			if err := st.SaveMemory(ctx, store.MemoryScopeUser, userID, params.Content); err != nil {
				return fmt.Sprintf("Failed to save memory: %s", err.Error()), nil
			}

			return "Memory saved.", nil
		},
	})
}
