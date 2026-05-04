package tool

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/victor/temporal-agent/store"
)

// RegisterMemoryTools registers tools that let the LLM persist user memory.
func RegisterMemoryTools(registry *Registry, st store.Store) {
	registry.Register(&Tool{
		Name: "save_user_memory",
		Description: `Save or update your memory about the current user. Use this to remember important information across sessions: preferences, role, expertise, ongoing projects, communication style, etc.
The content you save will be loaded automatically at the start of every future session with this user.
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

			userID := ""
			if sid := SessionIDFromContext(ctx); sid != "" {
				if uid, err := st.GetSessionUser(ctx, sid); err == nil {
					userID = uid
				}
			}
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
