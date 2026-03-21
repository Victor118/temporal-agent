package activity

import (
	"context"

	"github.com/victor/temporal-agent/provider"
)

type LLMActivities struct {
	Provider provider.LLMProvider
}

func (a *LLMActivities) CallLLM(ctx context.Context, request provider.ChatRequest) (provider.ChatResponse, error) {
	return a.Provider.Chat(ctx, request)
}
