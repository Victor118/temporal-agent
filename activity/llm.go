package activity

import (
	"context"
	"errors"

	"github.com/victor/temporal-agent/provider"
	"go.temporal.io/sdk/temporal"
)

type LLMActivities struct {
	Provider provider.LLMProvider
}

func (a *LLMActivities) CallLLM(ctx context.Context, request provider.ChatRequest) (provider.ChatResponse, error) {
	resp, err := a.Provider.Chat(ctx, request)
	if err != nil {
		var permErr *provider.PermanentAPIError
		if errors.As(err, &permErr) {
			return resp, temporal.NewNonRetryableApplicationError(err.Error(), "PermanentAPIError", err)
		}
	}
	return resp, err
}
