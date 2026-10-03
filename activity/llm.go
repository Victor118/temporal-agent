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
		// The API said when to come back: the next attempt waits that long
		// instead of the policy's interval. The attempts still count.
		var waitErr *provider.RetryAfterError
		if errors.As(err, &waitErr) {
			return resp, temporal.NewApplicationErrorWithOptions(err.Error(), "RetryAfterError", temporal.ApplicationErrorOptions{
				NextRetryDelay: waitErr.Delay,
				Cause:          err,
			})
		}
	}
	return resp, err
}
