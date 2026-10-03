package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/provider"
)

// failingProvider answers every request with err.
type failingProvider struct{ err error }

func (p failingProvider) Chat(context.Context, provider.ChatRequest) (provider.ChatResponse, error) {
	return provider.ChatResponse{}, p.err
}

// What the API says of its failure becomes Temporal's retry: never for a
// refused request, after the wait it asked for, or by the policy.
func TestCallLLM_TranslatesTheRetry(t *testing.T) {
	apiErr := errors.New("anthropic API error (status 429)")
	for _, c := range []struct {
		name         string
		err          error
		nonRetryable bool
		delay        time.Duration
	}{
		{"wait asked", &provider.RetryAfterError{Err: apiErr, Delay: 7 * time.Second}, false, 7 * time.Second},
		{"refused", &provider.PermanentAPIError{Err: apiErr}, true, 0},
	} {
		a := &LLMActivities{Provider: failingProvider{c.err}}
		_, err := a.CallLLM(context.Background(), provider.ChatRequest{})
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) {
			t.Errorf("%s: %v, want an application error", c.name, err)
			continue
		}
		if appErr.NonRetryable() != c.nonRetryable || appErr.NextRetryDelay() != c.delay || appErr.Message() != apiErr.Error() {
			t.Errorf("%s: non-retryable %v, next retry %s, message %q; want %v, %s, the API's", c.name, appErr.NonRetryable(), appErr.NextRetryDelay(), appErr.Message(), c.nonRetryable, c.delay)
		}
	}

	// Anything else is left to the retry policy.
	_, err := (&LLMActivities{Provider: failingProvider{apiErr}}).CallLLM(context.Background(), provider.ChatRequest{})
	var appErr *temporal.ApplicationError
	if err != apiErr || errors.As(err, &appErr) {
		t.Errorf("plain error: %v", err)
	}
}
