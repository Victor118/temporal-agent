package provider

import (
	"fmt"
	"sort"
	"sync"
)

// Factory builds a provider from its API key and the model a request without
// one uses.
type Factory func(apiKey, defaultModel string) LLMProvider

var (
	factoriesMu sync.RWMutex
	factories   = map[string]Factory{
		"anthropic": func(apiKey, model string) LLMProvider { return NewAnthropicProvider(apiKey, model) },
	}
)

// Register makes a provider available to New under name (LLM_PROVIDER). A new
// provider registers itself; nothing that picks one has to change.
func Register(name string, f Factory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	factories[name] = f
}

// New builds the provider registered under name.
func New(name, apiKey, defaultModel string) (LLMProvider, error) {
	factoriesMu.RLock()
	f, ok := factories[name]
	factoriesMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown LLM provider %q (known: %v)", name, names())
	}
	return f(apiKey, defaultModel), nil
}

func names() []string {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	var out []string
	for n := range factories {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
