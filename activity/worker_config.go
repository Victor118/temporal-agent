package activity

import "sync"

// WorkerConfig holds runtime configuration loaded by the worker and consumed
// by workflows via SideEffect. It is thread-safe for concurrent reads/writes.
type WorkerConfig struct {
	mu             sync.RWMutex
	activityQueues map[string]string // activity name → task queue
}

func NewWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		activityQueues: make(map[string]string),
	}
}

func (c *WorkerConfig) SetActivityQueues(m map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activityQueues = m
}

// GetActivityQueues returns a copy of the current activity → task queue mapping.
func (c *WorkerConfig) GetActivityQueues() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := make(map[string]string, len(c.activityQueues))
	for k, v := range c.activityQueues {
		cp[k] = v
	}
	return cp
}

// Global instance — set by the worker at startup, read by workflows via SideEffect.
var globalWorkerConfig *WorkerConfig

func SetGlobalWorkerConfig(c *WorkerConfig) {
	globalWorkerConfig = c
}

// GetActivityQueues returns the current activity → task queue mapping from the global config.
// Called from workflow SideEffect — must be safe for concurrent use.
func GetActivityQueues() map[string]string {
	if globalWorkerConfig == nil {
		return nil
	}
	return globalWorkerConfig.GetActivityQueues()
}
