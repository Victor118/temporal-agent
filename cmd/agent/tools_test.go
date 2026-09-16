package main

import (
	"fmt"
	"testing"

	"github.com/victor/temporal-agent/config"
)

func TestWorkerQueues(t *testing.T) {
	cfg := &config.Config{WorkflowQueue: "agent"}
	cases := []struct {
		wc   config.WorkerConfig
		want string
	}{
		{config.WorkerConfig{Queue: "tools-gpu"}, "[tools-gpu]"},
		{config.WorkerConfig{Queue: "tools-core", Workflows: true}, "[agent tools-core]"},
		{config.WorkerConfig{Queue: "agent", Workflows: true}, "[agent]"},
	}
	for _, c := range cases {
		if got := fmt.Sprint(workerQueues(cfg, &c.wc)); got != c.want {
			t.Errorf("%+v: got %s, want %s", c.wc, got, c.want)
		}
	}
}
