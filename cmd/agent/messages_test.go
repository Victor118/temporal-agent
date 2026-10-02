package main

import "testing"

func TestMentionsAgent(t *testing.T) {
	for text, want := range map[string]bool{
		"@agent résume":                  true,
		"ok @Agent, résume":              true,
		"(@agent)":                       true,
		"demande à @default":             true,  // the agent by its ID
		"écris à victor@agent.fr":        false, // an email address
		"@agentic n'est pas une mention": false,
		"@code-reviewer regarde":         false, // another agent: not this one
		"agent, résume":                  false,
		"":                               false,
	} {
		if got := mentionsAgent(text, "default"); got != want {
			t.Errorf("mentionsAgent(%q) = %v, want %v", text, got, want)
		}
	}
}

func TestCallsAgent(t *testing.T) {
	for _, c := range []struct {
		mode    string
		members int
		text    string
		want    bool
	}{
		{"auto", 1, "bonjour", true},        // alone: talking to the agent
		{"auto", 2, "bonjour", false},       // shared: talking to each other
		{"auto", 2, "@agent bonjour", true}, // ...unless called
		{"always", 3, "bonjour", true},
		{"mention", 1, "bonjour", false},
		{"mention", 1, "@agent bonjour", true},
		{"", 1, "bonjour", true}, // an unset mode is auto
	} {
		if got := callsAgent(c.mode, c.members, c.text, "default"); got != c.want {
			t.Errorf("callsAgent(%q, %d, %q) = %v, want %v", c.mode, c.members, c.text, got, c.want)
		}
	}
}
