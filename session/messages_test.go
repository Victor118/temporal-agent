package session

import "testing"

func TestMentionsAgent(t *testing.T) {
	for text, want := range map[string]bool{
		"@jarvis résume":                    true,
		"ok @Jarvis, résume":                true,
		"(@jarvis)":                         true,
		"@agent résume":                     false, // the generic name no longer calls it
		"demande à @default":                false, // nor its ID, once it has a mention
		"écris à victor@jarvis.fr":          false, // an email address
		"@jarvisette n'est pas une mention": false,
		"@code-reviewer regarde":            false, // another agent: not this one
		"jarvis, résume":                    false,
		"":                                  false,
	} {
		if got := mentionsAgent(text, "jarvis"); got != want {
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
		{"auto", 1, "bonjour", true},         // alone: talking to the agent
		{"auto", 2, "bonjour", false},        // shared: talking to each other
		{"auto", 2, "@jarvis bonjour", true}, // ...unless called
		{"always", 3, "bonjour", true},
		{"mention", 1, "bonjour", false},
		{"mention", 1, "@jarvis bonjour", true},
		{"mention", 1, "@agent bonjour", false},
		{"", 1, "bonjour", true}, // an unset mode is auto
	} {
		if got := callsAgent(c.mode, c.members, c.text, "jarvis"); got != c.want {
			t.Errorf("callsAgent(%q, %d, %q) = %v, want %v", c.mode, c.members, c.text, got, c.want)
		}
	}
}

// No mention, nothing calls: an agent always has one (MentionName).
func TestMentionsAgent_NeedsAMention(t *testing.T) {
	if mentionsAgent("@ hello", "") {
		t.Error("an empty mention matched")
	}
}
