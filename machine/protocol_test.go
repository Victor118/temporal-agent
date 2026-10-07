package machine

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestCheckFromMachine(t *testing.T) {
	hello := func() Message {
		return Message{Type: TypeHello, Protocol: Protocol, Capabilities: []string{"echo"}, MaxDirectives: 1,
			Running: []string{"d-1"}, Finished: []string{"d-2"}}
	}
	ok := []Message{
		hello(),
		{Type: TypeRotated},
		{Type: TypeProgress, ID: "d-1", Text: "half"},
		{Type: TypeResult, ID: "d-1", Status: StatusOK, Output: json.RawMessage(`{"text":"hi"}`)},
		{Type: TypeResult, ID: "d-1", Status: StatusStopping},
	}
	for _, m := range ok {
		if err := CheckFromMachine(&m); err != nil {
			t.Errorf("%+v: %v", m, err)
		}
	}
	many := make([]string, MaxListedIDs+1)
	for i := range many {
		many[i] = "d"
	}
	refused := map[string]Message{
		"no protocol":          {Type: TypeHello, MaxDirectives: 1},
		"bad capability":       func() Message { m := hello(); m.Capabilities = []string{"Echo!"}; return m }(),
		"no cap on directives": func() Message { m := hello(); m.MaxDirectives = 0; return m }(),
		"too many directives":  func() Message { m := hello(); m.MaxDirectives = MaxDirectives + 1; return m }(),
		"too many listed":      func() Message { m := hello(); m.Running = many; return m }(),
		"bad listed id":        func() Message { m := hello(); m.Finished = []string{"../x"}; return m }(),
		"progress without id":  {Type: TypeProgress},
		"unknown status":       {Type: TypeResult, ID: "d-1", Status: "done"},
		"output not JSON":      {Type: TypeResult, ID: "d-1", Status: StatusOK, Output: json.RawMessage(`{`)},
		"a gateway's message":  {Type: TypeDirective, ID: "d-1"},
		"no type":              {},
	}
	for name, m := range refused {
		if err := CheckFromMachine(&m); !errors.Is(err, ErrBadMessage) {
			t.Errorf("%s: %v", name, err)
		}
	}

	// What can be cut is cut, not refused.
	m := Message{Type: TypeProgress, ID: "d-1", Text: strings.Repeat("é", MaxProgressBytes)}
	if err := CheckFromMachine(&m); err != nil || len(m.Text) > MaxProgressBytes || !utf8.ValidString(m.Text) {
		t.Errorf("long progress: %d bytes, %v", len(m.Text), err)
	}
}

func TestCut(t *testing.T) {
	if got := Cut("short", 10); got != "short" {
		t.Errorf("short: %q", got)
	}
	got := Cut(strings.Repeat("ab", 20), 10)
	if len(got) > 10 || !strings.HasSuffix(got, "…") {
		t.Errorf("cut: %q", got)
	}
}

func TestUserCode(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		c := NewUserCode()
		if len(c) != UserCodeLength || NormalizeUserCode(c) != c {
			t.Fatalf("code %q", c)
		}
		seen[c] = true
	}
	if len(seen) < 190 {
		t.Errorf("%d distinct codes of 200", len(seen))
	}
	if got := FormatUserCode("KX492M"); got != "KX4-92M" {
		t.Errorf("format: %q", got)
	}
	for in, want := range map[string]string{
		"kx4-92m":    "KX492M",
		" KX4 92M ":  "KX492M",
		"KX492":      "", // too short
		"KX4-920":    "", // 0 is not in the alphabet
		"KX4-92M-AB": "", // too long
		"KX4–92M":    "", // another dash
	} {
		if got := NormalizeUserCode(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

func TestTokens(t *testing.T) {
	a, b := NewMachineToken(), NewMachineToken()
	if a == b || !strings.HasPrefix(a, "agm_") || len(a) < 40 {
		t.Errorf("machine tokens %q %q", a, b)
	}
	if !strings.HasPrefix(NewEnrollmentToken(), "age_") || !strings.HasPrefix(NewDeviceSecret(), "agd_") {
		t.Error("prefixes")
	}
	if HashToken(a) == HashToken(b) || HashToken(a) != HashToken(a) || strings.Contains(HashToken(a), a) {
		t.Error("hashes")
	}
}

func TestEchoInput_Check(t *testing.T) {
	for _, in := range []EchoInput{
		{Text: "hi"},
		{Text: "hi", DurationMS: 1000, ProgressEveryMS: 100},
	} {
		if err := in.Check(); err != nil {
			t.Errorf("%+v: %v", in, err)
		}
	}
	for _, in := range []EchoInput{
		{Text: strings.Repeat("x", 5000)},
		{DurationMS: -1},
		{DurationMS: int64(2 * MaxEchoDuration / 1e6)},
		{DurationMS: 1000, ProgressEveryMS: 10},
	} {
		if err := in.Check(); err == nil {
			t.Errorf("%+v accepted", in)
		}
	}
}

func TestCapabilitiesMessage(t *testing.T) {
	ok := Message{Type: TypeCapabilities, Capabilities: []string{"echo", CapClaudeCode}, ClaudeCode: "logged_out"}
	if err := CheckFromMachine(&ok); err != nil {
		t.Error(err)
	}
	for _, m := range []Message{
		{Type: TypeCapabilities, ClaudeCode: "maybe"},
		{Type: TypeCapabilities, Capabilities: []string{"Bad!"}},
		{Type: TypeHello, Protocol: Protocol, MaxDirectives: 1, ClaudeCode: "yes"},
	} {
		if err := CheckFromMachine(&m); !errors.Is(err, ErrBadMessage) {
			t.Errorf("%+v: %v", m, err)
		}
	}
}

func TestAnalyzeInput_Check(t *testing.T) {
	if err := (AnalyzeInput{Repo: "git@github.com:me/app", Ref: "main", Task: "why"}).Check(); err != nil {
		t.Error(err)
	}
	for _, in := range []AnalyzeInput{
		{Task: "why"},
		{Repo: "r"},
		{Repo: "--upload-pack=touch /tmp/x", Task: "why"},
		{Repo: "r\nx", Task: "why"},
		{Repo: "r", Ref: "--output=x", Task: "why"},
		{Repo: "r", Task: strings.Repeat("x", 70000)},
	} {
		if err := in.Check(); err == nil {
			t.Errorf("%+v accepted", in)
		}
	}
	if CapabilityOf(KindAnalyzeRepo) != CapClaudeCode || CapabilityOf(KindEcho) != KindEcho {
		t.Error("capabilities of kinds")
	}
	if got := CodingProgress(34, "Grep"); got != "34 outils (dernier : Grep)" {
		t.Errorf("progress %q", got)
	}
	if got := CodingProgress(1, ""); got != "1 outil" {
		t.Errorf("progress %q", got)
	}
}

// A hello says how many calls to the model the machine makes at once, and
// which model: bounded, cut, never trusted further.
func TestCheckFromMachine_LLM(t *testing.T) {
	m := Message{Type: TypeHello, Protocol: Protocol, MaxDirectives: 1, MaxLLM: 4, Capabilities: []string{CapLLM},
		LLMProvider: "anthropic", LLMModel: strings.Repeat("m", 300), LLM: LLMStateOK}
	if err := CheckFromMachine(&m); err != nil || len(m.LLMModel) > MaxLLMModelBytes {
		t.Errorf("hello: %v, model of %d bytes", err, len(m.LLMModel))
	}
	for name, m := range map[string]Message{
		"too many calls": {Type: TypeHello, Protocol: Protocol, MaxDirectives: 1, MaxLLM: MaxLLM + 1},
		"negative":       {Type: TypeHello, Protocol: Protocol, MaxDirectives: 1, MaxLLM: -1},
		"unknown state":  {Type: TypeCapabilities, LLM: "maybe"},
	} {
		if err := CheckFromMachine(&m); !errors.Is(err, ErrBadMessage) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestCheckLLMOutput(t *testing.T) {
	ok := []string{
		`{"content":"hi","stop_reason":"end_turn","model":"claude","usage":{"input_tokens":1,"output_tokens":2}}`,
		`{"content":"","tool_calls":[{"id":"a","name":"exec","input":{}},{"id":"b","name":"x","input":{"k":1}}],"stop_reason":"tool_use"}`,
	}
	for _, raw := range ok {
		if _, err := CheckLLMOutput(json.RawMessage(raw)); err != nil {
			t.Errorf("%s: %v", raw, err)
		}
	}
	calls := make([]string, MaxLLMToolCalls+1)
	for i := range calls {
		calls[i] = `{"id":"c` + strings.Repeat("x", i) + `","name":"t","input":{}}`
	}
	for name, raw := range map[string]string{
		"not JSON":          `{`,
		"unknown stop":      `{"stop_reason":"done"}`,
		"no stop reason":    `{"content":"hi"}`,
		"call without ID":   `{"tool_calls":[{"name":"t","input":{}}],"stop_reason":"tool_use"}`,
		"same ID twice":     `{"tool_calls":[{"id":"a","name":"t","input":{}},{"id":"a","name":"u","input":{}}],"stop_reason":"tool_use"}`,
		"input not object":  `{"tool_calls":[{"id":"a","name":"t","input":[1]}],"stop_reason":"tool_use"}`,
		"input missing":     `{"tool_calls":[{"id":"a","name":"t"}],"stop_reason":"tool_use"}`,
		"call without name": `{"tool_calls":[{"id":"a","input":{}}],"stop_reason":"tool_use"}`,
		"too many calls":    `{"tool_calls":[` + strings.Join(calls, ",") + `],"stop_reason":"tool_use"}`,
		"long model":        `{"stop_reason":"end_turn","model":"` + strings.Repeat("m", MaxLLMModelBytes+1) + `"}`,
		"too large":         `{"stop_reason":"end_turn","tool_calls":[{"id":"a","name":"t","input":{"x":"` + strings.Repeat("x", MaxLLMOutputBytes) + `"}}]}`,
		"text too long":     `{"stop_reason":"end_turn","content":"` + strings.Repeat("x", MaxLLMContentBytes+1) + `"}`,
	} {
		if _, err := CheckLLMOutput(json.RawMessage(raw)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestFamilyOf(t *testing.T) {
	for kind, want := range map[string]string{KindLLM: FamilyLLM, KindAnalyzeRepo: FamilyCoding, KindImplementFeature: FamilyCoding, KindEcho: FamilyCoding} {
		if got := FamilyOf(kind); got != want {
			t.Errorf("%s: %s", kind, got)
		}
	}
}
