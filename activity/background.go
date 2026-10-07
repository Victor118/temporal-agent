package activity

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/victor/temporal-agent/tool"
)

// BackgroundField is the input field the catalog adds to the tools a call
// may launch in the background (docs/design/async-tasks.md §3): the model
// chooses, call by call. The dispatch takes it out of the input before the
// tool reads it (TakeBackground); the stored tool call keeps it, so that
// the model reads its choice again.
const BackgroundField = "background"

// backgroundDescription is what the model reads of BackgroundField.
const backgroundDescription = "Run this call in the background: it starts, you get its task ID at once and your turn goes on, " +
	"and when it ends a message brings you its result in a new turn. Use it for long work when the user may have something " +
	"else to ask you meanwhile, or when they ask for it. Leave it out to wait for the result, the default."

// MaxRunningTasks bounds the background tasks one participant runs at once.
const MaxRunningTasks = 3

// Backgroundable reports whether a tool's calls may run in the background:
// a workflow tool, whose input is not private (its result would go to a
// message every member reads), whose schema does not use BackgroundField
// for itself. An activity tool is short: always waited for.
func Backgroundable(kind string, private bool, schema json.RawMessage) bool {
	return kind == string(tool.ToolKindWorkflow) && !private && !hasProperty(schema, BackgroundField)
}

// hasProperty reports whether a JSON schema declares a property.
func hasProperty(schema json.RawMessage, name string) bool {
	var s struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if json.Unmarshal(schema, &s) != nil {
		return false
	}
	_, ok := s.Properties[name]
	return ok
}

// WithBackgroundField is schema with BackgroundField among its properties,
// optional. A schema that is no JSON object is returned as it is. The same
// schema gives the same bytes: the tool definitions are part of a cached
// prefix.
func WithBackgroundField(schema json.RawMessage) json.RawMessage {
	var fields map[string]json.RawMessage
	if json.Unmarshal(schema, &fields) != nil || fields == nil {
		return schema
	}
	var props map[string]json.RawMessage
	if raw, ok := fields["properties"]; ok {
		if json.Unmarshal(raw, &props) != nil || props == nil {
			return schema
		}
	} else {
		props = map[string]json.RawMessage{}
	}
	field, _ := json.Marshal(map[string]string{"type": "boolean", "description": backgroundDescription})
	props[BackgroundField] = field
	fields["properties"], _ = json.Marshal(props)
	out, err := json.Marshal(fields)
	if err != nil {
		return schema
	}
	return out
}

// TakeBackground takes BackgroundField out of a tool call's input: the
// input the tool reads, and whether the model asked for the background.
// An input that is no JSON object is left as it is (the tool refuses it);
// a field that is no boolean is an error for the model.
func TakeBackground(input json.RawMessage) (json.RawMessage, bool, error) {
	if !bytes.Contains(input, []byte(`"`+BackgroundField+`"`)) {
		return input, false, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(input, &fields) != nil || fields == nil {
		return input, false, nil
	}
	raw, ok := fields[BackgroundField]
	if !ok {
		return input, false, nil
	}
	var background bool
	if err := json.Unmarshal(raw, &background); err != nil {
		return nil, false, fmt.Errorf("%s must be true or false", BackgroundField)
	}
	delete(fields, BackgroundField)
	out, err := json.Marshal(fields)
	if err != nil {
		return nil, false, err
	}
	return out, background, nil
}
