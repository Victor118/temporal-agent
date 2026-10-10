package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// MachineStore is what the machine activities read and write.
type MachineStore interface {
	PickMachine(ctx context.Context, req store.PickRequest) (store.Directive, store.Machine, error)
	DirectiveOfCall(ctx context.Context, runID, callKey string) (store.Directive, store.Machine, error)
	ChooseLLMMachine(ctx context.Context, userID string, excluded []string, seenAfter time.Time) (store.Machine, error)
	SetMachineAside(ctx context.Context, id string) error
	CreateLLMDirective(ctx context.Context, req store.LLMDirectiveRequest) (store.Directive, store.Machine, error)
	StartDirective(ctx context.Context, id string, token []byte, activityID string, deadline time.Time) (store.Directive, error)
	CloseDirective(ctx context.Context, id, state, errText string) (bool, error)
}

// DirectiveHandoff hands a running directive to the gateway, which sends it
// to its machine: in process in dev (*gateway.Gateway), through the
// server's internal API otherwise (HTTPDirectiveHandoff). DeliverLLM hands
// a call to the model with its request, sent to the machine at once or
// never.
type DirectiveHandoff interface {
	Deliver(ctx context.Context, directiveID string) error
	DeliverLLM(ctx context.Context, directiveID string, request json.RawMessage) error
}

// MachineActivities choose a user's machine for a directive and run it
// there (docs/design/machines.md, §6 and §9). RunOnMachine completes
// asynchronously: the gateway ends it, with the machine's result. Not
// methods: Store, Handoff.
type MachineActivities struct {
	Store   MachineStore
	Handoff DirectiveHandoff
	// OnlineWindow: a machine whose gateway has not heard from it for
	// longer is offline (a gateway that died without a word). Zero =
	// DefaultMachineOnlineWindow.
	OnlineWindow time.Duration
	// Routing is where this worker sends coding runs (CodingRoute).
	Routing CodingRouting
	// Skills reads the skills a coding run takes along (PickMachine); nil =
	// this worker has none.
	Skills RunSkillReader
}

// CodingRouting is where a coding run goes, as the worker that publishes the
// coding tools is configured: to the user's machine (Machines), and when
// none takes it, to the fallback queue of its tool ("" = none): one per
// tool, so that the read-only identity and the one that pushes stay apart.
type CodingRouting struct {
	Machines       bool   `json:"machines"`
	AnalyzeQueue   string `json:"analyze_queue,omitempty"`
	ImplementQueue string `json:"implement_queue,omitempty"`
}

// CodingRoute tells a coding run where it may go: the worker's
// configuration, which a workflow cannot read itself, recorded in its
// history.
func (a *MachineActivities) CodingRoute(context.Context) (CodingRouting, error) {
	return a.Routing, nil
}

const (
	// DefaultMachineOnlineWindow is four of the gateway's heartbeats.
	DefaultMachineOnlineWindow = 2 * time.Minute
	// machineHandoffWindow is how long a reserved directive waits for its
	// RunOnMachine before the sweep closes it as orphaned.
	machineHandoffWindow = 10 * time.Minute
)

// PickMachineInput is a directive to reserve on a machine of UserID's.
type PickMachineInput struct {
	UserID string `json:"user_id"`
	// Capabilities are what the machine must have announced, all of them
	// (machine.CapabilitiesOf).
	Capabilities []string        `json:"capabilities"`
	Kind         string          `json:"kind"`
	Input        json.RawMessage `json:"input"`
	// CallKey tells the directive apart within its workflow run: a
	// PickMachine made again with it finds its directive.
	CallKey string `json:"call_key"`
	// Timeout is how long the directive may run (RunOnMachine's
	// StartToCloseTimeout).
	Timeout time.Duration `json:"timeout"`
	// The turn the directive works for, whose line shows its progress
	// (web only); empty: none.
	SessionID   string `json:"session_id,omitempty"`
	Participant string `json:"participant,omitempty"`
	Agent       string `json:"agent,omitempty"`
	// TurnKey, CallID and AgentID are where the files the machine
	// publishes go (store.File): the session turn and tool call of the run,
	// and its agent. Empty TurnKey: nowhere, they are refused.
	TurnKey string `json:"turn_key,omitempty"`
	CallID  string `json:"call_id,omitempty"`
	AgentID string `json:"agent_id,omitempty"`
	// Skills names the calling agent's skills for a coding run
	// (tool.CallContext.RunSkills): PickMachine adds them to Input, read
	// here (Skills of MachineActivities).
	Skills []string `json:"skills,omitempty"`
}

// PickMachineOutput is the directive and its machine; NoMachine, with no
// directive, says why none took it, and Refused why the directive could not
// be made (its skills past a run's bounds): no machine is reserved either
// way, and the run may go elsewhere.
type PickMachineOutput struct {
	DirectiveID string `json:"directive_id,omitempty"`
	MachineID   string `json:"machine_id,omitempty"`
	MachineName string `json:"machine_name,omitempty"`
	NoMachine   string `json:"no_machine,omitempty"`
	// Lacks, with NoMachine, is why none took it though one of the user's
	// could but for a capability the directive needs besides its kind's
	// (its CLI lacks --plugin-dir, for a run with skills): the run's
	// fallback says it.
	Lacks   string `json:"lacks,omitempty"`
	Refused string `json:"refused,omitempty"`
	// Skills are the names of the skills the directive carries;
	// SkillsMissing, those named that were not found here
	// ("name: reason"); SkillsVersion, what they were loaded from.
	Skills        []string `json:"skills,omitempty"`
	SkillsMissing []string `json:"skills_missing,omitempty"`
	SkillsVersion string   `json:"skills_version,omitempty"`
}

// PickMachine chooses a machine of the user's (online, not paused, with the
// capability, the highest priority, then the least busy, under its cap)
// and creates the directive, in one transaction (store.PickMachine). No
// machine is a result, not an error: retrying would not make one appear.
// The directive's input is final before: the skills it names are read here
// and added to it (what the gateway sends, at once and at every hello, is
// what the database holds), and the machine must then load them
// (machine.CapRunSkills). Skills it cannot carry are a refusal, a result
// too, unless the call's directive exists already. Made again for the same
// call, it finds the first one's directive, and says of its skills what
// that one's input holds.
func (a *MachineActivities) PickMachine(ctx context.Context, in PickMachineInput) (PickMachineOutput, error) {
	var out PickMachineOutput
	var extra []string
	if len(in.Skills) > 0 {
		input, refused, err := a.withSkills(in, &out)
		if err != nil {
			return PickMachineOutput{}, err
		}
		if refused != "" {
			// An earlier attempt may have made the call's directive, with
			// the skills as they were: it holds a slot of its machine, and
			// is the call's.
			run := activity.GetInfo(ctx).WorkflowExecution.RunID
			d, m, err := a.Store.DirectiveOfCall(ctx, run, in.CallKey)
			switch {
			case err == nil:
				out := storedSkills(d.Input, in.Skills)
				out.DirectiveID, out.MachineID, out.MachineName = d.ID, m.ID, m.Name
				return out, nil
			case !errors.Is(err, store.ErrDirectiveNotFound):
				return PickMachineOutput{}, err
			}
			return PickMachineOutput{Refused: refused, SkillsMissing: out.SkillsMissing}, nil
		}
		in.Input = input
		if len(out.Skills) > 0 {
			extra = []string{machine.CapRunSkills}
		}
	}
	info := activity.GetInfo(ctx)
	window := a.onlineWindow()
	now := time.Now()
	id := uuid.NewString()
	d, m, err := a.Store.PickMachine(ctx, store.PickRequest{
		DirectiveID:  id,
		UserID:       in.UserID,
		Capabilities: in.Capabilities,
		Extra:        extra,
		Kind:         in.Kind,
		Input:        in.Input,
		WorkflowID:   info.WorkflowExecution.ID,
		RunID:        info.WorkflowExecution.RunID,
		CallKey:      in.CallKey,
		HandoffBy:    now.Add(machineHandoffWindow),
		Deadline:     now.Add(machineHandoffWindow + in.Timeout),
		SeenAfter:    now.Add(-window),
		SessionID:    in.SessionID,
		Participant:  in.Participant,
		Agent:        in.Agent,
		TurnKey:      in.TurnKey,
		CallID:       in.CallID,
		AgentID:      in.AgentID,
	})
	var lacks *store.MachineLacksError
	switch {
	case errors.As(err, &lacks):
		return PickMachineOutput{NoMachine: lacksText(lacks), Lacks: lacksText(lacks)}, nil
	case errors.Is(err, store.ErrNoMachine):
		return PickMachineOutput{NoMachine: fmt.Sprintf(
			"no machine of yours is connected with %s and a directive to spare", strings.Join(append(in.Capabilities, extra...), ", "))}, nil
	case err != nil:
		return PickMachineOutput{}, err
	}
	if d.ID != id && len(in.Skills) > 0 {
		// The call's directive, made by an earlier attempt: its input says
		// what went with it, not what the skills are now.
		out = storedSkills(d.Input, in.Skills)
	}
	out.DirectiveID, out.MachineID, out.MachineName = d.ID, m.ID, m.Name
	return out, nil
}

// lacksText says why no machine took a directive that one could have run
// but for what it lacks.
func lacksText(e *store.MachineLacksError) string {
	if slices.Contains(e.Missing, machine.CapRunSkills) {
		return fmt.Sprintf("your machine %q has Claude Code but its CLI lacks %s, which the agent's skills need (update the claude CLI, then restart agent connect)",
			e.Machine, claudecode.PluginDirFlag)
	}
	return fmt.Sprintf("your machine %q lacks %s", e.Machine, strings.Join(e.Missing, ", "))
}

// storedSkills is what a directive's input says of its skills: those it
// carries, its version, and those of named it does not (not found when it
// was made).
func storedSkills(input json.RawMessage, named []string) PickMachineOutput {
	var stored struct {
		Skills        []machine.RunSkill `json:"skills"`
		SkillsVersion string             `json:"skills_version"`
	}
	json.Unmarshal(input, &stored)
	out := PickMachineOutput{SkillsVersion: stored.SkillsVersion}
	for _, s := range stored.Skills {
		out.Skills = append(out.Skills, s.Name)
	}
	for _, name := range named {
		if !slices.Contains(out.Skills, name) && !slices.Contains(out.SkillsMissing, name+": "+machine.SkillNotFound) {
			out.SkillsMissing = append(out.SkillsMissing, name+": "+machine.SkillNotFound)
		}
	}
	return out
}

// withSkills is in's input with the skills it names (machine.RunSkill, in
// its "skills", and what they were loaded from, in its "skills_version"),
// and fills out with what they are; or why they cannot go (refused): past a
// run's bounds, a name no plugin can carry. A name not found is said in
// out, never in silence, and the run goes without it.
func (a *MachineActivities) withSkills(in PickMachineInput, out *PickMachineOutput) (json.RawMessage, string, error) {
	var set RunSkillSet
	if a.Skills != nil {
		set = a.Skills.RunSkills(in.Skills)
	} else {
		set.Missing = in.Skills
	}
	for _, name := range set.Missing {
		out.SkillsMissing = append(out.SkillsMissing, name+": "+machine.SkillNotFound)
	}
	if err := machine.CheckRunSkills(set.Skills); err != nil {
		return nil, fmt.Sprintf("the agent's skills cannot go with the run (%v)", err), nil
	}
	if len(set.Skills) == 0 {
		return in.Input, "", nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(in.Input, &fields); err != nil {
		return nil, "", temporal.NewNonRetryableApplicationError(fmt.Sprintf("the directive's input: %v", err), "InvalidInput", nil)
	}
	skills, err := json.Marshal(set.Skills)
	if err != nil {
		return nil, "", err
	}
	fields["skills"] = skills
	if set.Version != "" {
		fields["skills_version"], _ = json.Marshal(set.Version)
	}
	input, err := json.Marshal(fields)
	if err != nil {
		return nil, "", err
	}
	for _, s := range set.Skills {
		out.Skills = append(out.Skills, s.Name)
	}
	out.SkillsVersion = set.Version
	return input, "", nil
}

// RunOnMachineInput names the directive PickMachine created.
type RunOnMachineInput struct {
	DirectiveID string `json:"directive_id"`
}

// RunOnMachine sets the directive's task token, hands it to the gateway, and
// returns at once, pending: the gateway records its heartbeats while the
// machine answers, and completes it with the machine's result. No worker
// slot is held during a run. The task token never leaves the private
// network: the machine knows the directive's ID alone.
func (a *MachineActivities) RunOnMachine(ctx context.Context, in RunOnMachineInput) (machine.Result, error) {
	info := activity.GetInfo(ctx)
	d, err := a.Store.StartDirective(ctx, in.DirectiveID, info.TaskToken, info.ActivityID, info.Deadline)
	switch {
	case errors.Is(err, store.ErrDirectiveClosed) && d.State == store.DirectiveRevoked:
		return machine.Result{}, temporal.NewNonRetryableApplicationError("the machine was revoked before the directive started", machine.ErrTypeRevoked, nil)
	case errors.Is(err, store.ErrDirectiveClosed), errors.Is(err, store.ErrDirectiveNotFound):
		return machine.Result{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("the directive was closed before it started (%s)", d.State), machine.ErrTypeClosed, nil)
	case err != nil:
		return machine.Result{}, err
	}
	if err := a.deliver(ctx, d.ID); err != nil {
		// Never handed: nobody will run it, nor end it.
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a.Store.CloseDirective(closeCtx, d.ID, store.DirectiveFailed, "not handed to the gateway")
		// Nothing ran: the caller may take it elsewhere.
		return machine.Result{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("the directive could not be handed to its machine: %v", err), machine.ErrTypeRefused, nil)
	}
	return machine.Result{}, activity.ErrResultPending
}

// deliver tries the handoff a few times: the server may be restarting.
func (a *MachineActivities) deliver(ctx context.Context, id string) error {
	if a.Handoff == nil {
		return fmt.Errorf("%w: this worker hands no directive (no gateway)", errHandoffRefused)
	}
	var err error
	for i, wait := 0, time.Second; i < 4; i, wait = i+1, wait*2 {
		if err = a.Handoff.Deliver(ctx, id); err == nil || errors.Is(err, errHandoffRefused) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
	return err
}

// errHandoffRefused is a handoff the server refused for good (its key, or
// a directive it does not hold open): trying again would not help.
var errHandoffRefused = errors.New("handoff refused")

// HTTPDirectiveHandoff hands directives to the server's gateway through its
// internal API, as a worker running apart from it does (HTTPNotifier's
// sibling, with the same INTERNAL_API_KEY).
type HTTPDirectiveHandoff struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
}

func NewHTTPDirectiveHandoff(baseURL, apiKey string) *HTTPDirectiveHandoff {
	return &HTTPDirectiveHandoff{BaseURL: baseURL, APIKey: apiKey, Client: &http.Client{Timeout: 10 * time.Second}}
}

// DirectivePath is the server's internal route of the handoff.
const DirectivePath = "/internal/machines/directives"

// DirectiveHandoffInput is the handoff's body; Request, a call to the
// model's (DeliverLLM).
type DirectiveHandoffInput struct {
	DirectiveID string          `json:"directive_id"`
	Request     json.RawMessage `json:"request,omitempty"`
}

// DeliverLLM hands a call to the model with its request: one try, within
// machine.LLMHandoffTimeout (the gateway writes it to the machine within
// machine.LLMWriteTimeout). A failure is the step's: its workflow decides.
func (h *HTTPDirectiveHandoff) DeliverLLM(ctx context.Context, directiveID string, request json.RawMessage) error {
	body, err := json.Marshal(DirectiveHandoffInput{DirectiveID: directiveID, Request: request})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, machine.LLMHandoffTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.BaseURL+DirectivePath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.APIKey)
	// Not h.Client, bounded for a small body: the context bounds this one.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusFailedDependency:
		// The gateway closed the directive.
		return machine.ErrUnreachable
	case resp.StatusCode == http.StatusUnauthorized:
		return fmt.Errorf("%w: the server refused this worker's INTERNAL_API_KEY", errHandoffRefused)
	case resp.StatusCode/100 != 2:
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func (h *HTTPDirectiveHandoff) Deliver(ctx context.Context, directiveID string) error {
	body, err := json.Marshal(DirectiveHandoffInput{DirectiveID: directiveID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.BaseURL+DirectivePath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.APIKey)
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return fmt.Errorf("%w: the server refused this worker's INTERNAL_API_KEY", errHandoffRefused)
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusConflict:
		return fmt.Errorf("%w: status %d", errHandoffRefused, resp.StatusCode)
	case resp.StatusCode/100 != 2:
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
