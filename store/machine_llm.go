package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// The model on a machine (docs/design/machine-llm.md): a turn chooses one of
// its author's machines (ChooseLLMMachine), then each call to the model is a
// directive on it (CreateLLMDirective), under the machine's cap of calls,
// counted apart from its coding runs.

// llmKind is the kind of a call to the model (machine.KindLLM): counted
// apart, under the machine's MaxLLM.
const llmKind = "llm"

// llmCapability is what a machine announces when it calls a model
// (machine.CapLLM).
const llmCapability = "llm"

// queryer is what reads rows: the database, or a transaction.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// openLLMCalls counts the open calls to the model of each machine of ids.
func openLLMCalls(ctx context.Context, q queryer, ids []string) (map[string]int, error) {
	rows, err := q.QueryContext(ctx, `SELECT machine_id, COUNT(*) FROM machine_directives
		WHERE machine_id = ANY($1) AND state IN ('reserved', 'running') AND kind = 'llm' GROUP BY machine_id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	open := map[string]int{}
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		open[id] = n
	}
	return open, rows.Err()
}

// llmUnavailable says why m cannot take a call to the model now, "" when it
// can: online since seenAfter, not paused, its model offered, not set aside,
// under its cap with open calls already.
func llmUnavailable(m Machine, open int, seenAfter time.Time) string {
	switch {
	case !m.Online(seenAfter):
		return "offline"
	case m.Paused:
		return "paused"
	case !m.Can(llmCapability):
		return "its model is not offered"
	case m.AsideForLLM():
		return "set aside since a call to its model was lost"
	case open >= m.MaxLLM:
		return "at its cap of calls to the model"
	}
	return ""
}

// ChooseLLMMachine chooses a machine of userID's for a turn's model, and
// creates nothing: online since seenAfter, not paused, with its model, not
// set aside, under its cap of calls, none of excluded; the highest priority,
// then the least busy with calls to the model. None: ErrNoMachine.
func (s *PostgresStore) ChooseLLMMachine(ctx context.Context, userID string, excluded []string, seenAfter time.Time) (Machine, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+machineColumns+` FROM machines m
		WHERE m.user_id = $1 AND m.revoked_at IS NULL ORDER BY m.id`, userID)
	if err != nil {
		return Machine{}, err
	}
	var machines []Machine
	for rows.Next() {
		m, err := scanMachine(rows)
		if err != nil {
			rows.Close()
			return Machine{}, err
		}
		machines = append(machines, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Machine{}, err
	}
	ids := make([]string, len(machines))
	for i, m := range machines {
		ids[i] = m.ID
	}
	open, err := openLLMCalls(ctx, s.db, ids)
	if err != nil {
		return Machine{}, err
	}
	var choices []machineChoice
	for _, m := range machines {
		if slices.Contains(excluded, m.ID) || llmUnavailable(m, open[m.ID], seenAfter) != "" {
			continue
		}
		choices = append(choices, machineChoice{ID: m.ID, Priority: m.Priority, Max: m.MaxLLM, Open: open[m.ID], Online: true, Can: true})
	}
	if chosen := chooseMachine(choices); chosen != "" {
		for _, m := range machines {
			if m.ID == chosen {
				return m, nil
			}
		}
	}
	return Machine{}, ErrNoMachine
}

// LLMDirectiveRequest is one call to the model, on the machine its turn
// chose.
type LLMDirectiveRequest struct {
	DirectiveID string
	MachineID   string
	UserID      string
	// Input is what goes back to the workflow with the answer
	// (machine.LLMInput): never the request.
	Input      json.RawMessage
	WorkflowID string
	RunID      string
	ActivityID string
	// CallKey is "llm:<step>:<attempt>": each attempt is a directive of its
	// own.
	CallKey   string
	TaskToken []byte
	// Deadline is the activity's; SeenAfter: a machine not heard from since
	// is offline.
	Deadline  time.Time
	SeenAfter time.Time
	// The turn the call works for: traceability only (no note, no file).
	SessionID string
	TurnKey   string
	AgentID   string
}

// CreateLLMDirective creates a call to the model on its machine, running at
// once (its task token set: the activity hands it over right after), in one
// transaction that holds the machine's row: under its cap of calls, the
// machine online, not paused, with its model, not set aside. Otherwise
// ErrMachineUnavailable, saying why; a key used already: ErrDirectiveClosed
// (an attempt is never made twice). One whose activity never hands it over
// (its worker died) expires at its deadline, swept.
func (s *PostgresStore) CreateLLMDirective(ctx context.Context, req LLMDirectiveRequest) (Directive, Machine, error) {
	var d Directive
	var m Machine
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var err error
		m, err = scanMachine(tx.QueryRowContext(ctx, `SELECT `+machineColumns+` FROM machines m
			WHERE m.id = $1 AND m.user_id = $2 AND m.revoked_at IS NULL FOR UPDATE`, req.MachineID, req.UserID))
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: no such machine of the user's (revoked?)", ErrMachineUnavailable)
		}
		if err != nil {
			return err
		}
		var used bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM machine_directives WHERE run_id = $1 AND call_key = $2)`,
			req.RunID, req.CallKey).Scan(&used); err != nil {
			return err
		}
		if used {
			return ErrDirectiveClosed
		}
		open, err := openLLMCalls(ctx, tx, []string{m.ID})
		if err != nil {
			return err
		}
		if why := llmUnavailable(m, open[m.ID], req.SeenAfter); why != "" {
			return fmt.Errorf("%w: %s", ErrMachineUnavailable, why)
		}
		d, err = scanDirective(tx.QueryRowContext(ctx, `
			INSERT INTO machine_directives (id, machine_id, user_id, kind, input, workflow_id, run_id, activity_id, call_key,
				task_token, state, handoff_by, deadline, started_at, session_id, turn_key, agent_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'running', NOW(), $11, NOW(), $12, $13, $14)
			RETURNING `+directiveColumns,
			req.DirectiveID, m.ID, req.UserID, llmKind, []byte(req.Input), req.WorkflowID, req.RunID, req.ActivityID, req.CallKey,
			req.TaskToken, req.Deadline, req.SessionID, req.TurnKey, req.AgentID))
		if isUniqueViolation(err) {
			return ErrDirectiveClosed
		}
		return err
	})
	return d, m, err
}
