package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"
)

// Machine is a user's machine, enrolled to run directives for them
// (docs/design/machines.md). Only its token's hash is stored, and the hash
// of the one it replaces while the machine has not confirmed it wrote the
// new one.
type Machine struct {
	ID            string
	UserID        string
	Name          string
	OS            string
	Capabilities  []string
	MaxDirectives int
	Priority      int
	Paused        bool
	AgentVersion  string
	// ConnectedTo is the gateway holding its connection; "" = offline.
	ConnectedTo string
	LastAddr    string
	CreatedAt   time.Time
	SeenAt      *time.Time
	RevokedAt   *time.Time
	// RevokedReason says why, for its owner ("jeton réutilisé"…).
	RevokedReason string
	// ClaudeCode is the state of its claude CLI, as it last said:
	// "ok", "logged_out", "absent"; "" = never said.
	ClaudeCode string
	// OpenDirectives is how many directives it holds, and OpenKinds their
	// kinds: ListMachines only.
	OpenDirectives int
	OpenKinds      []string
}

// Online reports a machine a gateway holds and has heard from since since.
func (m Machine) Online(since time.Time) bool {
	return m.ConnectedTo != "" && m.RevokedAt == nil && m.SeenAt != nil && m.SeenAt.After(since)
}

// Can reports a capability the machine announced.
func (m Machine) Can(capability string) bool { return slices.Contains(m.Capabilities, capability) }

// MachineInfo is what a machine says of itself.
type MachineInfo struct {
	Name          string
	OS            string
	Capabilities  []string
	MaxDirectives int
	AgentVersion  string
	ClaudeCode    string
}

// Enrollment kinds: a device request, approved by its user code, or an
// enrollment token, created by a logged-in user for a script.
const (
	EnrollmentDevice = "device"
	EnrollmentToken  = "token"
)

// MachineEnrollment is a machine on its way in: a device request waiting for
// its approval (RFC 8628), or an enrollment token waiting for its machine.
// SecretHash is the hash of what the machine presents to get its token.
type MachineEnrollment struct {
	ID         string
	Kind       string
	SecretHash string
	UserCode   string // device requests only
	Info       MachineInfo
	ClientAddr string
	CreatedAt  time.Time
	ExpiresAt  time.Time
	ApprovedBy string
	MachineID  string // once the machine has its token
}

var (
	// ErrUserCodeTaken: draw another code.
	ErrUserCodeTaken = errors.New("user code taken")
	// ErrEnrollmentUnknown is no such pending enrollment (or one used).
	ErrEnrollmentUnknown = errors.New("unknown enrollment")
	// ErrEnrollmentExpired is an enrollment past its expiry.
	ErrEnrollmentExpired = errors.New("enrollment expired")
	// ErrEnrollmentPending is a device request nobody approved yet.
	ErrEnrollmentPending = errors.New("enrollment not approved yet")
	// ErrMachineNotFound is no such machine, or one of another user.
	ErrMachineNotFound = errors.New("machine not found")
	// ErrTokenRefused is a token that is no longer the machine's.
	ErrTokenRefused = errors.New("machine token refused")
	// ErrNoMachine: no machine of the user can take the directive now.
	ErrNoMachine = errors.New("no machine available")
	// ErrDirectiveClosed is a directive that is over.
	ErrDirectiveClosed = errors.New("directive closed")
	// ErrDirectiveNotFound is no such directive.
	ErrDirectiveNotFound = errors.New("directive not found")
)

// TokenUse is what a presented machine token is.
type TokenUse int

const (
	TokenUnknown TokenUse = iota
	// TokenCurrent is the machine's token.
	TokenCurrent
	// TokenPending is the one the current replaces, still valid until the
	// machine confirms it wrote the current one.
	TokenPending
	// TokenRetired is a token replaced for good: presenting it again shows a
	// copy of it exists.
	TokenRetired
)

// Directive states. A directive is open while reserved (created by
// PickMachine) or running (its activity's task token set); every other
// state is an end. A machine's load is its open directives.
const (
	DirectiveReserved = "reserved"
	DirectiveRunning  = "running"

	DirectiveCompleted = "completed"
	DirectiveFailed    = "failed"
	DirectiveCanceled  = "canceled"
	DirectiveStopping  = "machine_stopping"
	DirectiveLost      = "lost"
	DirectiveRevoked   = "revoked"
	DirectiveOrphaned  = "orphaned"
	DirectiveExpired   = "expired"
	// DirectiveGone is a directive whose activity no longer exists
	// (NotFound): its workflow ended, or Temporal timed it out.
	DirectiveGone = "gone"
)

// Directive is a task for a machine. Reservation and directive are one row:
// PickMachine creates it, RunOnMachine sets its task token, and any end is
// one update of its state.
type Directive struct {
	ID         string
	MachineID  string
	UserID     string
	Kind       string
	Input      json.RawMessage
	WorkflowID string
	RunID      string
	ActivityID string
	CallKey    string
	TaskToken  []byte
	State      string
	// HandoffBy is when a reserved directive must have its task token, or be
	// swept; Deadline when a running one ends at the latest.
	HandoffBy time.Time
	Deadline  time.Time
	// SentConn is the connection the gateway sent it on; "" = not sent.
	SentConn string
	// SessionID, Participant and Agent are the turn it works for, whose
	// line shows its progress (empty: none).
	SessionID   string
	Participant string
	Agent       string
	Result      json.RawMessage
	Error       string
	CreatedAt   time.Time
	StartedAt   *time.Time
	ClosedAt    *time.Time
}

// Open reports a directive that is not over.
func (d Directive) Open() bool { return d.State == DirectiveReserved || d.State == DirectiveRunning }

// PickRequest is a directive to reserve on one of a user's machines.
type PickRequest struct {
	DirectiveID string // the new directive's ID, unless the call made one already
	UserID      string
	Capability  string
	Kind        string
	Input       json.RawMessage
	WorkflowID  string
	RunID       string
	CallKey     string
	HandoffBy   time.Time
	Deadline    time.Time
	// SeenAfter: a machine not heard from since is offline.
	SeenAfter time.Time
	// The turn the directive works for (Directive.SessionID…).
	SessionID   string
	Participant string
	Agent       string
}

const machineSchema = `
		-- A user's machine (docs/design/machines.md). token_hash is its token's
		-- hash; prev_token_hash the one it replaces, still valid until the
		-- machine confirms it wrote the new one. Both are NULL once revoked.
		CREATE TABLE IF NOT EXISTS machines (
			id              TEXT PRIMARY KEY,
			user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			name            TEXT NOT NULL,
			os              TEXT NOT NULL DEFAULT '',
			capabilities    JSONB NOT NULL DEFAULT '[]',
			max_directives  INTEGER NOT NULL DEFAULT 1 CHECK (max_directives >= 1),
			priority        INTEGER NOT NULL DEFAULT 0,
			paused          BOOLEAN NOT NULL DEFAULT FALSE,
			agent_version   TEXT NOT NULL DEFAULT '',
			token_hash      TEXT UNIQUE,
			prev_token_hash TEXT UNIQUE,
			connected_to    TEXT NOT NULL DEFAULT '',
			last_addr       TEXT NOT NULL DEFAULT '',
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			seen_at         TIMESTAMPTZ,
			revoked_at      TIMESTAMPTZ,
			revoked_reason  TEXT NOT NULL DEFAULT ''
		);
		CREATE INDEX IF NOT EXISTS idx_machines_user ON machines(user_id);

		-- Tokens a machine replaced: one presented again reveals a copy, and
		-- revokes the machine.
		CREATE TABLE IF NOT EXISTS machine_retired_tokens (
			token_hash TEXT PRIMARY KEY,
			machine_id TEXT NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
			retired_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		-- A directive: a task for a machine, reserved and created in one
		-- transaction by PickMachine, keyed by its workflow run and call so a
		-- replay finds it. Open while reserved or running; a machine's load is
		-- its open rows, nothing to count apart.
		CREATE TABLE IF NOT EXISTS machine_directives (
			id          TEXT PRIMARY KEY,
			machine_id  TEXT NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
			user_id     TEXT NOT NULL,
			kind        TEXT NOT NULL,
			input       JSONB NOT NULL,
			workflow_id TEXT NOT NULL,
			run_id      TEXT NOT NULL,
			activity_id TEXT NOT NULL DEFAULT '',
			call_key    TEXT NOT NULL,
			task_token  BYTEA,
			state       TEXT NOT NULL DEFAULT 'reserved',
			handoff_by  TIMESTAMPTZ NOT NULL,
			deadline    TIMESTAMPTZ NOT NULL,
			sent_conn   TEXT NOT NULL DEFAULT '',
			result      JSONB,
			error       TEXT NOT NULL DEFAULT '',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at  TIMESTAMPTZ,
			closed_at   TIMESTAMPTZ
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_machine_directives_call ON machine_directives(run_id, call_key);
		CREATE INDEX IF NOT EXISTS idx_machine_directives_open ON machine_directives(machine_id)
			WHERE state IN ('reserved', 'running');

		-- A machine on its way in: a device request (user code, approved by a
		-- logged-in user) or an enrollment token (created by one). Only the hash
		-- of the machine's secret is kept.
		-- What its claude CLI is ("ok", "logged_out", "absent"), as it says.
		ALTER TABLE machines ADD COLUMN IF NOT EXISTS claude_code TEXT NOT NULL DEFAULT '';
		-- The turn a directive works for: where its progress is shown.
		ALTER TABLE machine_directives ADD COLUMN IF NOT EXISTS session_id TEXT NOT NULL DEFAULT '';
		ALTER TABLE machine_directives ADD COLUMN IF NOT EXISTS participant TEXT NOT NULL DEFAULT '';
		ALTER TABLE machine_directives ADD COLUMN IF NOT EXISTS agent TEXT NOT NULL DEFAULT '';

		CREATE TABLE IF NOT EXISTS machine_enrollments (
			id            TEXT PRIMARY KEY,
			kind          TEXT NOT NULL CHECK (kind IN ('device', 'token')),
			secret_hash   TEXT NOT NULL UNIQUE,
			user_code     TEXT UNIQUE,
			name          TEXT NOT NULL DEFAULT '',
			os            TEXT NOT NULL DEFAULT '',
			capabilities  JSONB NOT NULL DEFAULT '[]',
			agent_version TEXT NOT NULL DEFAULT '',
			max_directives INTEGER NOT NULL DEFAULT 1 CHECK (max_directives >= 1),
			client_addr   TEXT NOT NULL DEFAULT '',
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at    TIMESTAMPTZ NOT NULL,
			approved_by   TEXT REFERENCES users(id) ON DELETE CASCADE,
			approved_at   TIMESTAMPTZ,
			machine_id    TEXT NOT NULL DEFAULT ''
		);
`

// --- Enrollment ---

// CreateMachineEnrollment stores a device request or an enrollment token. A
// user code already taken is ErrUserCodeTaken.
func (s *PostgresStore) CreateMachineEnrollment(ctx context.Context, e MachineEnrollment) error {
	caps, err := json.Marshal(nonNil(e.Info.Capabilities))
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO machine_enrollments (id, kind, secret_hash, user_code, name, os, capabilities, agent_version,
			max_directives, client_addr, expires_at, approved_by, approved_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''),
			CASE WHEN $12 = '' THEN NULL ELSE NOW() END)`,
		e.ID, e.Kind, e.SecretHash, e.UserCode, e.Info.Name, e.Info.OS, caps, e.Info.AgentVersion,
		max(e.Info.MaxDirectives, 1), e.ClientAddr, e.ExpiresAt, e.ApprovedBy)
	if isUniqueViolation(err) && e.UserCode != "" {
		return ErrUserCodeTaken
	}
	return err
}

// CountPendingDeviceRequests counts the device requests not expired yet: the
// route that creates them needs no login, so their number is bounded.
func (s *PostgresStore) CountPendingDeviceRequests(ctx context.Context, now time.Time) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM machine_enrollments
		WHERE kind = 'device' AND expires_at > $1 AND machine_id = ''`, now).Scan(&n)
	return n, err
}

const enrollmentColumns = `id, kind, secret_hash, COALESCE(user_code, ''), name, os, capabilities, agent_version,
	max_directives, client_addr, created_at, expires_at, COALESCE(approved_by, ''), machine_id`

func scanEnrollment(row interface{ Scan(...any) error }) (*MachineEnrollment, error) {
	var e MachineEnrollment
	var caps []byte
	err := row.Scan(&e.ID, &e.Kind, &e.SecretHash, &e.UserCode, &e.Info.Name, &e.Info.OS, &caps, &e.Info.AgentVersion,
		&e.Info.MaxDirectives, &e.ClientAddr, &e.CreatedAt, &e.ExpiresAt, &e.ApprovedBy, &e.MachineID)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(caps, &e.Info.Capabilities); err != nil {
		return nil, err
	}
	return &e, nil
}

// FindDeviceRequest returns the device request of a user code, still waiting
// for its approval; nil when there is none.
func (s *PostgresStore) FindDeviceRequest(ctx context.Context, userCode string, now time.Time) (*MachineEnrollment, error) {
	e, err := scanEnrollment(s.db.QueryRowContext(ctx, `SELECT `+enrollmentColumns+` FROM machine_enrollments
		WHERE kind = 'device' AND user_code = $1 AND expires_at > $2 AND approved_by IS NULL`, userCode, now))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return e, err
}

// ApproveDeviceRequest attaches a device request to the user who typed its
// code: its machine will be theirs. One no longer pending (expired,
// approved) is ErrEnrollmentUnknown.
func (s *PostgresStore) ApproveDeviceRequest(ctx context.Context, id, userCode, userID string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE machine_enrollments SET approved_by = $3, approved_at = NOW()
		WHERE id = $1 AND kind = 'device' AND user_code = $2 AND expires_at > $4 AND approved_by IS NULL`,
		id, userCode, userID, now)
	return affectedOne(res, err, ErrEnrollmentUnknown)
}

// RedeemMachineEnrollment gives an approved enrollment its machine, once:
// m is the new machine (its ID and token hash); its user, and for a device
// request its description, come from the enrollment. A device request
// keeps what the machine said when it asked; a token takes info, said now.
// Unknown or used: ErrEnrollmentUnknown; ErrEnrollmentExpired;
// ErrEnrollmentPending while nobody approved it.
func (s *PostgresStore) RedeemMachineEnrollment(ctx context.Context, kind, secretHash string, info MachineInfo, m Machine, tokenHash string, now time.Time) (Machine, error) {
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		e, err := scanEnrollment(tx.QueryRowContext(ctx, `SELECT `+enrollmentColumns+` FROM machine_enrollments
			WHERE kind = $1 AND secret_hash = $2 FOR UPDATE`, kind, secretHash))
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrEnrollmentUnknown
		case err != nil:
			return err
		case e.MachineID != "":
			return ErrEnrollmentUnknown
		case !e.ExpiresAt.After(now):
			return ErrEnrollmentExpired
		case e.ApprovedBy == "":
			return ErrEnrollmentPending
		}
		if kind == EnrollmentDevice {
			info = e.Info
		}
		m.UserID = e.ApprovedBy
		m.Name = info.Name
		m.OS = info.OS
		m.Capabilities = nonNil(info.Capabilities)
		m.AgentVersion = info.AgentVersion
		m.MaxDirectives = max(info.MaxDirectives, 1)
		caps, err := json.Marshal(m.Capabilities)
		if err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO machines (id, user_id, name, os, capabilities, max_directives, agent_version, token_hash)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING created_at`,
			m.ID, m.UserID, m.Name, m.OS, caps, m.MaxDirectives, m.AgentVersion, tokenHash).Scan(&m.CreatedAt); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE machine_enrollments SET machine_id = $2 WHERE id = $1`, e.ID, m.ID)
		return err
	})
	return m, err
}

// DeleteExpiredEnrollments forgets the enrollments past their expiry, used or
// not.
func (s *PostgresStore) DeleteExpiredEnrollments(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM machine_enrollments WHERE expires_at <= $1`, now)
	return err
}

// --- Machines ---

const machineColumns = `m.id, m.user_id, m.name, m.os, m.capabilities, m.max_directives, m.priority, m.paused,
	m.agent_version, m.connected_to, m.last_addr, m.created_at, m.seen_at, m.revoked_at, m.revoked_reason, m.claude_code`

func scanMachine(row interface{ Scan(...any) error }, extra ...any) (Machine, error) {
	var m Machine
	var caps []byte
	dest := append([]any{&m.ID, &m.UserID, &m.Name, &m.OS, &caps, &m.MaxDirectives, &m.Priority, &m.Paused,
		&m.AgentVersion, &m.ConnectedTo, &m.LastAddr, &m.CreatedAt, &m.SeenAt, &m.RevokedAt, &m.RevokedReason, &m.ClaudeCode}, extra...)
	if err := row.Scan(dest...); err != nil {
		return m, err
	}
	return m, json.Unmarshal(caps, &m.Capabilities)
}

// ListMachines returns a user's machines, revoked ones included, with their
// open directives, the newest first.
func (s *PostgresStore) ListMachines(ctx context.Context, userID string) ([]Machine, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+machineColumns+`,
			(SELECT COALESCE(json_agg(d.kind ORDER BY d.created_at), '[]') FROM machine_directives d
				WHERE d.machine_id = m.id AND d.state IN ('reserved', 'running'))
		FROM machines m WHERE m.user_id = $1 ORDER BY m.revoked_at IS NOT NULL, m.created_at DESC, m.id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Machine
	for rows.Next() {
		var kinds []byte
		m, err := scanMachine(rows, &kinds)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(kinds, &m.OpenKinds); err != nil {
			return nil, err
		}
		m.OpenDirectives = len(m.OpenKinds)
		out = append(out, m)
	}
	return out, rows.Err()
}

// GetMachine returns a machine; nil when there is none.
func (s *PostgresStore) GetMachine(ctx context.Context, id string) (*Machine, error) {
	m, err := scanMachine(s.db.QueryRowContext(ctx, `SELECT `+machineColumns+` FROM machines m WHERE m.id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// MachineByToken finds the machine a token is, or was, the credential of,
// and what it is to it. TokenUnknown comes with no machine.
func (s *PostgresStore) MachineByToken(ctx context.Context, tokenHash string) (*Machine, TokenUse, error) {
	var current string
	m, err := scanMachine(s.db.QueryRowContext(ctx, `SELECT `+machineColumns+`, COALESCE(m.token_hash, '')
		FROM machines m WHERE m.token_hash = $1 OR m.prev_token_hash = $1`, tokenHash), &current)
	switch {
	case err == nil && current == tokenHash:
		return &m, TokenCurrent, nil
	case err == nil:
		return &m, TokenPending, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, TokenUnknown, err
	}
	m, err = scanMachine(s.db.QueryRowContext(ctx, `SELECT `+machineColumns+` FROM machines m
		JOIN machine_retired_tokens r ON r.machine_id = m.id WHERE r.token_hash = $1`, tokenHash))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, TokenUnknown, nil
	case err != nil:
		return nil, TokenUnknown, err
	}
	return &m, TokenRetired, nil
}

// rotateTokens is the first step of a token's rotation, on a connection that
// presented `presented` (hashes all). The machine gets next; the token it
// presented stays valid until it confirms it wrote next (confirmToken), so a
// machine that crashes in between connects again with it. What it returns
// replaces the machine's tokens; retire lists those replaced for good. ok is
// false when presented is neither of the machine's tokens.
func rotateTokens(current, prev, presented, next string) (newCurrent, newPrev string, retire []string, ok bool) {
	switch {
	case presented == "":
	case presented == current && prev == "":
		return next, current, nil, true
	case presented == current:
		// It wrote current, and its confirmation was lost: prev is over.
		return next, current, []string{prev}, true
	case presented == prev:
		// It never wrote current, which nobody else should hold.
		return next, prev, []string{current}, true
	}
	return current, prev, nil, false
}

// confirmToken is the second step: the machine wrote current, presented
// again to confirm it. The token it replaces is retired.
func confirmToken(current, prev, presented string) (newPrev string, retire []string, ok bool) {
	if presented == "" || presented != current {
		return prev, nil, false
	}
	if prev == "" {
		return "", nil, true
	}
	return "", []string{prev}, true
}

// RotateMachineToken gives a machine its next token (nextHash), on the
// connection that presented presentedHash (rotateTokens).
func (s *PostgresStore) RotateMachineToken(ctx context.Context, id, presentedHash, nextHash string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		var current, prev string
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(token_hash, ''), COALESCE(prev_token_hash, '')
			FROM machines WHERE id = $1 AND revoked_at IS NULL FOR UPDATE`, id).Scan(&current, &prev)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTokenRefused
		}
		if err != nil {
			return err
		}
		newCurrent, newPrev, retire, ok := rotateTokens(current, prev, presentedHash, nextHash)
		if !ok {
			return ErrTokenRefused
		}
		if err := retireTokens(ctx, tx, id, retire); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE machines SET token_hash = $2, prev_token_hash = NULLIF($3, '') WHERE id = $1`,
			id, newCurrent, newPrev)
		return err
	})
}

// ConfirmMachineToken ends a rotation: the machine wrote the token whose
// hash is currentHash, and the one it replaces is retired.
func (s *PostgresStore) ConfirmMachineToken(ctx context.Context, id, currentHash string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		var current, prev string
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(token_hash, ''), COALESCE(prev_token_hash, '')
			FROM machines WHERE id = $1 AND revoked_at IS NULL FOR UPDATE`, id).Scan(&current, &prev)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTokenRefused
		}
		if err != nil {
			return err
		}
		newPrev, retire, ok := confirmToken(current, prev, currentHash)
		if !ok {
			return ErrTokenRefused
		}
		if err := retireTokens(ctx, tx, id, retire); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE machines SET prev_token_hash = NULLIF($2, '') WHERE id = $1`, id, newPrev)
		return err
	})
}

func retireTokens(ctx context.Context, tx *sql.Tx, machineID string, hashes []string) error {
	for _, h := range hashes {
		if h == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO machine_retired_tokens (token_hash, machine_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, h, machineID); err != nil {
			return err
		}
	}
	return nil
}

// MachineConnected records a machine's connection to gateway (an instance
// ID), and what its hello said; it returns the address it last connected
// from.
func (s *PostgresStore) MachineConnected(ctx context.Context, id, gateway, addr string, hello MachineInfo) (prevAddr string, err error) {
	caps, err := json.Marshal(nonNil(hello.Capabilities))
	if err != nil {
		return "", err
	}
	err = s.db.QueryRowContext(ctx, `
		UPDATE machines m SET connected_to = $2, seen_at = NOW(), os = $3, capabilities = $4,
			max_directives = $5, agent_version = $6, last_addr = CASE WHEN $7 = '' THEN m.last_addr ELSE $7 END,
			claude_code = $8
		FROM machines old WHERE m.id = $1 AND old.id = m.id AND m.revoked_at IS NULL
		RETURNING old.last_addr`,
		id, gateway, hello.OS, caps, max(hello.MaxDirectives, 1), hello.AgentVersion, addr, hello.ClaudeCode).Scan(&prevAddr)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrMachineNotFound
	}
	return prevAddr, err
}

// UpdateMachineStatus records what a connected machine says it can do now
// (a login lost, or back).
func (s *PostgresStore) UpdateMachineStatus(ctx context.Context, id string, capabilities []string, claudeCode string) error {
	caps, err := json.Marshal(nonNil(capabilities))
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE machines SET capabilities = $2, claude_code = $3
		WHERE id = $1 AND revoked_at IS NULL`, id, caps, claudeCode)
	return affectedOne(res, err, ErrMachineNotFound)
}

// SetMachinePaused pauses (or resumes) a machine of userID's: a paused
// machine stays connected and gets no directive.
func (s *PostgresStore) SetMachinePaused(ctx context.Context, userID, id string, paused bool) error {
	res, err := s.db.ExecContext(ctx, `UPDATE machines SET paused = $3
		WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL`, id, userID, paused)
	return affectedOne(res, err, ErrMachineNotFound)
}

// SetMachinePriority sets the priority of a machine of userID's: the
// highest is chosen first.
func (s *PostgresStore) SetMachinePriority(ctx context.Context, userID, id string, priority int) error {
	res, err := s.db.ExecContext(ctx, `UPDATE machines SET priority = $3
		WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL`, id, userID, priority)
	return affectedOne(res, err, ErrMachineNotFound)
}

// MachineDisconnected records that gateway no longer holds the machine.
func (s *PostgresStore) MachineDisconnected(ctx context.Context, id, gateway string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE machines SET connected_to = '' WHERE id = $1 AND connected_to = $2`, id, gateway)
	return err
}

// TouchMachines records that the machines answered their gateway's ping.
func (s *PostgresStore) TouchMachines(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `UPDATE machines SET seen_at = NOW() WHERE id = ANY($1)`, ids)
	return err
}

// ResetMachineConnections forgets every connection a gateway held: a
// starting gateway holds none, and there is one gateway (one replica).
func (s *PostgresStore) ResetMachineConnections(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE machines SET connected_to = '' WHERE connected_to <> ''`)
	return err
}

// RevokeMachine revokes a machine for good: its tokens are refused (and
// kept as retired, so that one presented again is told apart), and its open
// directives are closed, returned for their activities to be ended. A
// machine already revoked returns no directive.
func (s *PostgresStore) RevokeMachine(ctx context.Context, id, reason string) ([]Directive, error) {
	var closed []Directive
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var current, prev string
		var revoked *time.Time
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(token_hash, ''), COALESCE(prev_token_hash, ''), revoked_at
			FROM machines WHERE id = $1 FOR UPDATE`, id).Scan(&current, &prev, &revoked)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrMachineNotFound
		}
		if err != nil || revoked != nil {
			return err
		}
		if err := retireTokens(ctx, tx, id, []string{current, prev}); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE machines SET revoked_at = NOW(), revoked_reason = $2,
			token_hash = NULL, prev_token_hash = NULL, connected_to = '' WHERE id = $1`, id, reason); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `UPDATE machine_directives SET state = 'revoked', closed_at = NOW(), error = $2
			WHERE machine_id = $1 AND state IN ('reserved', 'running') RETURNING `+directiveColumns, id, reason)
		if err != nil {
			return err
		}
		closed, err = scanDirectives(rows)
		return err
	})
	return closed, err
}

// --- Directives ---

const directiveColumns = `id, machine_id, user_id, kind, input, workflow_id, run_id, activity_id, call_key, task_token,
	state, handoff_by, deadline, sent_conn, result, error, created_at, started_at, closed_at, session_id, participant, agent`

func scanDirective(row interface{ Scan(...any) error }) (Directive, error) {
	var d Directive
	var input, result []byte
	err := row.Scan(&d.ID, &d.MachineID, &d.UserID, &d.Kind, &input, &d.WorkflowID, &d.RunID, &d.ActivityID, &d.CallKey,
		&d.TaskToken, &d.State, &d.HandoffBy, &d.Deadline, &d.SentConn, &result, &d.Error, &d.CreatedAt, &d.StartedAt, &d.ClosedAt,
		&d.SessionID, &d.Participant, &d.Agent)
	d.Input, d.Result = input, result
	return d, err
}

func scanDirectives(rows *sql.Rows) ([]Directive, error) {
	defer rows.Close()
	var out []Directive
	for rows.Next() {
		d, err := scanDirective(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// machineChoice is a machine PickMachine may choose, and its load.
type machineChoice struct {
	ID       string
	Priority int
	Max      int
	Open     int
	Online   bool
	Paused   bool
	Can      bool
}

// chooseMachine picks among a user's machines: online, not paused, with the
// capability, under the cap it announced; the highest priority, then the
// least busy, then the first by ID. "" = none.
func chooseMachine(choices []machineChoice) string {
	best := -1
	for i, c := range choices {
		if !c.Online || c.Paused || !c.Can || c.Open >= c.Max {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		b := choices[best]
		if c.Priority > b.Priority || (c.Priority == b.Priority && (c.Open < b.Open || (c.Open == b.Open && c.ID < b.ID))) {
			best = i
		}
	}
	if best < 0 {
		return ""
	}
	return choices[best].ID
}

// PickMachine reserves a machine of req.UserID for a directive, and creates
// the directive, in one transaction that holds the user's machines: two
// picks never both take a machine's last slot. A call made again (same run,
// same call key) finds its directive, whatever became of it. No machine to
// take it: ErrNoMachine.
func (s *PostgresStore) PickMachine(ctx context.Context, req PickRequest) (Directive, Machine, error) {
	var d Directive
	var m Machine
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT `+machineColumns+` FROM machines m
			WHERE m.user_id = $1 AND m.revoked_at IS NULL ORDER BY m.id FOR UPDATE`, req.UserID)
		if err != nil {
			return err
		}
		var machines []Machine
		for rows.Next() {
			mm, err := scanMachine(rows)
			if err != nil {
				rows.Close()
				return err
			}
			machines = append(machines, mm)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		existing, err := scanDirective(tx.QueryRowContext(ctx, `SELECT `+directiveColumns+` FROM machine_directives
			WHERE run_id = $1 AND call_key = $2`, req.RunID, req.CallKey))
		switch {
		case err == nil:
			d = existing
			m, err = scanMachine(tx.QueryRowContext(ctx, `SELECT `+machineColumns+` FROM machines m WHERE m.id = $1`, d.MachineID))
			return err
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}

		ids := make([]string, len(machines))
		for i, mm := range machines {
			ids[i] = mm.ID
		}
		open := map[string]int{}
		loads, err := tx.QueryContext(ctx, `SELECT machine_id, COUNT(*) FROM machine_directives
			WHERE machine_id = ANY($1) AND state IN ('reserved', 'running') GROUP BY machine_id`, ids)
		if err != nil {
			return err
		}
		for loads.Next() {
			var id string
			var n int
			if err := loads.Scan(&id, &n); err != nil {
				loads.Close()
				return err
			}
			open[id] = n
		}
		loads.Close()
		if err := loads.Err(); err != nil {
			return err
		}

		choices := make([]machineChoice, len(machines))
		for i, mm := range machines {
			choices[i] = machineChoice{ID: mm.ID, Priority: mm.Priority, Max: mm.MaxDirectives, Open: open[mm.ID],
				Online: mm.Online(req.SeenAfter), Paused: mm.Paused, Can: mm.Can(req.Capability)}
		}
		chosen := chooseMachine(choices)
		if chosen == "" {
			return ErrNoMachine
		}
		for _, mm := range machines {
			if mm.ID == chosen {
				m = mm
			}
		}
		d, err = scanDirective(tx.QueryRowContext(ctx, `
			INSERT INTO machine_directives (id, machine_id, user_id, kind, input, workflow_id, run_id, call_key, handoff_by, deadline,
				session_id, participant, agent)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING `+directiveColumns,
			req.DirectiveID, chosen, req.UserID, req.Kind, []byte(req.Input), req.WorkflowID, req.RunID, req.CallKey,
			req.HandoffBy, req.Deadline, req.SessionID, req.Participant, req.Agent))
		return err
	})
	return d, m, err
}

// StartDirective sets a reserved directive's task token: it runs, until
// deadline at the latest. One already running with the same token is a
// retry; a closed one is ErrDirectiveClosed.
func (s *PostgresStore) StartDirective(ctx context.Context, id string, token []byte, activityID string, deadline time.Time) (Directive, error) {
	d, err := scanDirective(s.db.QueryRowContext(ctx, `UPDATE machine_directives
		SET task_token = $2, activity_id = $3, deadline = $4, state = 'running', started_at = NOW()
		WHERE id = $1 AND state = 'reserved' RETURNING `+directiveColumns, id, token, activityID, deadline))
	if !errors.Is(err, sql.ErrNoRows) {
		return d, err
	}
	got, err := s.GetDirective(ctx, id)
	switch {
	case err != nil:
		return Directive{}, err
	case got == nil:
		return Directive{}, ErrDirectiveNotFound
	case got.State == DirectiveRunning && string(got.TaskToken) == string(token):
		return *got, nil
	}
	return *got, ErrDirectiveClosed
}

// GetDirective returns a directive; nil when there is none.
func (s *PostgresStore) GetDirective(ctx context.Context, id string) (*Directive, error) {
	d, err := scanDirective(s.db.QueryRowContext(ctx, `SELECT `+directiveColumns+` FROM machine_directives WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// MarkDirectiveSent records that conn sends a running directive to its
// machine; false when it was sent already, or is no longer running: only
// one sender sends it.
func (s *PostgresStore) MarkDirectiveSent(ctx context.Context, id, conn string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE machine_directives SET sent_conn = $2
		WHERE id = $1 AND sent_conn = '' AND state = 'running'`, id, conn)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// OpenDirectives returns a machine's open directives.
func (s *PostgresStore) OpenDirectives(ctx context.Context, machineID string) ([]Directive, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+directiveColumns+` FROM machine_directives
		WHERE machine_id = $1 AND state IN ('reserved', 'running') ORDER BY created_at, id`, machineID)
	if err != nil {
		return nil, err
	}
	return scanDirectives(rows)
}

// SaveDirectiveResult keeps the result a machine sent for an open directive,
// before its activity is completed with it.
func (s *PostgresStore) SaveDirectiveResult(ctx context.Context, id string, result json.RawMessage) error {
	res, err := s.db.ExecContext(ctx, `UPDATE machine_directives SET result = $2
		WHERE id = $1 AND state IN ('reserved', 'running')`, id, []byte(result))
	return affectedOne(res, err, ErrDirectiveClosed)
}

// CloseDirective ends an open directive in state, with errText; false when
// it was over already.
func (s *PostgresStore) CloseDirective(ctx context.Context, id, state, errText string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE machine_directives SET state = $2, error = $3, closed_at = NOW()
		WHERE id = $1 AND state IN ('reserved', 'running')`, id, state, errText)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// SweepDirectives closes the directives nobody will end: reserved ones past
// their handoff (their activity never started, or failed before it set its
// token: orphaned) and running ones past their deadline (expired). It
// returns them.
func (s *PostgresStore) SweepDirectives(ctx context.Context, now time.Time) ([]Directive, error) {
	rows, err := s.db.QueryContext(ctx, `UPDATE machine_directives
		SET state = CASE WHEN state = 'reserved' THEN 'orphaned' ELSE 'expired' END, closed_at = NOW(),
			error = CASE WHEN state = 'reserved' THEN 'never handed to its machine' ELSE 'past its deadline' END
		WHERE (state = 'reserved' AND handoff_by <= $1) OR (state = 'running' AND deadline <= $1)
		RETURNING `+directiveColumns, now)
	if err != nil {
		return nil, err
	}
	return scanDirectives(rows)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// PendingDirectiveResults returns the running directives whose machine's
// result is kept but whose activity was not completed with it (Temporal
// away): the gateway's sweep completes them again.
func (s *PostgresStore) PendingDirectiveResults(ctx context.Context) ([]Directive, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+directiveColumns+` FROM machine_directives
		WHERE state = 'running' AND result IS NOT NULL AND task_token IS NOT NULL ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	return scanDirectives(rows)
}
