package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	s := &PostgresStore{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}

	return s, nil
}

// migrateLockID keys the advisory lock that serializes migrations: the server
// and every worker migrate at startup, and two concurrent ALTER TABLE on the
// same table deadlock.
const migrateLockID = 7_431_906_215

func (s *PostgresStore) migrate() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("SELECT pg_advisory_xact_lock($1)", migrateLockID); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	if _, err := tx.Exec(schema); err != nil {
		return err
	}
	return tx.Commit()
}

const schema = `
		-- An account. The email is the login, unique whatever its case; the id is
		-- what everything else refers to, so the email can change.
		CREATE TABLE IF NOT EXISTS users (
			id            TEXT PRIMARY KEY,
			email         TEXT NOT NULL,
			display_name  TEXT NOT NULL DEFAULT '',
			password_hash TEXT NOT NULL,
			role          TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user')),
			telegram_id   BIGINT UNIQUE,
			disabled_at   TIMESTAMPTZ,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (lower(email));

		-- A logged-in browser. Only the token's hash is stored: a leaked table
		-- logs nobody in.
		CREATE TABLE IF NOT EXISTS login_sessions (
			token_hash TEXT PRIMARY KEY,
			user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_login_sessions_user ON login_sessions(user_id);

		-- msg_key is the idempotency key of a message within its session:
		-- "{run id}-{turn}:{index}" for a conversation turn, "sched:{id}:{run}"
		-- for a delivered task result. Writes are append-only and ON CONFLICT DO
		-- NOTHING, so a replayed activity is a no-op instead of a duplicate.
		-- Ordering is by id: nothing ever renumbers an existing row.
		CREATE TABLE IF NOT EXISTS messages (
			id BIGSERIAL PRIMARY KEY,
			session_id TEXT NOT NULL,
			msg_key TEXT NOT NULL,
			data JSONB NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_session_key ON messages(session_id, msg_key);
		CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id, id);

		CREATE TABLE IF NOT EXISTS memory (
			scope TEXT NOT NULL,
			scope_id TEXT NOT NULL,
			content TEXT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (scope, scope_id)
		);

		-- A conversation. created_by is who opened it; who may use it is in
		-- session_members.
		CREATE TABLE IF NOT EXISTS sessions (
			session_id TEXT PRIMARY KEY,
			created_by TEXT NOT NULL REFERENCES users(id),
			title TEXT NOT NULL DEFAULT '',
			agent_id TEXT NOT NULL DEFAULT '',
			channel TEXT NOT NULL DEFAULT 'web',
			channel_id TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ DEFAULT NOW()
		);

		-- A fork starts from a message of another session, seeded with a summary
		-- of the conversation up to it. Deleting the parent leaves the fork
		-- whole: it keeps its summary, and loses only the link.
		ALTER TABLE sessions ADD COLUMN IF NOT EXISTS parent_session_id TEXT REFERENCES sessions(session_id) ON DELETE SET NULL;
		ALTER TABLE sessions ADD COLUMN IF NOT EXISTS forked_at_message_id BIGINT;
		ALTER TABLE sessions ADD COLUMN IF NOT EXISTS forked_by TEXT;
		CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);

		-- When a human message calls the agent: 'auto' (every message when one
		-- user is alone in the session, on @agent once several are), 'always',
		-- or 'mention' (only on @agent).
		ALTER TABLE sessions ADD COLUMN IF NOT EXISTS agent_mode TEXT NOT NULL DEFAULT 'auto';

		-- The users of a session. Any member may add others.
		CREATE TABLE IF NOT EXISTS session_members (
			session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
			user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			added_by   TEXT NOT NULL DEFAULT '',
			added_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (session_id, user_id)
		);
		CREATE INDEX IF NOT EXISTS idx_session_members_user ON session_members(user_id);

		CREATE TABLE IF NOT EXISTS task_logs (
			schedule_id TEXT PRIMARY KEY,
			type TEXT NOT NULL DEFAULT 'schedule',
			description TEXT NOT NULL,
			cron TEXT,
			delay TEXT,
			prompt TEXT NOT NULL,
			user_id TEXT,
			channel TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			status TEXT NOT NULL DEFAULT 'scheduled'
		);
		-- A user lists and cancels their own tasks only.
		CREATE INDEX IF NOT EXISTS idx_task_logs_user ON task_logs(user_id);

		CREATE TABLE IF NOT EXISTS agents (
			agent_id      TEXT PRIMARY KEY,
			name          TEXT NOT NULL,
			description   TEXT NOT NULL DEFAULT '',
			skills        JSONB NOT NULL DEFAULT '[]',
			tools         JSONB NOT NULL DEFAULT '[]',
			revision      BIGINT NOT NULL DEFAULT 1,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		-- revision is bumped on every update, so an edit made from a stale copy
		-- is refused instead of silently overwriting a newer one.
		ALTER TABLE agents ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;
		-- spawn_session gave way to one agent_<id> tool per agent, generated from
		-- this table. An allowlist that granted it keeps delegating to everyone,
		-- now spelled agent_*.
		UPDATE agents SET tools = (tools - 'spawn_session') || '["agent_*"]', revision = revision + 1
			WHERE tools ? 'spawn_session';
		-- tools is a deny-by-default allowlist: [] grants nothing, ["*"] grants
		-- everything. A NULL once meant "every tool"; close it on older tables.
		-- Guarded so a migrated table is not locked again on every startup.
		DO $$
		BEGIN
			IF EXISTS (SELECT 1 FROM information_schema.columns
			           WHERE table_name = 'agents' AND column_name = 'tools' AND is_nullable = 'YES') THEN
				UPDATE agents SET tools = '[]' WHERE tools IS NULL;
				ALTER TABLE agents ALTER COLUMN tools SET DEFAULT '[]';
				ALTER TABLE agents ALTER COLUMN tools SET NOT NULL;
			END IF;
		END $$;

		CREATE TABLE IF NOT EXISTS skills_version (
			id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
			version BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		);
		INSERT INTO skills_version (id, version) VALUES (1, 0) ON CONFLICT DO NOTHING;

		CREATE TABLE IF NOT EXISTS tools (
			name            TEXT PRIMARY KEY,
			task_queue      TEXT NOT NULL,
			description     TEXT NOT NULL DEFAULT '',
			input_schema    JSONB NOT NULL,
			kind            TEXT NOT NULL,
			workflow_name   TEXT NOT NULL DEFAULT '',
			fire_and_forget BOOLEAN NOT NULL DEFAULT FALSE,
			schema_hash     TEXT NOT NULL,
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_tools_task_queue ON tools(task_queue);
		-- What a tool is, published with it (tool.Tool): sensitive, private
		-- input, needs the caller's context. These were lists of names in the
		-- code; the rows published before they became columns get them here,
		-- once, so a server reading them keeps hiding a user's memory until
		-- the workers publish again.
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns
			               WHERE table_name = 'tools' AND column_name = 'private_input') THEN
				ALTER TABLE tools ADD COLUMN sensitive BOOLEAN NOT NULL DEFAULT FALSE;
				ALTER TABLE tools ADD COLUMN private_input BOOLEAN NOT NULL DEFAULT FALSE;
				ALTER TABLE tools ADD COLUMN needs_call_context BOOLEAN NOT NULL DEFAULT FALSE;
				UPDATE tools SET sensitive = TRUE
					WHERE name IN ('exec', 'write_file', 'edit_file', 'implement_feature', 'send_email');
				UPDATE tools SET private_input = TRUE WHERE name = 'save_user_memory';
				UPDATE tools SET needs_call_context = TRUE WHERE name = 'ask_user';
			END IF;
		END $$;
		-- The spawn_session row the workers published: nothing else deletes from
		-- tools, and no worker publishes it any more.
		DELETE FROM tools WHERE name = 'spawn_session';

		CREATE TABLE IF NOT EXISTS activity_queues (
			activity_name TEXT PRIMARY KEY,
			task_queue TEXT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		);
`

func (s *PostgresStore) UpdateSessionTitle(ctx context.Context, sessionID, title string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE sessions SET title = $1 WHERE session_id = $2 AND title = ''", title, sessionID)
	return err
}

func (s *PostgresStore) LoadMessages(ctx context.Context, sessionID string) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT data FROM messages WHERE session_id = $1 ORDER BY id", sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var msg Message
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, rows.Err()
}

func (s *PostgresStore) LoadMessagesWithID(ctx context.Context, sessionID string) ([]MessageWithID, error) {
	return s.LoadMessagesUpTo(ctx, sessionID, 0)
}

// LoadMessagesUpTo returns a session's messages up to and including lastID;
// all of them when lastID is 0.
func (s *PostgresStore) LoadMessagesUpTo(ctx context.Context, sessionID string, lastID int64) ([]MessageWithID, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, created_at, data FROM messages WHERE session_id = $1 AND ($2 = 0 OR id <= $2) ORDER BY id", sessionID, lastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []MessageWithID
	for rows.Next() {
		var m MessageWithID
		var data string
		var createdAt sql.NullTime
		if err := rows.Scan(&m.ID, &createdAt, &data); err != nil {
			return nil, err
		}
		m.CreatedAt = createdAt.Time
		if err := json.Unmarshal([]byte(data), &m.Message); err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

func (s *PostgresStore) AppendMessages(ctx context.Context, sessionID, turnKey string, startIndex int, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		"INSERT INTO messages (session_id, msg_key, data) VALUES ($1, $2, $3) ON CONFLICT (session_id, msg_key) DO NOTHING")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i, msg := range messages {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		key := TurnMessageKey(turnKey, startIndex+i)
		if _, err := stmt.ExecContext(ctx, sessionID, key, string(data)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *PostgresStore) AppendMessage(ctx context.Context, sessionID, key string, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO messages (session_id, msg_key, data)
		VALUES ($1, $2, $3) ON CONFLICT (session_id, msg_key) DO NOTHING`,
		sessionID, key, string(data))
	return err
}

func (s *PostgresStore) DeleteMessage(ctx context.Context, sessionID string, id int64) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM messages WHERE session_id = $1 AND id = $2", sessionID, id)
	return err
}

func (s *PostgresStore) DeleteMessagesBySession(ctx context.Context, sessionID string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM messages WHERE session_id = $1", sessionID)
	return err
}

func (s *PostgresStore) LoadMemory(ctx context.Context, scope MemoryScope, scopeID string) (string, error) {
	var content string
	err := s.db.QueryRowContext(ctx,
		"SELECT content FROM memory WHERE scope = $1 AND scope_id = $2",
		string(scope), scopeID).Scan(&content)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return content, err
}

func (s *PostgresStore) SaveMemory(ctx context.Context, scope MemoryScope, scopeID string, content string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO memory (scope, scope_id, content, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (scope, scope_id) DO UPDATE SET content = EXCLUDED.content, updated_at = NOW()`,
		string(scope), scopeID, content)
	return err
}

func (s *PostgresStore) SaveTaskLog(ctx context.Context, log TaskLog) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO task_logs (schedule_id, type, description, cron, delay, prompt, user_id, channel, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		log.ScheduleID, log.Type, log.Description, log.Cron, log.Delay, log.Prompt, log.UserID, log.Channel, log.Status)
	return err
}

// taskLogColumns is the column list of task log queries, in scanTaskLog order.
const taskLogColumns = `schedule_id, type, description, COALESCE(cron, ''), COALESCE(delay, ''), prompt,
	COALESCE(user_id, ''), COALESCE(channel, ''), created_at, status`

// ListTaskLogsByUser returns the scheduled tasks of one user, newest first.
// There is no listing of everyone's: a task's prompt is often personal.
func (s *PostgresStore) ListTaskLogsByUser(ctx context.Context, userID string) ([]TaskLog, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+taskLogColumns+" FROM task_logs WHERE user_id = $1 ORDER BY created_at DESC", userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []TaskLog
	for rows.Next() {
		l, err := scanTaskLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}
	return logs, rows.Err()
}

// GetTaskLog returns one scheduled task, or nil if there is none.
func (s *PostgresStore) GetTaskLog(ctx context.Context, scheduleID string) (*TaskLog, error) {
	l, err := scanTaskLog(s.db.QueryRowContext(ctx,
		"SELECT "+taskLogColumns+" FROM task_logs WHERE schedule_id = $1", scheduleID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &l, nil
}

func scanTaskLog(row interface{ Scan(...any) error }) (TaskLog, error) {
	var l TaskLog
	err := row.Scan(&l.ScheduleID, &l.Type, &l.Description, &l.Cron, &l.Delay, &l.Prompt, &l.UserID, &l.Channel, &l.CreatedAt, &l.Status)
	return l, err
}

func (s *PostgresStore) UpdateTaskLogStatus(ctx context.Context, scheduleID, status string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE task_logs SET status = $1 WHERE schedule_id = $2", status, scheduleID)
	return err
}

// agentColumns is the column list shared by agent queries, in scanAgent order.
const agentColumns = "agent_id, name, description, skills, tools, revision, created_at, updated_at"

// InsertAgentIfAbsent inserts the agent only if no agent with the same ID exists.
// Existing rows are never modified. Returns true if the agent was inserted.
func (s *PostgresStore) InsertAgentIfAbsent(ctx context.Context, agent Agent) (bool, error) {
	skillsJSON, toolsJSON, err := marshalAgentLists(agent)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, name, description, skills, tools)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (agent_id) DO NOTHING`,
		agent.ID, agent.Name, agent.Description, skillsJSON, toolsJSON)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *PostgresStore) ListAgents(ctx context.Context) ([]Agent, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+agentColumns+" FROM agents ORDER BY agent_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var agents []Agent
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		agents = append(agents, *a)
	}
	return agents, rows.Err()
}

func (s *PostgresStore) GetAgent(ctx context.Context, agentID string) (*Agent, error) {
	a, err := scanAgent(s.db.QueryRowContext(ctx,
		"SELECT "+agentColumns+" FROM agents WHERE agent_id = $1", agentID))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return a, err
}

// CreateAgent inserts a new agent. It fails with ErrAgentExists if the ID is
// taken.
func (s *PostgresStore) CreateAgent(ctx context.Context, agent Agent) error {
	inserted, err := s.InsertAgentIfAbsent(ctx, agent)
	if err != nil {
		return err
	}
	if !inserted {
		return ErrAgentExists
	}
	return nil
}

// UpdateAgent replaces an agent's definition if its revision is still
// expectedRevision, and returns the new revision. It fails with ErrAgentNotFound
// or, when the agent changed since it was read, ErrAgentConflict.
func (s *PostgresStore) UpdateAgent(ctx context.Context, agent Agent, expectedRevision int64) (int64, error) {
	skillsJSON, toolsJSON, err := marshalAgentLists(agent)
	if err != nil {
		return 0, err
	}
	var revision int64
	err = s.db.QueryRowContext(ctx, `
		UPDATE agents SET name = $2, description = $3, skills = $4, tools = $5,
			revision = revision + 1, updated_at = NOW()
		WHERE agent_id = $1 AND revision = $6
		RETURNING revision`,
		agent.ID, agent.Name, agent.Description, skillsJSON, toolsJSON, expectedRevision).Scan(&revision)
	if err != sql.ErrNoRows {
		return revision, err
	}
	current, err := s.GetAgent(ctx, agent.ID)
	if err != nil {
		return 0, err
	}
	if current == nil {
		return 0, ErrAgentNotFound
	}
	return 0, ErrAgentConflict
}

// DeleteAgent removes an agent. It fails with ErrAgentNotFound if there is none.
func (s *PostgresStore) DeleteAgent(ctx context.Context, agentID string) error {
	res, err := s.db.ExecContext(ctx, "DELETE FROM agents WHERE agent_id = $1", agentID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrAgentNotFound
	}
	return nil
}

// CountSessionsByAgent returns the number of sessions per agent ID.
func (s *PostgresStore) CountSessionsByAgent(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT agent_id, count(*) FROM sessions GROUP BY agent_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		counts[id] = n
	}
	return counts, rows.Err()
}

// marshalAgentLists encodes skills and tools for storage.
// A nil Tools slice is stored as [] (no tool allowed), never as NULL.
func marshalAgentLists(agent Agent) (skills, tools string, err error) {
	if agent.Skills == nil {
		agent.Skills = []string{}
	}
	if agent.Tools == nil {
		agent.Tools = []string{}
	}
	b, err := json.Marshal(agent.Skills)
	if err != nil {
		return "", "", err
	}
	t, err := json.Marshal(agent.Tools)
	if err != nil {
		return "", "", err
	}
	return string(b), string(t), nil
}

func scanAgent(row interface{ Scan(...any) error }) (*Agent, error) {
	var a Agent
	var skillsJSON, toolsJSON string
	if err := row.Scan(&a.ID, &a.Name, &a.Description, &skillsJSON, &toolsJSON, &a.Revision, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(skillsJSON), &a.Skills); err != nil {
		return nil, fmt.Errorf("agent %s: decode skills: %w", a.ID, err)
	}
	if err := json.Unmarshal([]byte(toolsJSON), &a.Tools); err != nil {
		return nil, fmt.Errorf("agent %s: decode tools: %w", a.ID, err)
	}
	return &a, nil
}

// UpsertTool publishes a tool, replacing any previous row with the same name.
// Callers decide beforehand whether taking over another queue's tool is allowed.
func (s *PostgresStore) UpsertTool(ctx context.Context, t ToolRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tools (name, task_queue, description, input_schema, kind, workflow_name, fire_and_forget,
		                   sensitive, private_input, needs_call_context, schema_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (name) DO UPDATE SET
			task_queue = EXCLUDED.task_queue,
			description = EXCLUDED.description,
			input_schema = EXCLUDED.input_schema,
			kind = EXCLUDED.kind,
			workflow_name = EXCLUDED.workflow_name,
			fire_and_forget = EXCLUDED.fire_and_forget,
			sensitive = EXCLUDED.sensitive,
			private_input = EXCLUDED.private_input,
			needs_call_context = EXCLUDED.needs_call_context,
			schema_hash = EXCLUDED.schema_hash,
			updated_at = NOW()`,
		t.Name, t.TaskQueue, t.Description, string(t.InputSchema), t.Kind, t.WorkflowName,
		t.FireAndForget, t.Sensitive, t.PrivateInput, t.NeedsCallContext, t.SchemaHash)
	return err
}

func (s *PostgresStore) ListTools(ctx context.Context) ([]ToolRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, task_queue, description, input_schema, kind, workflow_name,
		       fire_and_forget, sensitive, private_input, needs_call_context, schema_hash, updated_at
		FROM tools ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tools []ToolRecord
	for rows.Next() {
		var t ToolRecord
		var schema string
		if err := rows.Scan(&t.Name, &t.TaskQueue, &t.Description, &schema, &t.Kind, &t.WorkflowName,
			&t.FireAndForget, &t.Sensitive, &t.PrivateInput, &t.NeedsCallContext, &t.SchemaHash, &t.UpdatedAt); err != nil {
			return nil, err
		}
		t.InputSchema = json.RawMessage(schema)
		tools = append(tools, t)
	}
	return tools, rows.Err()
}

func (s *PostgresStore) GetSkillsVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, "SELECT version FROM skills_version WHERE id = 1").Scan(&version)
	return version, err
}

func (s *PostgresStore) IncrementSkillsVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRowContext(ctx,
		"UPDATE skills_version SET version = version + 1, updated_at = NOW() WHERE id = 1 RETURNING version").Scan(&version)
	return version, err
}

// Activity queue mapping

func (s *PostgresStore) GetActivityQueueMap(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT activity_name, task_queue FROM activity_queues")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[string]string)
	for rows.Next() {
		var name, queue string
		if err := rows.Scan(&name, &queue); err != nil {
			return nil, err
		}
		m[name] = queue
	}
	return m, rows.Err()
}

func (s *PostgresStore) SetActivityQueue(ctx context.Context, activityName, taskQueue string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO activity_queues (activity_name, task_queue, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (activity_name) DO UPDATE SET task_queue = EXCLUDED.task_queue, updated_at = NOW()`,
		activityName, taskQueue)
	return err
}

func (s *PostgresStore) DeleteActivityQueue(ctx context.Context, activityName string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM activity_queues WHERE activity_name = $1", activityName)
	return err
}

func (s *PostgresStore) ListActivityQueues(ctx context.Context) ([]ActivityQueueEntry, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT activity_name, task_queue FROM activity_queues ORDER BY activity_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ActivityQueueEntry
	for rows.Next() {
		var e ActivityQueueEntry
		if err := rows.Scan(&e.ActivityName, &e.TaskQueue); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
