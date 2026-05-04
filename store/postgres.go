package store

import (
	"context"
	"database/sql"
	"encoding/json"

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

func (s *PostgresStore) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL DEFAULT '',
			telegram_id BIGINT UNIQUE,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS messages (
			id BIGSERIAL PRIMARY KEY,
			session_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			data JSONB NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, seq);

		CREATE TABLE IF NOT EXISTS memory (
			scope TEXT NOT NULL,
			scope_id TEXT NOT NULL,
			content TEXT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (scope, scope_id)
		);

		CREATE TABLE IF NOT EXISTS sessions (
			session_id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			title TEXT NOT NULL DEFAULT '',
			task_queue TEXT NOT NULL DEFAULT '',
			channel TEXT NOT NULL DEFAULT 'web',
			channel_id TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, created_at DESC);
		ALTER TABLE sessions ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT 'web';
		ALTER TABLE sessions ADD COLUMN IF NOT EXISTS channel_id TEXT NOT NULL DEFAULT '';

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

		CREATE TABLE IF NOT EXISTS agent_catalog (
			task_queue TEXT PRIMARY KEY,
			skills JSONB NOT NULL DEFAULT '[]',
			updated_at TIMESTAMPTZ DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS skills_version (
			id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
			version BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		);
		INSERT INTO skills_version (id, version) VALUES (1, 0) ON CONFLICT DO NOTHING;

		CREATE TABLE IF NOT EXISTS activity_queues (
			activity_name TEXT PRIMARY KEY,
			task_queue TEXT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		);
	`)
	return err
}

func (s *PostgresStore) GetUserByTelegramID(ctx context.Context, telegramID int64) (*User, error) {
	var u User
	err := s.db.QueryRowContext(ctx,
		"SELECT id, name, telegram_id, created_at FROM users WHERE telegram_id = $1",
		telegramID).Scan(&u.ID, &u.Name, &u.TelegramID, &u.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (s *PostgresStore) CreateSession(ctx context.Context, session Session) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sessions (session_id, user_id, title, task_queue, channel, channel_id)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		session.SessionID, session.UserID, session.Title, session.TaskQueue, session.Channel, session.ChannelID)
	return err
}

func (s *PostgresStore) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	var sess Session
	err := s.db.QueryRowContext(ctx,
		"SELECT session_id, user_id, title, task_queue, channel, channel_id, created_at FROM sessions WHERE session_id = $1",
		sessionID).Scan(&sess.SessionID, &sess.UserID, &sess.Title, &sess.TaskQueue, &sess.Channel, &sess.ChannelID, &sess.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sess, nil
}

func (s *PostgresStore) GetActiveSessionByChannel(ctx context.Context, userID, channel, channelID string) (*Session, error) {
	var sess Session
	err := s.db.QueryRowContext(ctx,
		`SELECT session_id, user_id, title, task_queue, channel, channel_id, created_at
		 FROM sessions WHERE user_id = $1 AND channel = $2 AND channel_id = $3
		 ORDER BY created_at DESC LIMIT 1`,
		userID, channel, channelID).Scan(&sess.SessionID, &sess.UserID, &sess.Title, &sess.TaskQueue, &sess.Channel, &sess.ChannelID, &sess.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sess, nil
}

func (s *PostgresStore) GetSessionUser(ctx context.Context, sessionID string) (string, error) {
	var userID string
	err := s.db.QueryRowContext(ctx, "SELECT user_id FROM sessions WHERE session_id = $1", sessionID).Scan(&userID)
	if err != nil {
		return "", err
	}
	return userID, nil
}

func (s *PostgresStore) DeleteSession(ctx context.Context, sessionID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tx.ExecContext(ctx, "DELETE FROM messages WHERE session_id = $1", sessionID)
	tx.ExecContext(ctx, "DELETE FROM memory WHERE scope = 'session' AND scope_id = $1", sessionID)
	tx.ExecContext(ctx, "DELETE FROM sessions WHERE session_id = $1", sessionID)

	return tx.Commit()
}

func (s *PostgresStore) ListSessionsByUser(ctx context.Context, userID string) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT session_id, user_id, title, task_queue, channel, channel_id, created_at FROM sessions WHERE user_id = $1 ORDER BY created_at DESC",
		userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		var s Session
		if err := rows.Scan(&s.SessionID, &s.UserID, &s.Title, &s.TaskQueue, &s.Channel, &s.ChannelID, &s.CreatedAt); err != nil {
			return nil, err
		}
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

func (s *PostgresStore) UpdateSessionTitle(ctx context.Context, sessionID, title string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE sessions SET title = $1 WHERE session_id = $2 AND title = ''", title, sessionID)
	return err
}

func (s *PostgresStore) LoadMessages(ctx context.Context, sessionID string) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT data FROM messages WHERE session_id = $1 ORDER BY seq", sessionID)
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
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, data FROM messages WHERE session_id = $1 ORDER BY seq", sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []MessageWithID
	for rows.Next() {
		var m MessageWithID
		var data string
		if err := rows.Scan(&m.ID, &data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(data), &m.Message); err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

func (s *PostgresStore) SaveMessages(ctx context.Context, sessionID string, messages []Message) error {
	stmt, err := s.db.PrepareContext(ctx,
		"INSERT INTO messages (session_id, seq, data) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i, msg := range messages {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, sessionID, i, string(data)); err != nil {
			return err
		}
	}

	return nil
}

func (s *PostgresStore) AppendMessage(ctx context.Context, sessionID string, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO messages (session_id, seq, data)
		VALUES ($1, COALESCE((SELECT MAX(seq) FROM messages WHERE session_id = $1), -1) + 1, $2)`,
		sessionID, string(data))
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

func (s *PostgresStore) ListTaskLogs(ctx context.Context) ([]TaskLog, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT schedule_id, type, description, cron, delay, prompt, user_id, channel, created_at, status FROM task_logs ORDER BY created_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []TaskLog
	for rows.Next() {
		var l TaskLog
		if err := rows.Scan(&l.ScheduleID, &l.Type, &l.Description, &l.Cron, &l.Delay, &l.Prompt, &l.UserID, &l.Channel, &l.CreatedAt, &l.Status); err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}
	return logs, rows.Err()
}

func (s *PostgresStore) UpdateTaskLogStatus(ctx context.Context, scheduleID, status string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE task_logs SET status = $1 WHERE schedule_id = $2", status, scheduleID)
	return err
}

func (s *PostgresStore) UpsertAgent(ctx context.Context, taskQueue string, skills []string) error {
	data, err := json.Marshal(skills)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO agent_catalog (task_queue, skills, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (task_queue) DO UPDATE SET skills = EXCLUDED.skills, updated_at = NOW()`,
		taskQueue, string(data))
	return err
}

func (s *PostgresStore) ListAgents(ctx context.Context) ([]AgentEntry, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT task_queue, skills FROM agent_catalog ORDER BY task_queue")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var agents []AgentEntry
	for rows.Next() {
		var a AgentEntry
		var skillsJSON string
		if err := rows.Scan(&a.TaskQueue, &skillsJSON); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(skillsJSON), &a.Skills)
		agents = append(agents, a)
	}
	return agents, rows.Err()
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
