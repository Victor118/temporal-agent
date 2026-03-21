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
		CREATE TABLE IF NOT EXISTS messages (
			id BIGSERIAL PRIMARY KEY,
			session_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			data JSONB NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, seq);

		CREATE TABLE IF NOT EXISTS memory (
			scope TEXT NOT NULL,
			scope_id TEXT NOT NULL,
			content TEXT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (scope, scope_id)
		);

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
	`)
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

func (s *PostgresStore) SaveMessages(ctx context.Context, sessionID string, messages []Message) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM messages WHERE session_id = $1", sessionID); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO messages (session_id, seq, data) VALUES ($1, $2, $3)")
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

	return tx.Commit()
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

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
