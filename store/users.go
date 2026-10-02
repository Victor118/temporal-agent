package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const userColumns = "id, email, display_name, password_hash, role, telegram_id, disabled_at, created_at"

func scanUser(row interface{ Scan(...any) error }) (*User, error) {
	var u User
	err := row.Scan(&u.ID, &u.Email, &u.DisplayName, &u.PasswordHash, &u.Role, &u.TelegramID, &u.DisabledAt, &u.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// isUniqueViolation reports a duplicate key, Postgres error 23505.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// CreateUser inserts a user. It fails with ErrUserExists if the email (in any
// case) or the Telegram ID is taken.
func (s *PostgresStore) CreateUser(ctx context.Context, u User) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users (id, email, display_name, password_hash, role, telegram_id)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		u.ID, strings.TrimSpace(u.Email), u.DisplayName, u.PasswordHash, u.Role, u.TelegramID)
	if isUniqueViolation(err) {
		return ErrUserExists
	}
	return err
}

func (s *PostgresStore) GetUser(ctx context.Context, id string) (*User, error) {
	return scanUser(s.db.QueryRowContext(ctx, "SELECT "+userColumns+" FROM users WHERE id = $1", id))
}

// GetUserByEmail finds a user whatever the case of the email.
func (s *PostgresStore) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	return scanUser(s.db.QueryRowContext(ctx,
		"SELECT "+userColumns+" FROM users WHERE lower(email) = lower($1)", strings.TrimSpace(email)))
}

func (s *PostgresStore) GetUserByTelegramID(ctx context.Context, telegramID int64) (*User, error) {
	return scanUser(s.db.QueryRowContext(ctx, "SELECT "+userColumns+" FROM users WHERE telegram_id = $1", telegramID))
}

func (s *PostgresStore) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+userColumns+" FROM users ORDER BY lower(email)")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, *u)
	}
	return users, rows.Err()
}

// UpdateUser changes what an admin may edit: email, display name, role and
// Telegram ID. Not the password, nor the disabled state, which have their
// own methods because they also end login sessions.
func (s *PostgresStore) UpdateUser(ctx context.Context, u User) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE users SET email = $2, display_name = $3, role = $4, telegram_id = $5 WHERE id = $1`,
		u.ID, strings.TrimSpace(u.Email), u.DisplayName, u.Role, u.TelegramID)
	if isUniqueViolation(err) {
		return ErrUserExists
	}
	return affectedOne(res, err, ErrUserNotFound)
}

// SetUserPassword replaces the password and ends every login session of the
// user: whoever knew the old password is logged out.
func (s *PostgresStore) SetUserPassword(ctx context.Context, id, passwordHash string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, "UPDATE users SET password_hash = $2 WHERE id = $1", id, passwordHash)
		if err := affectedOne(res, err, ErrUserNotFound); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM login_sessions WHERE user_id = $1", id)
		return err
	})
}

// SetUserDisabled disables or re-enables a user. Disabling ends every login
// session of the user.
func (s *PostgresStore) SetUserDisabled(ctx context.Context, id string, disabled bool) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE users SET disabled_at = CASE WHEN $2 THEN COALESCE(disabled_at, NOW()) END WHERE id = $1`,
			id, disabled)
		if err := affectedOne(res, err, ErrUserNotFound); err != nil {
			return err
		}
		if disabled {
			_, err = tx.ExecContext(ctx, "DELETE FROM login_sessions WHERE user_id = $1", id)
		}
		return err
	})
}

// CreateLoginSession records a login: the hash of its token, never the token.
func (s *PostgresStore) CreateLoginSession(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO login_sessions (token_hash, user_id, expires_at) VALUES ($1, $2, $3)",
		tokenHash, userID, expiresAt)
	return err
}

// GetLoginSessionUser returns the user a login token belongs to, or nil if the
// token is unknown, expired, or its user disabled.
func (s *PostgresStore) GetLoginSessionUser(ctx context.Context, tokenHash string) (*User, error) {
	return scanUser(s.db.QueryRowContext(ctx, `
		SELECT `+prefixed("u.", userColumns)+`
		FROM login_sessions ls JOIN users u ON u.id = ls.user_id
		WHERE ls.token_hash = $1 AND ls.expires_at > NOW() AND u.disabled_at IS NULL`, tokenHash))
}

func (s *PostgresStore) DeleteLoginSession(ctx context.Context, tokenHash string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM login_sessions WHERE token_hash = $1", tokenHash)
	return err
}

// --- Sessions and their members ---

const sessionColumns = "s.session_id, s.created_by, s.title, s.agent_id, s.channel, s.channel_id, s.created_at, " +
	"s.parent_session_id, s.forked_at_message_id, s.forked_by"

func scanSession(row interface{ Scan(...any) error }) (*Session, error) {
	var sess Session
	var parent, forkedBy sql.NullString
	var forkedAt sql.NullInt64
	err := row.Scan(&sess.SessionID, &sess.CreatedBy, &sess.Title, &sess.AgentID, &sess.Channel, &sess.ChannelID, &sess.CreatedAt,
		&parent, &forkedAt, &forkedBy)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sess.ParentSessionID, sess.ForkedAtMessageID, sess.ForkedBy = parent.String, forkedAt.Int64, forkedBy.String
	return &sess, nil
}

// nullIfEmpty stores "" and 0 as SQL NULL: a session that is not a fork has
// no parent, rather than a parent named "".
func nullIfEmpty(v any) any {
	switch x := v.(type) {
	case string:
		if x == "" {
			return nil
		}
	case int64:
		if x == 0 {
			return nil
		}
	}
	return v
}

// CreateSession records a session with its creator as first member.
func (s *PostgresStore) CreateSession(ctx context.Context, session Session) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO sessions (session_id, created_by, title, agent_id, channel, channel_id,
				parent_session_id, forked_at_message_id, forked_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			session.SessionID, session.CreatedBy, session.Title, session.AgentID, session.Channel, session.ChannelID,
			nullIfEmpty(session.ParentSessionID), nullIfEmpty(session.ForkedAtMessageID), nullIfEmpty(session.ForkedBy)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			"INSERT INTO session_members (session_id, user_id, added_by) VALUES ($1, $2, $2)",
			session.SessionID, session.CreatedBy)
		return err
	})
}

func (s *PostgresStore) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	return scanSession(s.db.QueryRowContext(ctx,
		"SELECT "+sessionColumns+" FROM sessions s WHERE s.session_id = $1", sessionID))
}

// GetActiveSessionByChannel returns the latest session of a channel (a
// Telegram chat, say) that userID is a member of.
func (s *PostgresStore) GetActiveSessionByChannel(ctx context.Context, userID, channel, channelID string) (*Session, error) {
	return scanSession(s.db.QueryRowContext(ctx, `
		SELECT `+sessionColumns+`
		FROM sessions s JOIN session_members m ON m.session_id = s.session_id
		WHERE m.user_id = $1 AND s.channel = $2 AND s.channel_id = $3
		ORDER BY s.created_at DESC LIMIT 1`, userID, channel, channelID))
}

// ListSessionsByUser returns the sessions userID is a member of, newest first.
func (s *PostgresStore) ListSessionsByUser(ctx context.Context, userID string) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+sessionColumns+`
		FROM sessions s JOIN session_members m ON m.session_id = s.session_id
		WHERE m.user_id = $1 ORDER BY s.created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []Session
	for rows.Next() {
		sess, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, *sess)
	}
	return sessions, rows.Err()
}

// DeleteSession removes a session, its members and its messages.
func (s *PostgresStore) DeleteSession(ctx context.Context, sessionID string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		for _, q := range []string{
			"DELETE FROM messages WHERE session_id = $1",
			"DELETE FROM memory WHERE scope = 'session' AND scope_id = $1",
			"DELETE FROM sessions WHERE session_id = $1", // members cascade
		} {
			if _, err := tx.ExecContext(ctx, q, sessionID); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListForks returns the forks of a session that userID is a member of: the
// others are not theirs to know about.
func (s *PostgresStore) ListForks(ctx context.Context, sessionID, userID string) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+sessionColumns+`
		FROM sessions s JOIN session_members m ON m.session_id = s.session_id
		WHERE s.parent_session_id = $1 AND m.user_id = $2
		ORDER BY s.forked_at_message_id, s.created_at`, sessionID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var forks []Session
	for rows.Next() {
		sess, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		forks = append(forks, *sess)
	}
	return forks, rows.Err()
}

func (s *PostgresStore) IsSessionMember(ctx context.Context, sessionID, userID string) (bool, error) {
	var ok bool
	err := s.db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM session_members WHERE session_id = $1 AND user_id = $2)",
		sessionID, userID).Scan(&ok)
	return ok, err
}

func (s *PostgresStore) ListSessionMembers(ctx context.Context, sessionID string) ([]SessionMember, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.user_id, u.email, u.display_name, m.added_by, m.added_at
		FROM session_members m JOIN users u ON u.id = m.user_id
		WHERE m.session_id = $1 ORDER BY m.added_at`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []SessionMember
	for rows.Next() {
		var m SessionMember
		if err := rows.Scan(&m.UserID, &m.Email, &m.DisplayName, &m.AddedBy, &m.AddedAt); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

// AddSessionMember adds userID to a session. Adding a member twice is a no-op.
func (s *PostgresStore) AddSessionMember(ctx context.Context, sessionID, userID, addedBy string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO session_members (session_id, user_id, added_by) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, sessionID, userID, addedBy)
	return err
}

func (s *PostgresStore) RemoveSessionMember(ctx context.Context, sessionID, userID string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM session_members WHERE session_id = $1 AND user_id = $2", sessionID, userID)
	return err
}

// --- helpers ---

func (s *PostgresStore) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// affectedOne turns an update that matched no row into notFound.
func affectedOne(res sql.Result, err error, notFound error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return notFound
	}
	return nil
}

// prefixed qualifies each column of a comma-separated list with prefix.
func prefixed(prefix, columns string) string {
	cols := strings.Split(columns, ", ")
	for i, c := range cols {
		cols[i] = prefix + c
	}
	return strings.Join(cols, ", ")
}
