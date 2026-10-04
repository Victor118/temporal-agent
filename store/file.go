package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// File is a file an agent published in a session: what is known of it
// without reading its content. The content is kept apart (FileStore), so
// listing a session's files reads no bytes.
type File struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	// TurnKey is the session turn that published it (TurnKey): a
	// sub-agent's file goes to the turn that launched it.
	TurnKey string `json:"turn_key"`
	// CallID is the tool call that published it: with the turn and the
	// name, it identifies the file, so a retried call stores nothing more.
	CallID  string `json:"call_id"`
	AgentID string `json:"agent_id"` // the agent whose tool call published it
	UserID  string `json:"user_id"`  // the user that turn answered
	Name    string `json:"name"`
	// ContentType is what the file says it is, from its name or its first
	// bytes. Informative only: it never decides how the file is served.
	ContentType string    `json:"content_type"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"` // of the content, hex
	CreatedAt   time.Time `json:"created_at"`
}

// ErrFileSessionGone is the error of a file published in a session that no
// longer exists: nothing was stored.
var ErrFileSessionGone = errors.New("the session no longer exists")

// FileStore keeps published files: their metadata, and their content apart.
// PostgresStore is one; an object store (S3) would keep the content.
type FileStore interface {
	// SaveFile stores f and its content, and returns f as stored (its
	// creation time). A file the same call of the same turn already stored
	// under that name is returned as it is, f and content ignored. A
	// session that no longer exists is ErrFileSessionGone.
	SaveFile(ctx context.Context, f File, content []byte) (File, error)
	// GetFile returns a file's metadata; nil when there is none.
	GetFile(ctx context.Context, id string) (*File, error)
	// ReadFileContent returns a file's content; nil, with no error, when
	// there is none.
	ReadFileContent(ctx context.Context, id string) ([]byte, error)
	// ListSessionFiles returns a session's files, oldest first.
	ListSessionFiles(ctx context.Context, sessionID string) ([]File, error)
}

const fileColumns = "id, session_id, turn_key, call_id, agent_id, user_id, name, content_type, size, sha256, created_at"

// SaveFile writes the file's row and its content in one transaction: no
// file is listed without its content. The row is keyed by its session, turn,
// call and name (idx_files_call): a second write of it finds the first.
func (s *PostgresStore) SaveFile(ctx context.Context, f File, content []byte) (File, error) {
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `
			INSERT INTO files (id, session_id, turn_key, call_id, agent_id, user_id, name, content_type, size, sha256)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (session_id, turn_key, call_id, name) DO NOTHING
			RETURNING created_at`,
			f.ID, f.SessionID, f.TurnKey, f.CallID, f.AgentID, f.UserID, f.Name, f.ContentType, f.Size, f.SHA256,
		).Scan(&f.CreatedAt)
		if errors.Is(err, sql.ErrNoRows) {
			// Stored already: the first write stands, content included.
			f, err = scanFile(tx.QueryRowContext(ctx, `SELECT `+fileColumns+` FROM files
				WHERE session_id = $1 AND turn_key = $2 AND call_id = $3 AND name = $4`,
				f.SessionID, f.TurnKey, f.CallID, f.Name))
			return err
		}
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO file_contents (file_id, data) VALUES ($1, $2)`, f.ID, content)
		return err
	})
	if isForeignKeyViolation(err) {
		return File{}, ErrFileSessionGone
	}
	if err != nil {
		return File{}, err
	}
	return f, nil
}

// isForeignKeyViolation reports a row referring to one that does not exist,
// Postgres error 23503.
func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

func (s *PostgresStore) GetFile(ctx context.Context, id string) (*File, error) {
	f, err := scanFile(s.db.QueryRowContext(ctx, `SELECT `+fileColumns+` FROM files WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func (s *PostgresStore) ReadFileContent(ctx context.Context, id string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM file_contents WHERE file_id = $1`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return data, err
}

func (s *PostgresStore) ListSessionFiles(ctx context.Context, sessionID string) ([]File, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+fileColumns+` FROM files WHERE session_id = $1 ORDER BY created_at, id`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []File
	for rows.Next() {
		f, err := scanFile(rows)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

func scanFile(row interface{ Scan(...any) error }) (File, error) {
	var f File
	err := row.Scan(&f.ID, &f.SessionID, &f.TurnKey, &f.CallID, &f.AgentID, &f.UserID, &f.Name, &f.ContentType, &f.Size, &f.SHA256, &f.CreatedAt)
	return f, err
}
