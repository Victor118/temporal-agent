package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// Why a fork's report cannot be posted. None goes away on a retry.
var (
	ErrReportForkGone        = errors.New("the fork was deleted")
	ErrReportParentGone      = errors.New("the fork no longer has this parent session")
	ErrReportNotForkMember   = errors.New("the reporter is no longer a member of the fork")
	ErrReportNotParentMember = errors.New("the reporter is not a member of the parent session")
	ErrReportStale           = errors.New("another report of this fork was posted since this one started")
)

// ForkReport is a fork's report, to post into its parent session.
type ForkReport struct {
	ForkSessionID   string
	ParentSessionID string
	// ReporterID sends it: a member of the fork and of the parent, who
	// signs Message.
	ReporterID string
	// The fork's messages it covers: after From (the fork's last reported
	// message when it started), up to UpTo.
	From, UpTo int64
	Message    Message
}

// AppendForkReport posts r into the parent, under ForkReportKey, and records
// it on the fork as its latest report, in one transaction; it returns the
// report's message ID in the parent. A report already posted (a retry) is
// returned as it is.
//
// The fork's row is held while it runs: two reports of one fork are posted
// one after the other, and the parent cannot be deleted in between (deleting
// it unlinks the fork, which waits). Refused, with nothing written: the fork
// is gone (ErrReportForkGone) or no longer the parent's
// (ErrReportParentGone), the reporter left the fork (ErrReportNotForkMember)
// or the parent (ErrReportNotParentMember), another report moved the fork's
// last reported message since From (ErrReportStale): this one would cover it
// again.
func (s *PostgresStore) AppendForkReport(ctx context.Context, r ForkReport) (int64, error) {
	key := ForkReportKey(r.ForkSessionID, r.From, r.UpTo)
	data, err := json.Marshal(r.Message)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		var parent sql.NullString
		var reported int64
		err := tx.QueryRowContext(ctx,
			"SELECT parent_session_id, last_reported_message_id FROM sessions WHERE session_id = $1 FOR UPDATE",
			r.ForkSessionID).Scan(&parent, &reported)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrReportForkGone
		case err != nil:
			return err
		case parent.String != r.ParentSessionID:
			return ErrReportParentGone
		}
		err = tx.QueryRowContext(ctx,
			"SELECT id FROM messages WHERE session_id = $1 AND msg_key = $2", r.ParentSessionID, key).Scan(&id)
		if err == nil {
			return nil // posted already: a retry
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if reported != r.From {
			return ErrReportStale
		}
		// The reporter signs what the fork says: they must still read it,
		// and the parent's members must still count them in.
		var inFork, inParent bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM session_members WHERE session_id = $1 AND user_id = $3),
			       EXISTS (SELECT 1 FROM session_members WHERE session_id = $2 AND user_id = $3)`,
			r.ForkSessionID, r.ParentSessionID, r.ReporterID).Scan(&inFork, &inParent); err != nil {
			return err
		}
		switch {
		case !inFork:
			return ErrReportNotForkMember
		case !inParent:
			return ErrReportNotParentMember
		}
		if err := tx.QueryRowContext(ctx,
			"INSERT INTO messages (session_id, msg_key, data) VALUES ($1, $2, $3) RETURNING id",
			r.ParentSessionID, key, string(data)).Scan(&id); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE sessions SET last_reported_message_id = $2, last_report_id = $3, last_reported_at = NOW()
			WHERE session_id = $1`, r.ForkSessionID, r.UpTo, id)
		return err
	})
	return id, err
}
