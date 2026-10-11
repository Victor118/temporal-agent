package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/victor/temporal-agent/access"
)

// Who may use which tool (docs/design/permissions.md): groups of users, and
// the rules that close a server's tools or a name pattern to everyone but
// their holders. The decision is access.Allowed's; the store keeps the rows.

// Group is a set of users a rule names at once.
type Group struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Members   []string  `json:"members"` // user IDs
	CreatedAt time.Time `json:"created_at"`
}

// AccessRule is a rule and what the sweep knows of it.
type AccessRule struct {
	access.Rule
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// LastMatchedAt is when the sweep last saw it target a published tool
	// or an agent; nil = never yet.
	LastMatchedAt *time.Time `json:"last_matched_at,omitempty"`
	// UnmatchedAlertedAt is when the admins were told it targets nothing
	// any more; cleared once it targets something again.
	UnmatchedAlertedAt *time.Time `json:"unmatched_alerted_at,omitempty"`
}

var (
	ErrGroupExists   = errors.New("a group already has this name")
	ErrGroupNotFound = errors.New("group not found")
	ErrRuleNotFound  = errors.New("access rule not found")
)

// SoleHolderError refuses to delete a group that is the only holder of
// rules: they would close their tools to everyone but the admins, silently.
type SoleHolderError struct {
	Rules []AccessRule
}

func (e *SoleHolderError) Error() string {
	targets := make([]string, len(e.Rules))
	for i, r := range e.Rules {
		targets[i] = r.Target
	}
	return fmt.Sprintf("the group is the only holder of the rules on %s: change them first", strings.Join(targets, ", "))
}

const accessSchema = `
		-- A set of users an access rule names at once.
		CREATE TABLE IF NOT EXISTS groups (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL CHECK (name <> ''),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_name ON groups (lower(name));
		CREATE TABLE IF NOT EXISTS group_members (
			group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
			user_id  TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (group_id, user_id)
		);
		CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(user_id);

		-- A rule closes what it targets (an MCP server's tools, or the tools
		-- a name pattern matches) to everyone but its holders: its groups and
		-- its users. last_matched_at: the sweep last saw it target something;
		-- unmatched_alerted_at: the admins were told it targets nothing.
		CREATE TABLE IF NOT EXISTS access_rules (
			id                   BIGSERIAL PRIMARY KEY,
			target_kind          TEXT NOT NULL CHECK (target_kind IN ('mcp_server', 'pattern')),
			target               TEXT NOT NULL CHECK (target <> ''),
			created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_matched_at      TIMESTAMPTZ,
			unmatched_alerted_at TIMESTAMPTZ
		);
		-- A group goes from the rules it holds with others when it is deleted;
		-- the only holder of a rule is not deleted (DeleteGroup).
		CREATE TABLE IF NOT EXISTS access_rule_groups (
			rule_id  BIGINT NOT NULL REFERENCES access_rules(id) ON DELETE CASCADE,
			group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
			PRIMARY KEY (rule_id, group_id)
		);
		CREATE TABLE IF NOT EXISTS access_rule_users (
			rule_id BIGINT NOT NULL REFERENCES access_rules(id) ON DELETE CASCADE,
			user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			PRIMARY KEY (rule_id, user_id)
		);
`

// ListGroups lists the groups by name, with their members.
func (s *PostgresStore) ListGroups(ctx context.Context) ([]Group, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.id, g.name, g.created_at, COALESCE(json_agg(m.user_id ORDER BY m.user_id) FILTER (WHERE m.user_id IS NOT NULL), '[]')
		FROM groups g LEFT JOIN group_members m ON m.group_id = g.id
		GROUP BY g.id ORDER BY lower(g.name)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Name, &g.CreatedAt, (*stringList)(&g.Members)); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// GetGroup returns a group with its members, nil if there is none.
func (s *PostgresStore) GetGroup(ctx context.Context, id string) (*Group, error) {
	groups, err := s.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		if g.ID == id {
			return &g, nil
		}
	}
	return nil, nil
}

// CreateGroup creates a group without members; ErrGroupExists if its name,
// in any case, is another's.
func (s *PostgresStore) CreateGroup(ctx context.Context, g Group) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO groups (id, name) VALUES ($1, $2)`, g.ID, strings.TrimSpace(g.Name))
	if isUniqueViolation(err) {
		return ErrGroupExists
	}
	return err
}

// RenameGroup renames a group.
func (s *PostgresStore) RenameGroup(ctx context.Context, id, name string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE groups SET name = $2 WHERE id = $1`, id, strings.TrimSpace(name))
	if isUniqueViolation(err) {
		return ErrGroupExists
	}
	return affectedOne(res, err, ErrGroupNotFound)
}

// DeleteGroup deletes a group, and takes it out of the rules it holds with
// others. The only holder of a rule is refused (SoleHolderError), in the
// same transaction, the rules locked: a rule changed meanwhile is read as
// it is.
func (s *PostgresStore) DeleteGroup(ctx context.Context, id string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT r.id FROM access_rules r
			JOIN access_rule_groups rg ON rg.rule_id = r.id AND rg.group_id = $1
			WHERE NOT EXISTS (SELECT 1 FROM access_rule_groups o WHERE o.rule_id = r.id AND o.group_id <> $1)
			  AND NOT EXISTS (SELECT 1 FROM access_rule_users u WHERE u.rule_id = r.id)
			ORDER BY r.id
			FOR UPDATE OF r`, id)
		if err != nil {
			return err
		}
		var sole []int64
		for rows.Next() {
			var rid int64
			if err := rows.Scan(&rid); err != nil {
				rows.Close()
				return err
			}
			sole = append(sole, rid)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(sole) > 0 {
			rules, err := listAccessRules(ctx, tx, sole)
			if err != nil {
				return err
			}
			return &SoleHolderError{Rules: rules}
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, id)
		return affectedOne(res, err, ErrGroupNotFound)
	})
}

// AddGroupMember adds a user to a group; already there is no error.
func (s *PostgresStore) AddGroupMember(ctx context.Context, groupID, userID string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO group_members (group_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, groupID, userID)
	return err
}

// RemoveGroupMember takes a user out of a group.
func (s *PostgresStore) RemoveGroupMember(ctx context.Context, groupID, userID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`, groupID, userID)
	return err
}

// stringList scans a JSON array of strings (json_agg), empty as nil.
type stringList []string

func (l *stringList) Scan(src any) error {
	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("stringList: cannot scan %T", src)
	}
	*l = nil
	if err := json.Unmarshal(b, (*[]string)(l)); err != nil {
		return err
	}
	if len(*l) == 0 {
		*l = nil
	}
	return nil
}

// ListAccessRules lists every rule, by ID, with its holders.
func (s *PostgresStore) ListAccessRules(ctx context.Context) ([]AccessRule, error) {
	return listAccessRules(ctx, s.db, nil)
}

// GetAccessRule returns a rule, nil if there is none.
func (s *PostgresStore) GetAccessRule(ctx context.Context, id int64) (*AccessRule, error) {
	rules, err := listAccessRules(ctx, s.db, []int64{id})
	if err != nil || len(rules) == 0 {
		return nil, err
	}
	return &rules[0], nil
}

// listAccessRules lists the rules of ids, every rule when nil.
func listAccessRules(ctx context.Context, q queryer, ids []int64) ([]AccessRule, error) {
	query := `
		SELECT r.id, r.target_kind, r.target, r.created_at, r.updated_at, r.last_matched_at, r.unmatched_alerted_at,
		       COALESCE((SELECT json_agg(group_id ORDER BY group_id) FROM access_rule_groups WHERE rule_id = r.id), '[]'),
		       COALESCE((SELECT json_agg(user_id ORDER BY user_id) FROM access_rule_users WHERE rule_id = r.id), '[]')
		FROM access_rules r`
	var args []any
	if ids != nil {
		query += ` WHERE r.id = ANY($1)`
		args = append(args, ids)
	}
	rows, err := q.QueryContext(ctx, query+` ORDER BY r.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rules []AccessRule
	for rows.Next() {
		var r AccessRule
		if err := rows.Scan(&r.ID, &r.Kind, &r.Target, &r.CreatedAt, &r.UpdatedAt, &r.LastMatchedAt, &r.UnmatchedAlertedAt,
			(*stringList)(&r.Groups), (*stringList)(&r.Users)); err != nil {
			return nil, err
		}
		rules = append(rules, r)
	}
	return rules, rows.Err()
}

// CreateAccessRule creates a rule with its holders, and returns its ID.
func (s *PostgresStore) CreateAccessRule(ctx context.Context, r access.Rule) (int64, error) {
	var id int64
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `INSERT INTO access_rules (target_kind, target) VALUES ($1, $2) RETURNING id`,
			r.Kind, r.Target).Scan(&id); err != nil {
			return err
		}
		return setHolders(ctx, tx, id, r)
	})
	return id, err
}

// UpdateAccessRule replaces a rule's target and holders. What the sweep
// knew of it starts again.
func (s *PostgresStore) UpdateAccessRule(ctx context.Context, r access.Rule) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE access_rules SET target_kind = $2, target = $3, updated_at = NOW(),
				last_matched_at = NULL, unmatched_alerted_at = NULL
			WHERE id = $1`, r.ID, r.Kind, r.Target)
		if err := affectedOne(res, err, ErrRuleNotFound); err != nil {
			return err
		}
		for _, q := range []string{`DELETE FROM access_rule_groups WHERE rule_id = $1`, `DELETE FROM access_rule_users WHERE rule_id = $1`} {
			if _, err := tx.ExecContext(ctx, q, r.ID); err != nil {
				return err
			}
		}
		return setHolders(ctx, tx, r.ID, r)
	})
}

// setHolders writes a rule's groups and users, each once.
func setHolders(ctx context.Context, tx *sql.Tx, id int64, r access.Rule) error {
	for _, g := range slices.Compact(slices.Sorted(slices.Values(r.Groups))) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO access_rule_groups (rule_id, group_id) VALUES ($1, $2)`, id, g); err != nil {
			return err
		}
	}
	for _, u := range slices.Compact(slices.Sorted(slices.Values(r.Users))) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO access_rule_users (rule_id, user_id) VALUES ($1, $2)`, id, u); err != nil {
			return err
		}
	}
	return nil
}

// DeleteAccessRule deletes a rule.
func (s *PostgresStore) DeleteAccessRule(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM access_rules WHERE id = $1`, id)
	return affectedOne(res, err, ErrRuleNotFound)
}

// AccessSnapshot reads what the rules need, in one transaction: the rules,
// and every user with their role, state and groups. An installation is a
// few dozen people: the workers read it all with their catalog.
func (s *PostgresStore) AccessSnapshot(ctx context.Context) (access.Snapshot, error) {
	snap := access.Snapshot{Users: map[string]access.User{}}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return snap, err
	}
	defer tx.Rollback()
	rules, err := listAccessRules(ctx, tx, nil)
	if err != nil {
		return snap, err
	}
	for _, r := range rules {
		snap.Rules = append(snap.Rules, r.Rule)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT u.id, u.role = 'admin', u.disabled_at IS NOT NULL,
		       COALESCE((SELECT json_agg(group_id ORDER BY group_id) FROM group_members WHERE user_id = u.id), '[]')
		FROM users u`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var u access.User
		if err := rows.Scan(&u.ID, &u.Admin, &u.Disabled, (*stringList)(&u.Groups)); err != nil {
			return snap, err
		}
		snap.Users[u.ID] = u
	}
	return snap, rows.Err()
}

// ListMCPServers lists the MCP servers the published tools come from, by
// name: what a rule on a server may target.
func (s *PostgresStore) ListMCPServers(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT mcp_server FROM tools WHERE mcp_server <> '' ORDER BY mcp_server`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var servers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		servers = append(servers, name)
	}
	return servers, rows.Err()
}

// SetAccessRulesMatched records that the rules of ids target something now:
// a later alert that one targets nothing may go again.
func (s *PostgresStore) SetAccessRulesMatched(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `UPDATE access_rules SET last_matched_at = NOW(), unmatched_alerted_at = NULL WHERE id = ANY($1)`, ids)
	return err
}

// ClaimUnmatchedAlert reports whether the admins are to be told that rule
// id targets nothing any more: it targeted something before, and they were
// not told since. Claimed once: a second sweep, or another server, gets
// false.
func (s *PostgresStore) ClaimUnmatchedAlert(ctx context.Context, id int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE access_rules SET unmatched_alerted_at = NOW()
		WHERE id = $1 AND last_matched_at IS NOT NULL AND unmatched_alerted_at IS NULL`, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
