// Package pgstore keeps rules and assignments in PostgreSQL, the source of
// truth for configuration. Quota state stays in Redis.
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

const (
	pgForeignKeyViolation = "23503"
	pgUniqueViolation     = "23505"
)

const assignmentColumns = `id, version, tenant, namespace, rule_id, created_at, updated_at`

const ruleColumns = `r.id, r.name, r.algorithm, r.limit_count, r.period_ms, r.burst, r.burst_window_ms, r.fail_open, r.version, r.created_at, r.updated_at`

type Store struct {
	pool *pgxpool.Pool
}

var _ persistence.RuleStore = (*Store)(nil)

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) CreateRule(ctx context.Context, r types.Rule) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO rules
		(id, name, algorithm, limit_count, period_ms, burst, burst_window_ms, fail_open, version, created_at, updated_at)
		VALUES ($1, $11, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		r.ID, r.Algorithm, r.Limit, r.Period.Milliseconds(), r.Burst, r.BurstWindow.Milliseconds(),
		r.FailOpen, r.Version, r.CreatedAt, r.UpdatedAt, r.Name)
	if err != nil {
		return fmt.Errorf("create rule: %w", err)
	}
	return nil
}

func (s *Store) UpdateRule(ctx context.Context, r types.Rule, expectedVersion int64) error {
	tag, err := s.pool.Exec(ctx, `UPDATE rules SET
		algorithm = $3, limit_count = $4, period_ms = $5, burst = $6, burst_window_ms = $7,
		fail_open = $8, version = $9, updated_at = $10, name = $11
		WHERE id = $1 AND version = $2`,
		r.ID, expectedVersion, r.Algorithm, r.Limit, r.Period.Milliseconds(), r.Burst,
		r.BurstWindow.Milliseconds(), r.FailOpen, r.Version, r.UpdatedAt, r.Name)
	if err != nil {
		return fmt.Errorf("update rule: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	if _, err := s.GetRule(ctx, r.ID); err != nil {
		return err
	}
	return types.ErrVersionMismatch
}

func (s *Store) DeleteRule(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM rules WHERE id = $1`, id)
	if isForeignKeyViolation(err) {
		return types.ErrRuleInUse
	}
	if err != nil {
		return fmt.Errorf("delete rule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return types.ErrNotFound
	}
	return nil
}

func (s *Store) GetRule(ctx context.Context, id string) (types.Rule, error) {
	rows, _ := s.pool.Query(ctx, `SELECT `+ruleColumns+` FROM rules r WHERE r.id = $1`, id)
	rule, err := pgx.CollectExactlyOneRow(rows, scanRule)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Rule{}, types.ErrNotFound
	}
	if err != nil {
		return types.Rule{}, fmt.Errorf("get rule: %w", err)
	}
	return rule, nil
}

func (s *Store) ListRules(ctx context.Context) ([]types.Rule, error) {
	rows, _ := s.pool.Query(ctx, `SELECT `+ruleColumns+` FROM rules r ORDER BY r.created_at, r.id`)
	rules, err := pgx.CollectRows(rows, scanRule)
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	return rules, nil
}

func (s *Store) CreateAssignment(ctx context.Context, a types.Assignment) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO assignments
		(id, version, tenant, namespace, rule_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		a.ID, a.Version, a.Tenant, a.Namespace, a.RuleID, a.CreatedAt, a.UpdatedAt)
	if err := assignmentWriteError(err); err != nil {
		return fmt.Errorf("create assignment: %w", err)
	}
	return nil
}

func (s *Store) UpdateAssignment(ctx context.Context, a types.Assignment, expectedVersion int64) error {
	tag, err := s.pool.Exec(ctx, `UPDATE assignments SET
		tenant = $3, namespace = $4, rule_id = $5, version = $6, updated_at = $7
		WHERE id = $1 AND version = $2`,
		a.ID, expectedVersion, a.Tenant, a.Namespace, a.RuleID, a.Version, a.UpdatedAt)
	if err := assignmentWriteError(err); err != nil {
		return fmt.Errorf("update assignment: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	if _, err := s.GetAssignment(ctx, a.ID); err != nil {
		return err
	}
	return types.ErrVersionMismatch
}

func (s *Store) DeleteAssignment(ctx context.Context, id string) (types.Assignment, error) {
	rows, _ := s.pool.Query(ctx, `DELETE FROM assignments WHERE id = $1 RETURNING `+assignmentColumns, id)
	a, err := pgx.CollectExactlyOneRow(rows, scanAssignment)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Assignment{}, types.ErrNotFound
	}
	if err != nil {
		return types.Assignment{}, fmt.Errorf("delete assignment: %w", err)
	}
	return a, nil
}

func (s *Store) GetAssignment(ctx context.Context, id string) (types.Assignment, error) {
	rows, _ := s.pool.Query(ctx, `SELECT `+assignmentColumns+` FROM assignments WHERE id = $1`, id)
	a, err := pgx.CollectExactlyOneRow(rows, scanAssignment)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Assignment{}, types.ErrNotFound
	}
	if err != nil {
		return types.Assignment{}, fmt.Errorf("get assignment: %w", err)
	}
	return a, nil
}

func (s *Store) ListAssignments(ctx context.Context, f persistence.AssignmentFilter) ([]types.Assignment, error) {
	var (
		where []string
		args  []any
	)
	for col, v := range map[string]string{"tenant": f.Tenant, "namespace": f.Namespace, "rule_id": f.RuleID} {
		if v != "" {
			args = append(args, v)
			where = append(where, col+" = $"+strconv.Itoa(len(args)))
		}
	}
	q := `SELECT ` + assignmentColumns + ` FROM assignments`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	rows, _ := s.pool.Query(ctx, q+" ORDER BY namespace, tenant", args...)
	out, err := pgx.CollectRows(rows, scanAssignment)
	if err != nil {
		return nil, fmt.Errorf("list assignments: %w", err)
	}
	return out, nil
}

func (s *Store) Bindings(ctx context.Context) ([]types.Binding, error) {
	rows, _ := s.pool.Query(ctx, `SELECT a.tenant, a.namespace, `+ruleColumns+`
		FROM assignments a JOIN rules r ON r.id = a.rule_id`)
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (types.Binding, error) {
		var b types.Binding
		err := scanRuleInto(row, &b.Rule, &b.Tenant, &b.Namespace)
		return b, err
	})
	if err != nil {
		return nil, fmt.Errorf("load bindings: %w", err)
	}
	return out, nil
}

func (s *Store) Resolve(ctx context.Context, tenant, namespace string) (types.Resolution, error) {
	rows, _ := s.pool.Query(ctx, `SELECT a.tenant, a.namespace, `+ruleColumns+`
		FROM assignments a JOIN rules r ON r.id = a.rule_id
		WHERE a.namespace = $2 AND a.tenant IN ($1, $3)
		ORDER BY a.tenant = $3
		LIMIT 1`, tenant, namespace, constants.DefaultTenant)
	b, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (types.Binding, error) {
		var b types.Binding
		err := scanRuleInto(row, &b.Rule, &b.Tenant, &b.Namespace)
		return b, err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Resolution{}, nil
	}
	if err != nil {
		return types.Resolution{}, fmt.Errorf("resolve rule: %w", err)
	}
	match := types.MatchTenant
	if b.Tenant == constants.DefaultTenant {
		match = types.MatchDefault
	}
	return types.Resolution{Found: true, Match: match, Rule: b.Rule}, nil
}

func scanRule(row pgx.CollectableRow) (types.Rule, error) {
	var r types.Rule
	err := scanRuleInto(row, &r)
	return r, err
}

// scanRuleInto scans optional leading columns (prefix) followed by ruleColumns.
func scanRuleInto(row pgx.CollectableRow, r *types.Rule, prefix ...any) error {
	var periodMs, burstWindowMs int64
	dest := append(prefix, &r.ID, &r.Name, &r.Algorithm, &r.Limit, &periodMs, &r.Burst, &burstWindowMs,
		&r.FailOpen, &r.Version, &r.CreatedAt, &r.UpdatedAt)
	if err := row.Scan(dest...); err != nil {
		return err
	}
	r.Period = time.Duration(periodMs) * time.Millisecond
	r.BurstWindow = time.Duration(burstWindowMs) * time.Millisecond
	r.CreatedAt, r.UpdatedAt = r.CreatedAt.UTC(), r.UpdatedAt.UTC()
	return nil
}

func scanAssignment(row pgx.CollectableRow) (types.Assignment, error) {
	var a types.Assignment
	err := row.Scan(&a.ID, &a.Version, &a.Tenant, &a.Namespace, &a.RuleID, &a.CreatedAt, &a.UpdatedAt)
	a.CreatedAt, a.UpdatedAt = a.CreatedAt.UTC(), a.UpdatedAt.UTC()
	return a, err
}

func assignmentWriteError(err error) error {
	switch {
	case err == nil:
		return nil
	case isViolation(err, pgForeignKeyViolation):
		return types.ErrUnknownRule
	case isViolation(err, pgUniqueViolation):
		return types.ErrAssignmentExists
	}
	return err
}

func isForeignKeyViolation(err error) bool { return isViolation(err, pgForeignKeyViolation) }

func isViolation(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}
