package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const lockID int64 = 719836421004

type script struct {
	version              int
	name, checksum, body string
}

// Runner owns a validated copy of trusted migration scripts, never external request input.
type Runner struct{ scripts []script }

// Status records whether a bundled version has been committed to this database.
type Status struct {
	Version int
	Name    string
	Applied bool
}

// New validates sequential versions and existing transaction wrappers before any database access.
// CRLF is normalized to LF so Windows checkouts produce the same checksums as Linux.
func New(files fs.FS) (*Runner, error) {
	names, e := fs.Glob(files, "*.up.sql")
	if e != nil || len(names) == 0 {
		return nil, errors.New("no migrations found")
	}
	sort.Strings(names)
	pattern := regexp.MustCompile(`^([0-9]{6})_([a-z0-9_]+)\.up\.sql$`)
	r := &Runner{}
	for i, name := range names {
		match := pattern.FindStringSubmatch(name)
		if match == nil {
			return nil, errors.New("invalid migration filename")
		}
		version, _ := strconv.Atoi(match[1])
		if version != i+1 {
			return nil, errors.New("migration versions must start at one and remain contiguous")
		}
		data, e := fs.ReadFile(files, name)
		if e != nil {
			return nil, errors.New("cannot read migration")
		}
		normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
		sum := sha256.Sum256([]byte(normalized))
		body := strings.TrimSpace(normalized)
		if !strings.HasPrefix(body, "BEGIN;\n") || !strings.HasSuffix(body, "\nCOMMIT;") {
			return nil, errors.New("migration must have explicit BEGIN/COMMIT boundary")
		}
		// The runner replaces only the outer wrappers; function bodies remain untouched.
		body = strings.TrimSuffix(strings.TrimPrefix(body, "BEGIN;\n"), "\nCOMMIT;")
		r.scripts = append(r.scripts, script{version, match[2], hex.EncodeToString(sum[:]), body})
	}
	return r, nil
}

// connect opens a dedicated session: the advisory lock cannot escape into a connection pool.
func connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("WALLET_DATABASE_URL is required")
	}
	c, e := pgx.Connect(ctx, dsn)
	if e != nil {
		return nil, errors.New("cannot connect to migration database")
	}
	return c, nil
}

// closeSession releases all session locks even when the operation's context was cancelled.
func closeSession(c *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.Close(ctx)
}

// history validates a complete prefix, rejecting unknown versions, gaps or modified scripts.
func (r *Runner) history(ctx context.Context, c *pgx.Conn) (int, error) {
	var exists bool
	if e := c.QueryRow(ctx, `SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&exists); e != nil {
		return 0, errors.New("cannot inspect migration history")
	}
	if !exists {
		var legacy bool
		if e := c.QueryRow(ctx, `SELECT to_regclass('public.wallets') IS NOT NULL OR to_regclass('public.wager_transactions') IS NOT NULL OR to_regclass('public.wallet_ledger_entries') IS NOT NULL OR to_regclass('public.outbox_events') IS NOT NULL OR to_regclass('public.inbox_messages') IS NOT NULL`).Scan(&legacy); e != nil {
			return 0, errors.New("cannot inspect existing schema")
		}
		if legacy {
			return 0, errors.New("untracked schema found; automatic adoption is disabled")
		}
		return 0, nil
	}
	rows, e := c.Query(ctx, `SELECT version,name,checksum FROM public.schema_migrations ORDER BY version`)
	if e != nil {
		return 0, errors.New("cannot read migration history")
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var v int
		var name, hash string
		if e = rows.Scan(&v, &name, &hash); e != nil {
			return 0, errors.New("invalid migration history")
		}
		if count >= len(r.scripts) || v != count+1 {
			return 0, errors.New("unknown or noncontiguous migration history")
		}
		s := r.scripts[count]
		if name != s.name || hash != s.checksum {
			return 0, fmt.Errorf("migration %06d differs from committed history", v)
		}
		count++
	}
	if rows.Err() != nil {
		return 0, errors.New("cannot finish reading migration history")
	}
	return count, nil
}

// Status reads under the same session lock as Up, without creating tables or applying SQL.
func (r *Runner) Status(ctx context.Context, dsn string) ([]Status, error) {
	c, e := connect(ctx, dsn)
	if e != nil {
		return nil, e
	}
	defer closeSession(c)
	if _, e = c.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); e != nil {
		return nil, errors.New("cannot acquire migration lock")
	}
	count, e := r.history(ctx, c)
	if e != nil {
		return nil, e
	}
	result := make([]Status, len(r.scripts))
	for i, s := range r.scripts {
		result[i] = Status{s.version, s.name, i < count}
	}
	return result, nil
}

// Up applies each pending version and its checksum in one transaction while holding the session lock.
// The returned count includes committed versions even when a later migration fails.
func (r *Runner) Up(ctx context.Context, dsn string) (int, error) {
	c, e := connect(ctx, dsn)
	if e != nil {
		return 0, e
	}
	defer closeSession(c)
	if _, e = c.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); e != nil {
		return 0, errors.New("cannot acquire migration lock")
	}
	count, e := r.history(ctx, c)
	if e != nil {
		return 0, e
	}
	applied := 0
	for _, s := range r.scripts[count:] {
		if e = r.apply(ctx, c, s); e != nil {
			return applied, e
		}
		applied++
	}
	return applied, nil
}

// apply replaces the script's outer transaction to atomically include the history row.
func (r *Runner) apply(ctx context.Context, c *pgx.Conn, s script) (err error) {
	tx, e := c.Begin(ctx)
	if e != nil {
		return errors.New("cannot begin migration transaction")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, e = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.schema_migrations(version INTEGER PRIMARY KEY CHECK(version>0),name TEXT NOT NULL,checksum TEXT NOT NULL CHECK(checksum ~ '^[0-9a-f]{64}$'),applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)`); e != nil {
		return errors.New("cannot create migration history")
	}
	if _, e = tx.Exec(ctx, s.body); e != nil {
		var pg *pgconn.PgError
		if errors.As(e, &pg) {
			return fmt.Errorf("migration %06d failed (SQLSTATE %s); transaction rolled back", s.version, pg.Code)
		}
		return fmt.Errorf("migration %06d interrupted; inspect status before retrying", s.version)
	}
	if _, e = tx.Exec(ctx, `INSERT INTO public.schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, s.version, s.name, s.checksum); e != nil {
		return errors.New("cannot record migration")
	}
	if e = tx.Commit(ctx); e != nil {
		return errors.New("migration commit could not be confirmed; inspect status before retrying")
	}
	return nil
}
