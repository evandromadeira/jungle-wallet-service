//go:build integration

package integration

import (
	"context"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/migration"
	"github.com/evandromadeira/jungle-wallet-service/migrations"
	"github.com/jackc/pgx/v5"
)

// TestMigrationRunner exercises actual DDL/history transactions and independent session locks.
func TestMigrationRunner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, dsn := repositoryStoresWithDSN(t)
	c, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal("connect test database")
	}
	defer c.Close(context.Background())
	runner, e := migration.New(migrations.Files)
	if e != nil {
		t.Fatal(e)
	}
	// The fixture initially has a manually applied wallet table: no automatic adoption.
	if _, e = runner.Up(ctx, dsn); e == nil {
		t.Fatal("untracked schema adopted")
	}
	down, e := fs.ReadFile(migrations.Files, "000001_wallets.down.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Exec(ctx, string(down)); e != nil {
		t.Fatal(e)
	}
	status, e := runner.Status(ctx, dsn)
	if e != nil || len(status) != 5 {
		t.Fatalf("empty status: %v", e)
	}
	var exists bool
	if e = c.QueryRow(ctx, `SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&exists); e != nil || exists {
		t.Fatal("status mutated database")
	}
	var wg sync.WaitGroup
	type result struct {
		n int
		e error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; n, e := runner.Up(ctx, dsn); results <- result{n, e} }()
	}
	close(start)
	wg.Wait()
	close(results)
	total := 0
	for r := range results {
		if r.e != nil {
			t.Fatal(r.e)
		}
		total += r.n
	}
	if total != 5 {
		t.Fatalf("committed versions: %d", total)
	}
	if n, e := runner.Up(ctx, dsn); e != nil || n != 0 {
		t.Fatalf("repeat: %d %v", n, e)
	}
	status, e = runner.Status(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range status {
		if !s.Applied {
			t.Fatal("missing committed version")
		}
	}
	files := fstest.MapFS{}
	names, _ := fs.Glob(migrations.Files, "*.up.sql")
	for _, name := range names {
		b, e := fs.ReadFile(migrations.Files, name)
		if e != nil {
			t.Fatal(e)
		}
		files[name] = &fstest.MapFile{Data: b}
	}
	original := files[names[0]]
	files[names[0]] = &fstest.MapFile{Data: append(append([]byte{}, original.Data...), []byte("\n")...)}
	changed, e := migration.New(files)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = changed.Up(ctx, dsn); e == nil {
		t.Fatal("changed committed migration accepted")
	}
	files[names[0]] = original
	// A failed new migration cannot leave either its table or history row behind.
	files["000006_failure.up.sql"] = &fstest.MapFile{Data: []byte("BEGIN;\nCREATE TABLE public.partial_migration(id integer); SELECT 1/0;\nCOMMIT;\n")}
	broken, e := migration.New(files)
	if e != nil {
		t.Fatal(e)
	}
	if n, e := broken.Up(ctx, dsn); e == nil || n != 0 {
		t.Fatal("failure not reported")
	}
	if e = c.QueryRow(ctx, `SELECT to_regclass('public.partial_migration') IS NOT NULL`).Scan(&exists); e != nil || exists {
		t.Fatal("partial DDL survived rollback")
	}
	var count int
	if e = c.QueryRow(ctx, `SELECT count(*) FROM public.schema_migrations`).Scan(&count); e != nil || count != 5 {
		t.Fatal("failed migration recorded")
	}
	// Cancellation also rolls back DDL and releases the session lock for a later retry.
	files["000006_failure.up.sql"] = &fstest.MapFile{Data: []byte("BEGIN;\nCREATE TABLE public.partial_migration(id integer); SELECT pg_sleep(5);\nCOMMIT;\n")}
	slow, e := migration.New(files)
	if e != nil {
		t.Fatal(e)
	}
	short, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	_, e = slow.Up(short, dsn)
	stop()
	if e == nil {
		t.Fatal("cancelled migration succeeded")
	}
	files["000006_failure.up.sql"] = &fstest.MapFile{Data: []byte("BEGIN;\nCREATE TABLE public.partial_migration(id integer);\nCOMMIT;\n")}
	fixed, e := migration.New(files)
	if e != nil {
		t.Fatal(e)
	}
	if n, e := fixed.Up(ctx, dsn); e != nil || n != 1 {
		t.Fatalf("retry after cancellation: %d %v", n, e)
	}
	if _, e = runner.Status(ctx, dsn); e == nil {
		t.Fatal("older binary accepted unknown applied version")
	}
}
