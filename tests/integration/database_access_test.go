//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/postgres"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/bootstrap"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/evandromadeira/jungle-wallet-service/internal/migration"
	"github.com/evandromadeira/jungle-wallet-service/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// restrictedStore provisions unique test identities; no development roles or data are changed.
func restrictedStore(t *testing.T) (*postgres.Store, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, adminDSN := repositoryStoresWithDSN(t)
	parsed, e := url.Parse(adminDSN)
	if e != nil {
		t.Fatal("invalid test DSN")
	}
	database := strings.TrimPrefix(parsed.Path, "/")
	down, _ := fs.ReadFile(migrations.Files, "000001_wallets.down.sql")
	postgresSQL(t, database, string(down))
	prefix := fmt.Sprintf("access_%d", time.Now().UnixNano())
	migrator, worker := prefix+"_m", prefix+"_w"
	// Remove only these uniquely named test roles and their objects in the disposable database.
	t.Cleanup(func() {
		postgresSQL(t, database, fmt.Sprintf("SELECT format('DROP OWNED BY %%I',rolname) FROM pg_roles WHERE rolname IN ('%s','%s') \\gexec\n", migrator, worker))
		postgresSQL(t, "postgres", "DROP ROLE IF EXISTS "+migrator+","+worker+";")
	})
	provision := func(file string) {
		phase := "Grants"
		if file == "roles.sql" {
			phase = "Roles"
		}
		var cmd *exec.Cmd
		if runtime.GOOS == "windows" {
			cmd = exec.CommandContext(ctx, "powershell", "-NoProfile", "-File", filepath.Join("..", "..", "scripts", "Set-DatabaseAccess.ps1"), "-Phase", phase, "-Database", database)
		} else {
			b, e := os.ReadFile(filepath.Join("..", "..", "deploy", "postgres", file))
			if e != nil {
				t.Fatal(e)
			}
			cmd = exec.CommandContext(ctx, "docker", "compose", "exec", "-T", "-e", "WALLET_MIGRATOR_USER", "-e", "WALLET_WORKER_USER", "-e", "WALLET_MIGRATOR_PASSWORD", "-e", "WALLET_WORKER_PASSWORD", "postgres", "sh", "-c", `exec psql -X -U "$POSTGRES_USER" -d "$1" -v ON_ERROR_STOP=1 -f -`, "sh", database)
			cmd.Dir = filepath.Join("..", "..")
			cmd.Stdin = strings.NewReader(string(b))
		}
		cmd.Env = append(os.Environ(), "WALLET_MIGRATOR_USER="+migrator, "WALLET_WORKER_USER="+worker, "WALLET_MIGRATOR_PASSWORD=migration-test-only", "WALLET_WORKER_PASSWORD=worker-test-only")

		if e := cmd.Run(); e != nil {
			t.Fatal("database access provisioning failed:", file, e)
		}
	}
	provision("roles.sql")
	provision("roles.sql")
	parsed.User = url.UserPassword(migrator, "migration-test-only")
	runner, e := migration.New(migrations.Files)
	if e != nil {
		t.Fatal(e)
	}
	if n, e := runner.Up(ctx, parsed.String()); e != nil || n != 5 {
		t.Fatalf("migration identity failed: %d %v", n, e)
	}
	provision("grants.sql")
	provision("grants.sql")
	parsed.User = url.UserPassword(worker, "worker-test-only")
	workerDSN := parsed.String()
	pool, e := pgxpool.New(ctx, workerDSN)
	if e != nil {
		t.Fatal("worker pool creation failed")
	}
	t.Cleanup(pool.Close)
	if e := pool.Ping(ctx); e != nil {
		t.Fatal("worker cannot connect")
	}
	store, e := postgres.NewStore(pool)
	if e != nil {
		t.Fatal(e)
	}
	return store, workerDSN, adminDSN
}

// TestDatabaseAccess checks actual worker authentication, permitted finance and denied administration.
func TestDatabaseAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	store, dsn, adminDSN := restrictedStore(t)
	amount, _ := domain.NewMoney(10000, "BRL")
	wallet, e := openingService(t, store, "restricted").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: amount, CorrelationID: "c"})
	if e != nil {
		t.Fatal(e)
	}
	ws, _ := wallet.Snapshot()
	seq := 0
	p, e := application.NewProcessWager(store, func() (string, error) { seq++; return fmt.Sprintf("restricted-wager-%d", seq), nil }, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	bet, _ := domain.NewMoney(100, "BRL")
	if _, e = p.Execute(ctx, application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: "c", Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: "bet", IdempotencyKey: "bet", WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: bet}}); e != nil {
		t.Fatal(e)
	}
	c, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal("worker connect failed")
	}
	defer c.Close(context.Background())
	for _, sql := range []string{
		"CREATE TABLE public.forbidden(id integer)", "CREATE SCHEMA forbidden", "CREATE TEMP TABLE forbidden(id integer)",
		"ALTER TABLE public.wallets DISABLE TRIGGER ALL", "DROP TABLE public.wallets",
		"UPDATE public.wallet_ledger_entries SET id=id", "DELETE FROM public.wallet_ledger_entries", "TRUNCATE public.wallet_ledger_entries",
		"UPDATE public.wallets SET player_id=player_id", "UPDATE public.outbox_events SET payload=payload", "UPDATE public.inbox_messages SET payload_hash=payload_hash",
		"DELETE FROM public.inbox_messages", "DELETE FROM public.outbox_events", "DELETE FROM public.wager_transactions", "DELETE FROM public.wallets",
		"SELECT * FROM public.schema_migrations", "INSERT INTO public.schema_migrations(version,name,checksum) VALUES(99,'bad',repeat('a',64))",
		"SET session_replication_role=replica",
	} {
		_, e := c.Exec(ctx, sql)
		var pg *pgconn.PgError
		if !errors.As(e, &pg) || pg.Code != "42501" {
			t.Errorf("expected permission denial for %s, got %v", sql, e)
		}
	}
	// The worker bootstrap must reject the administrator identity before launching any workers.
	app := bootstrap.New(bootstrap.Config{AWSRegion: "us-east-1", AWSAccessKeyID: "test", AWSSecretAccessKey: "test", DatabaseURL: adminDSN, SQSEndpoint: "http://127.0.0.1:4566", InputQueueURL: "http://127.0.0.1:4566/input.fifo", OutputQueueURL: "http://127.0.0.1:4566/output", ProviderID: "a", ConsumerName: "a"})
	if e := app.Start(ctx); e == nil {
		_ = app.Stop(ctx)
		t.Fatal("administrator accepted by worker bootstrap")
	} else if !strings.Contains(e.Error(), "restricted non-owner") {
		t.Fatal("administrator failed for the wrong reason", e)
	}
}

// TestFunctionDefaults verifies both PUBLIC ACLs and actual execution as the worker.
func TestFunctionDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, workerDSN, adminDSN := restrictedStore(t)
	admin, e := pgx.Connect(ctx, adminDSN)
	if e != nil {
		t.Fatal("admin connection failed")
	}
	defer admin.Close(context.Background())
	var owner string
	if e = admin.QueryRow(ctx, `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.wallets'::regclass`).Scan(&owner); e != nil {
		t.Fatal(e)
	}
	// SET ROLE makes the actual creator identity determine the default ACL.
	if _, e = admin.Exec(ctx, "SET ROLE "+pgx.Identifier{owner}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `CREATE FUNCTION public.permission_probe() RETURNS integer LANGUAGE sql AS 'SELECT 42'`); e != nil {
		t.Fatal(e)
	}
	var publicExecute bool
	if e = admin.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pg_proc p CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE p.oid='public.permission_probe()'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE')`).Scan(&publicExecute); e != nil {
		t.Fatal(e)
	}
	if publicExecute {
		t.Error("PUBLIC received EXECUTE on a future migrator function")
	}
	var answer int
	if e = admin.QueryRow(ctx, `SELECT public.permission_probe()`).Scan(&answer); e != nil || answer != 42 {
		t.Fatal("function owner lost execution permission")
	}
	worker, e := pgx.Connect(ctx, workerDSN)
	if e != nil {
		t.Fatal("worker connection failed")
	}
	defer worker.Close(context.Background())
	_, e = worker.Exec(ctx, `SELECT public.permission_probe()`)
	var pg *pgconn.PgError
	if !errors.As(e, &pg) || pg.Code != "42501" {
		t.Errorf("worker must be denied function execution; got %v", e)
	}
}
