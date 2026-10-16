//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// postgresSQL executes SQL through the running Compose service without exposing
// credentials. Each call uses a fresh connection and stops on the first SQL error.
func postgresSQL(t *testing.T, database, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "exec", "-T", "postgres", "sh", "-c",
		"exec psql -X -U \"$POSTGRES_USER\" -d \"$1\" -v ON_ERROR_STOP=1 -f -", "sh", database)
	cmd.Dir = filepath.Join("..", "..")
	cmd.Stdin = strings.NewReader(sql)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("PostgreSQL check failed: %v\n%s", err, output)
	}
}

// TestWalletMigration verifies up, constraints, down, and reapplication in a
// uniquely named disposable database. It never migrates the development database.
func TestWalletMigration(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	up := read("migrations/000001_wallets.up.sql")
	down := read("migrations/000001_wallets.down.sql")
	checks := read("tests/integration/wallets.sql")
	database := fmt.Sprintf("wallet_migration_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	postgresSQL(t, "postgres", "CREATE DATABASE "+database+";")
	t.Cleanup(func() { postgresSQL(t, "postgres", "DROP DATABASE "+database+";") })
	postgresSQL(t, database, up)
	postgresSQL(t, database, checks)
	postgresSQL(t, database, down)
	postgresSQL(t, database, `DO $$ BEGIN IF to_regclass('public.wallets') IS NOT NULL THEN RAISE EXCEPTION 'Down did not remove wallets'; END IF; END $$;`)
	postgresSQL(t, database, up)
	postgresSQL(t, database, checks)
}
