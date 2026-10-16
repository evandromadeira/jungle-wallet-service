//go:build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLedgerMigration verifies ledger integrity and immutability on real PostgreSQL,
// then reverses all migrations and reapplies them in an isolated database.
func TestLedgerMigration(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	database := fmt.Sprintf("ledger_migration_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	postgresSQL(t, "postgres", "CREATE DATABASE "+database+";")
	t.Cleanup(func() { postgresSQL(t, "postgres", "DROP DATABASE "+database+";") })
	for i := 0; i < 2; i++ {
		postgresSQL(t, database, read("migrations/000001_wallets.up.sql"))
		postgresSQL(t, database, read("migrations/000002_wager_transactions.up.sql"))
		postgresSQL(t, database, read("migrations/000003_wallet_ledger_entries.up.sql"))
		postgresSQL(t, database, read("tests/integration/wallet_ledger_entries.sql"))
		postgresSQL(t, database, read("migrations/000003_wallet_ledger_entries.down.sql"))
		postgresSQL(t, database, `DO $$ BEGIN IF to_regclass('public.wallet_ledger_entries') IS NOT NULL OR to_regprocedure('public.validate_wallet_ledger_entry()') IS NOT NULL OR to_regprocedure('public.reject_wallet_ledger_mutation()') IS NOT NULL THEN RAISE EXCEPTION 'Incomplete ledger reversal'; END IF; END $$;`)
		postgresSQL(t, database, read("migrations/000002_wager_transactions.down.sql"))
		postgresSQL(t, database, `DO $$ BEGIN IF to_regclass('public.wager_transactions') IS NOT NULL OR to_regprocedure('public.protect_wager_transaction()') IS NOT NULL THEN RAISE EXCEPTION 'Incomplete reversal'; END IF; END $$;`)
		postgresSQL(t, database, read("migrations/000001_wallets.down.sql"))
	}
}
