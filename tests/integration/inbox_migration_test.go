//go:build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestInboxMigration verifies inbox identity and durable completion on real PostgreSQL,
// then reverses all migrations and reapplies them in an isolated database.
func TestInboxMigration(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	database := fmt.Sprintf("inbox_migration_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	postgresSQL(t, "postgres", "CREATE DATABASE "+database+";")
	t.Cleanup(func() { postgresSQL(t, "postgres", "DROP DATABASE "+database+";") })
	for i := 0; i < 2; i++ {
		postgresSQL(t, database, read("migrations/000001_wallets.up.sql"))
		postgresSQL(t, database, read("migrations/000002_wager_transactions.up.sql"))
		postgresSQL(t, database, read("migrations/000003_wallet_ledger_entries.up.sql"))
		postgresSQL(t, database, read("migrations/000004_outbox_events.up.sql"))
		postgresSQL(t, database, read("migrations/000005_inbox_messages.up.sql"))
		postgresSQL(t, database, read("tests/integration/inbox_messages.sql"))
		postgresSQL(t, database, read("migrations/000005_inbox_messages.down.sql"))
		postgresSQL(t, database, `DO $$ BEGIN IF to_regclass('public.inbox_messages') IS NOT NULL OR to_regprocedure('public.protect_inbox_message()') IS NOT NULL OR to_regprocedure('public.reject_inbox_removal()') IS NOT NULL THEN RAISE EXCEPTION 'Incomplete inbox reversal'; END IF; END $$;`)
		postgresSQL(t, database, read("migrations/000004_outbox_events.down.sql"))
		postgresSQL(t, database, `DO $$ BEGIN IF to_regclass('public.outbox_events') IS NOT NULL OR to_regprocedure('public.protect_outbox_event()') IS NOT NULL OR to_regprocedure('public.reject_outbox_removal()') IS NOT NULL THEN RAISE EXCEPTION 'Incomplete outbox reversal'; END IF; END $$;`)
		postgresSQL(t, database, read("migrations/000003_wallet_ledger_entries.down.sql"))
		postgresSQL(t, database, `DO $$ BEGIN IF to_regclass('public.wallet_ledger_entries') IS NOT NULL OR to_regprocedure('public.validate_wallet_ledger_entry()') IS NOT NULL OR to_regprocedure('public.reject_wallet_ledger_mutation()') IS NOT NULL THEN RAISE EXCEPTION 'Incomplete ledger reversal'; END IF; END $$;`)
		postgresSQL(t, database, read("migrations/000002_wager_transactions.down.sql"))
		postgresSQL(t, database, `DO $$ BEGIN IF to_regclass('public.wager_transactions') IS NOT NULL OR to_regprocedure('public.protect_wager_transaction()') IS NOT NULL THEN RAISE EXCEPTION 'Incomplete reversal'; END IF; END $$;`)
		postgresSQL(t, database, read("migrations/000001_wallets.down.sql"))
	}
}
