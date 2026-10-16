package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/migration"
	"github.com/evandromadeira/jungle-wallet-service/migrations"
)

// run executes explicit up/status only; connection strings and SQL details are never printed.
func run() int {
	if len(os.Args) != 2 || (os.Args[1] != "up" && os.Args[1] != "status") {
		fmt.Fprintln(os.Stderr, "usage: wallet-migrate up|status")
		return 2
	}
	runner, e := migration.New(migrations.Files)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 1
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 2*time.Minute)
	defer cancel()
	dsn := os.Getenv("WALLET_MIGRATION_DATABASE_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "WALLET_MIGRATION_DATABASE_URL is required")
		return 1
	}
	if os.Args[1] == "up" {
		n, e := runner.Up(ctx, dsn)
		fmt.Printf("Committed migrations: %d\n", n)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			return 1
		}
		return 0
	}
	status, e := runner.Status(ctx, dsn)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 1
	}
	for _, s := range status {
		state := "pending"
		if s.Applied {
			state = "applied"
		}
		fmt.Printf("%06d %s %s\n", s.Version, s.Name, state)
	}
	return 0
}

// main propagates a useful process exit code after deferred cleanup has run.
func main() { os.Exit(run()) }
