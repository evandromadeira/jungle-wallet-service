//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/postgres"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// repositoryStores creates two independent pools against one disposable database.
// Compose credentials stay in memory and are never printed or written to a file.
func repositoryStores(t *testing.T, extraMigrations ...string) (*postgres.Store, *postgres.Store) {
	t.Helper()
	first, second, _ := repositoryStoresWithDSN(t, extraMigrations...)
	return first, second
}

// repositoryStoresWithDSN also supplies credentials in memory for bootstrap lifecycle tests.
func repositoryStoresWithDSN(t *testing.T, extraMigrations ...string) (*postgres.Store, *postgres.Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	configCmd := exec.CommandContext(ctx, "docker", "compose", "config", "--format", "json")
	configCmd.Dir = filepath.Join("..", "..")
	raw, err := configCmd.Output()
	if err != nil {
		t.Fatal("cannot read Compose configuration")
	}
	var compose struct {
		Services map[string]struct{ Environment map[string]string }
	}
	if err = json.Unmarshal(raw, &compose); err != nil {
		t.Fatal("invalid Compose configuration")
	}
	env := compose.Services["postgres"].Environment
	portCmd := exec.CommandContext(ctx, "docker", "compose", "port", "postgres", "5432")
	portCmd.Dir = configCmd.Dir
	address, err := portCmd.Output()
	if err != nil {
		t.Fatal("cannot resolve PostgreSQL port")
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(string(address)))
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("repository_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	postgresSQL(t, "postgres", "CREATE DATABASE "+database+";")
	t.Cleanup(func() { postgresSQL(t, "postgres", "DROP DATABASE "+database+";") })
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000001_wallets.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	postgresSQL(t, database, string(migration))
	for _, name := range extraMigrations {
		data, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		postgresSQL(t, database, string(data))
	}
	stores := make([]*postgres.Store, 2)
	for i := range stores {
		cfg, err := pgxpool.ParseConfig("postgres://localhost/postgres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.Host = host
		cfg.ConnConfig.Port = uint16(portNumber)
		cfg.ConnConfig.User = env["POSTGRES_USER"]
		cfg.ConnConfig.Password = env["POSTGRES_PASSWORD"]
		cfg.ConnConfig.Database = database
		cfg.MaxConns = 4
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal("cannot create PostgreSQL pool")
		}
		t.Cleanup(pool.Close)
		if err = pool.Ping(ctx); err != nil {
			t.Fatal("cannot connect to disposable PostgreSQL database")
		}
		stores[i], err = postgres.NewStore(pool)
		if err != nil {
			t.Fatal(err)
		}
	}
	dsn := url.URL{Scheme: "postgres", Host: net.JoinHostPort(host, port), User: url.UserPassword(env["POSTGRES_USER"], env["POSTGRES_PASSWORD"]), Path: "/" + database, RawQuery: "sslmode=disable"}
	return stores[0], stores[1], dsn.String()
}

// repositoryWallet creates a deterministic initial state with sub-microsecond time precision.
func repositoryWallet(t *testing.T, id string) domain.Wallet {
	t.Helper()
	money, err := domain.NewMoney(10000, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	wallet, err := domain.NewWallet(id, "player-"+id, money, time.Date(2026, 10, 14, 10, 0, 0, 123456789, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return wallet
}

// TestWalletRepository verifies commit, rollback, invalid writes and stale updates.
func TestWalletRepository(t *testing.T) {
	store, other := repositoryStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := repositoryWallet(t, "w")
	err := store.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wallets.Create(ctx, w) })
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := other.GetWallet(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := loaded.Snapshot()
	minor, _ := snapshot.Balance.Minor()
	if minor != 10000 || snapshot.Version != 1 || snapshot.CreatedAt.Nanosecond() != 123456000 {
		t.Fatalf("unexpected persisted state: %+v", snapshot)
	}
	if _, err = store.GetWallet(ctx, "missing"); !errors.Is(err, application.ErrWalletNotFound) {
		t.Fatal(err)
	}
	for _, duplicate := range []domain.Wallet{w, func() domain.Wallet {
		s, _ := w.Snapshot()
		s.ID = "duplicate-player"
		v, e := domain.RehydrateWallet(s)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}()} {
		err = store.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wallets.Create(ctx, duplicate) })
		if !errors.Is(err, application.ErrWalletAlreadyExists) {
			t.Fatalf("duplicate: %v", err)
		}
	}
	marker := errors.New("deliberate rollback")
	rolled := repositoryWallet(t, "rolled")
	err = store.WithinTransaction(ctx, func(r application.Repositories) error {
		if e := r.Wallets.Create(ctx, rolled); e != nil {
			return e
		}
		return marker
	})
	if !errors.Is(err, marker) {
		t.Fatal(err)
	}
	if _, err = store.GetWallet(ctx, "rolled"); !errors.Is(err, application.ErrWalletNotFound) {
		t.Fatal("rollback retained wallet")
	}
	func() {
		defer func() {
			if recover() != marker {
				t.Error("panic was not preserved")
			}
		}()
		_ = store.WithinTransaction(ctx, func(r application.Repositories) error {
			if e := r.Wallets.Create(ctx, rolled); e != nil {
				return e
			}
			panic(marker)
		})
	}()
	if _, err = store.GetWallet(ctx, "rolled"); !errors.Is(err, application.ErrWalletNotFound) {
		t.Fatal("panic retained wallet")
	}
	amount, _ := domain.NewMoney(8000, "BRL")
	after, err := w.Debit(amount, snapshot.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Original nanosecond timestamps must still match PostgreSQL's microsecond storage.
	err = store.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wallets.Update(ctx, w, after) })
	if err != nil {
		t.Fatal(err)
	}
	err = other.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wallets.Update(ctx, w, after) })
	if !errors.Is(err, application.ErrWalletConflict) {
		t.Fatalf("stale update: %v", err)
	}
	err = store.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wallets.Update(ctx, after, after) })
	if !errors.Is(err, application.ErrInvalidWalletUpdate) {
		t.Fatalf("no-op: %v", err)
	}
	loaded, err = store.GetWallet(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ = loaded.Snapshot()
	minor, _ = snapshot.Balance.Minor()
	if minor != 2000 || snapshot.Version != 2 {
		t.Fatalf("lost state: %+v", snapshot)
	}
	if _, err = loaded.Debit(amount, snapshot.UpdatedAt); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatal(err)
	}
	// Cancellation after a write must not prevent rollback cleanup.
	cancelled, stop := context.WithCancel(ctx)
	err = store.WithinTransaction(cancelled, func(r application.Repositories) error {
		if e := r.Wallets.Create(cancelled, rolled); e != nil {
			return e
		}
		stop()
		return cancelled.Err()
	})
	stop()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err = store.GetWallet(ctx, "rolled"); !errors.Is(err, application.ErrWalletNotFound) {
		t.Fatal("cancelled transaction retained wallet")
	}
}

// TestWalletConcurrentUpdates allows only one write from two identical old versions.
func TestWalletConcurrentUpdates(t *testing.T) {
	first, second := repositoryStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	w := repositoryWallet(t, "w")
	if err := first.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wallets.Create(ctx, w) }); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for _, store := range []*postgres.Store{first, second} {
		go func(s *postgres.Store) {
			results <- s.WithinTransaction(ctx, func(r application.Repositories) error {
				before, err := r.Wallets.Get(ctx, "w")
				if err != nil {
					return err
				}
				ready <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				money, _ := domain.NewMoney(8000, "BRL")
				after, err := before.Debit(money, time.Date(2026, 10, 14, 11, 0, 0, 0, time.UTC))
				if err != nil {
					return err
				}
				return r.Wallets.Update(ctx, before, after)
			})
		}(store)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("concurrent readers timed out")
		}
	}
	close(release)
	success, conflict := 0, 0
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err == nil {
				success++
			} else if errors.Is(err, application.ErrWalletConflict) {
				conflict++
			} else {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent updates timed out")
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
	final, err := first.GetWallet(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	state, _ := final.Snapshot()
	minor, _ := state.Balance.Minor()
	if minor != 2000 || state.Version != 2 {
		t.Fatal("concurrent updates corrupted balance")
	}
}

// TestWalletRowLock checks database locking and independence of different wallets.
func TestWalletRowLock(t *testing.T) {
	first, second := repositoryStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, id := range []string{"w", "other"} {
		w := repositoryWallet(t, id)
		if err := first.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wallets.Create(ctx, w) }); err != nil {
			t.Fatal(err)
		}
	}
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- first.WithinTransaction(ctx, func(r application.Repositories) error {
			if _, err := r.Wallets.GetForUpdate(ctx, "w"); err != nil {
				return err
			}
			close(locked)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-locked:
	case <-ctx.Done():
		t.Fatal("lock acquisition timed out")
	}
	short, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	err := second.WithinTransaction(short, func(r application.Repositories) error { _, e := r.Wallets.GetForUpdate(short, "w"); return e })
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same wallet was not locked: %v", err)
	}
	err = second.WithinTransaction(ctx, func(r application.Repositories) error { _, e := r.Wallets.GetForUpdate(ctx, "other"); return e })
	if err != nil {
		t.Fatalf("another wallet was blocked: %v", err)
	}
}
