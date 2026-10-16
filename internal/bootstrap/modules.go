package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/httpapi"
	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/oidcauth"
	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/postgres"
	sqsadapter "github.com/evandromadeira/jungle-wallet-service/internal/adapters/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

var infrastructureModule = fx.Module("infrastructure", fx.Provide(newLogger, newPool, newTransport, newSQS, postgres.NewStore, telemetry.New))
var applicationModule = fx.Module("application", fx.Provide(newProcessor, newOpening, newQueries, newWalletQueries, newLedgerQueries, newReconciliation, newPublisher, newReferences))
var transportModule = fx.Module("transport", fx.Provide(newConsumer, newRoutes))

type runtimeDependencies struct {
	fx.In
	Config     Config
	Logger     *slog.Logger
	Pool       *pgxpool.Pool
	Transport  *http.Transport
	Client     *awssqs.Client
	Metrics    *telemetry.Registry
	Consumer   *sqsadapter.Consumer
	Publisher  *application.OutboxPublisher
	References *application.ReferenceWorker
	Routes     routeFactory
}
type routeFactory func(context.Context) (http.Handler, error)

// newLogger creates the shared JSON logger without exposing configuration.
func newLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stderr, nil)) }

// newPool constructs a lazy pool; startup checks and runtime cleanup own its lifecycle.
func newPool(c Config, lc fx.Lifecycle) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	cfg.MaxConns = 8
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["application_name"] = "jungle-wallet-worker"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, errors.New("cannot create database pool")
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { pool.Close(); return nil }})
	return pool, nil
}

// newTransport provides a shared connection cache, closed after workers drain.
func newTransport(lc fx.Lifecycle) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	lc.Append(fx.Hook{OnStop: func(context.Context) error { transport.CloseIdleConnections(); return nil }})
	return transport
}

// newSQS uses explicit deployment credentials; example credentials live only in Compose.
func newSQS(c Config, t *http.Transport) *awssqs.Client {
	return awssqs.New(awssqs.Options{Region: c.AWSRegion, BaseEndpoint: aws.String(c.SQSEndpoint), Credentials: credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, c.AWSSessionToken), RetryMaxAttempts: 1, HTTPClient: &http.Client{Transport: t, Timeout: 15 * time.Second}})
}

// newProcessor injects persistence, independent identity generation and the clock.
func newProcessor(s *postgres.Store) (*application.ProcessWager, error) {
	return application.NewProcessWager(s, newID, time.Now)
}

// newOpening constructs internal wallet administration.
func newOpening(s *postgres.Store) (*application.OpenWallet, error) {
	return application.NewOpenWallet(s, newID, time.Now)
}

// newQueries constructs provider-scoped transaction reads.
func newQueries(s *postgres.Store) (*application.QueryWager, error) {
	return application.NewQueryWager(s)
}

// newWalletQueries constructs committed wallet reads.
func newWalletQueries(s *postgres.Store) (*application.QueryWallet, error) {
	return application.NewQueryWallet(s)
}

// newLedgerQueries constructs bounded ledger pagination.
func newLedgerQueries(s *postgres.Store) (*application.QueryLedger, error) {
	return application.NewQueryLedger(s)
}

// newReconciliation shares the structured application logger.
func newReconciliation(s *postgres.Store, l *slog.Logger) (*application.ReconcileWallet, error) {
	return application.NewReconcileWallet(s, l)
}

// newConsumer binds a queue to a trusted provider identity.
func newConsumer(c Config, client *awssqs.Client, p *application.ProcessWager) (*sqsadapter.Consumer, error) {
	return sqsadapter.NewConsumer(client, p, sqsadapter.ConsumerPolicy{QueueURL: c.InputQueueURL, ConsumerName: c.ConsumerName, ProviderID: c.ProviderID, WaitSeconds: 10, VisibilitySeconds: 60, ProcessingTimeout: 30 * time.Second, InitialRetrySeconds: 2, MaxRetrySeconds: 60})
}

// newPublisher injects the real broker sender and bounded retry policy.
func newPublisher(c Config, s *postgres.Store, client *awssqs.Client) (*application.OutboxPublisher, error) {
	sender, err := sqsadapter.NewSender(client, c.OutputQueueURL)
	if err != nil {
		return nil, err
	}
	return application.NewOutboxPublisher(s, sender, time.Now, application.OutboxPolicy{BatchSize: 100, SendTimeout: 5 * time.Second, InitialDelay: time.Second, MaxDelay: time.Minute, PollInterval: time.Second})
}

// newReferences constructs the durable reference recovery worker.
func newReferences(p *application.ProcessWager) (*application.ReferenceWorker, error) {
	return application.NewReferenceWorker(p, application.ReferencePolicy{BatchSize: 100, MaxAttempts: 100, InitialDelay: time.Second, MaxDelay: time.Minute, TTL: 24 * time.Hour, PollInterval: time.Second})
}

// newRoutes defers OIDC discovery until OnStart while injecting all handler dependencies.
func newRoutes(c Config, t *http.Transport, o *application.OpenWallet, p *application.ProcessWager, q *application.QueryWager, w *application.QueryWallet, l *application.QueryLedger, r *application.ReconcileWallet) routeFactory {
	return func(ctx context.Context) (http.Handler, error) {
		auth, err := oidcauth.New(ctx, c.OIDCIssuer, c.OIDCAudience, &http.Client{Transport: t, Timeout: 5 * time.Second})
		if err != nil {
			return nil, err
		}
		return httpapi.New(auth, o, p, q, w, l, r)
	}
}
