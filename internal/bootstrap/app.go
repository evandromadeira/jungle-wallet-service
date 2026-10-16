package bootstrap

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/httpapi"

	"github.com/evandromadeira/jungle-wallet-service/internal/telemetry"
	"log/slog"
	"net"
	"net/http"

	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	sqsadapter "github.com/evandromadeira/jungle-wallet-service/internal/adapters/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// New assembles a local worker application; construction never opens external resources.
// Fx owns SIGINT/SIGTERM, startup rollback and shutdown deadlines through App.Run.
func New(c Config, options ...fx.Option) *fx.App {
	base := []fx.Option{fx.NopLogger, fx.Error(c.Validate()), fx.Supply(c), infrastructureModule, applicationModule, transportModule, fx.Provide(newRuntime), fx.Invoke(register), fx.StartTimeout(30 * time.Second), fx.StopTimeout(20 * time.Second)}
	return fx.New(append(base, options...)...)
}

type runner interface{ Run(context.Context) error }
type runtime struct {
	client     *awssqs.Client
	metrics    *telemetry.Registry
	consumer   *sqsadapter.Consumer
	publisher  *application.OutboxPublisher
	references *application.ReferenceWorker
	routes     routeFactory
	ready      atomic.Bool
	config     Config
	log        *slog.Logger
	pool       *pgxpool.Pool
	transport  *http.Transport
	cancel     context.CancelFunc
	done       chan struct{}
	server     *http.Server
	listener   net.Listener
}

// newRuntime stores configuration without opening pools or starting goroutines.
func newRuntime(d runtimeDependencies) *runtime {
	r := &runtime{config: d.Config, log: d.Logger, pool: d.Pool, transport: d.Transport, client: d.Client, metrics: d.Metrics, consumer: d.Consumer, publisher: d.Publisher, references: d.References, routes: d.Routes}
	d.Metrics.Collect = func(ctx context.Context) (map[string]float64, error) { return r.collectMetrics(ctx, d.Client) }
	return r
}

// register connects runtime ownership to Fx's lifecycle.
func register(lc fx.Lifecycle, r *runtime, shutdown fx.Shutdowner) {
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error { return r.start(ctx, shutdown) }, OnStop: r.stop})
}

// newID creates UUID v4 identifiers independently across processes.
func newID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", errors.New("cannot generate identifier")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// start checks prerequisites and constructs workers before launching any background work.
// Any startup error closes resources here, since a failed hook does not receive OnStop.
func (r *runtime) start(ctx context.Context, shutdown fx.Shutdowner) (err error) {
	defer func() {
		if err != nil {
			r.log.Error("worker startup failed", "error", err.Error())
			r.closeResources()
		}
	}()
	var e error
	if e = r.pool.Ping(ctx); e != nil {
		return errors.New("PostgreSQL startup check failed")
	}
	var unsafeRole bool
	e = r.pool.QueryRow(ctx, `SELECT r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
 OR EXISTS(SELECT FROM pg_auth_members m WHERE m.member=r.oid)
 OR has_schema_privilege(current_user,'public','CREATE')
 OR has_database_privilege(current_user,current_database(),'CREATE')
 OR has_database_privilege(current_user,current_database(),'TEMP')
 OR EXISTS(SELECT FROM pg_shdepend d WHERE d.refobjid=r.oid AND d.deptype='o')
 FROM pg_roles r WHERE r.rolname=current_user`).Scan(&unsafeRole)
	if e != nil || unsafeRole {
		return errors.New("workers require a restricted non-owner database identity")
	}
	var schemaReady bool
	e = r.pool.QueryRow(ctx, `SELECT to_regclass('public.wallets') IS NOT NULL AND to_regclass('public.wager_transactions') IS NOT NULL AND to_regclass('public.wallet_ledger_entries') IS NOT NULL AND to_regclass('public.outbox_events') IS NOT NULL AND to_regclass('public.inbox_messages') IS NOT NULL`).Scan(&schemaReady)
	if e != nil || !schemaReady {
		return errors.New("required schema missing; apply migrations before starting workers")
	}
	client := r.client
	metrics := r.metrics
	consumer, publisher, references := r.consumer, r.publisher, r.references
	if e = checkQueues(ctx, client, r.config); e != nil {
		return e
	}
	if r.config.HTTPAddress != "" {
		routes, e := r.routes(ctx)
		if e != nil {
			return e
		}
		business := routes
		routes = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			business.ServeHTTP(w, req.WithContext(telemetry.With(req.Context(), metrics)))
		})
		r.server = &http.Server{Handler: httpapi.WithHealth(routes, func(ctx context.Context) error {
			if !r.ready.Load() {
				return errors.New("runtime not ready")
			}
			if err := r.pool.Ping(ctx); err != nil {
				return err
			}
			if err := checkQueues(ctx, client, r.config); err != nil {
				return err
			}
			if !r.ready.Load() {
				return errors.New("runtime stopping")
			}
			return nil
		}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
		r.listener, e = net.Listen("tcp", r.config.HTTPAddress)
		if e != nil {
			return errors.New("HTTP listen failed")
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	runCtx, cancel := context.WithCancel(telemetry.With(context.Background(), metrics))
	r.cancel = func() { r.ready.Store(false); cancel() }
	r.done = make(chan struct{})
	r.ready.Store(true)
	var wg sync.WaitGroup
	if r.server != nil {
		wg.Add(2)
		go func() {
			defer wg.Done()
			e := r.server.Serve(r.listener)
			if runCtx.Err() == nil && !errors.Is(e, http.ErrServerClosed) {
				r.log.Error("HTTP server stopped unexpectedly")
				cancel()
				_ = shutdown.Shutdown(fx.ExitCode(1))
			}
		}()
		go func() {
			defer wg.Done()
			<-runCtx.Done()
			cleanup, finish := context.WithTimeout(context.Background(), 15*time.Second)
			defer finish()
			if r.server.Shutdown(cleanup) != nil {
				_ = r.server.Close()
			}
		}()
	}
	for name, w := range map[string]runner{"consumer": consumer, "publisher": publisher, "references": references} {
		wg.Add(1)
		go func(name string, w runner) {
			defer wg.Done()
			e := w.Run(runCtx)
			if runCtx.Err() == nil {
				// Raw errors may include SQL details. Report the worker, never its payload or credentials.
				r.log.Error("worker stopped unexpectedly", "worker", name, "returned_error", e != nil)
				cancel()
				_ = shutdown.Shutdown(fx.ExitCode(1))
			}
		}(name, w)
	}
	go func() { wg.Wait(); r.closeResources(); close(r.done) }()
	r.log.Info("workers started")
	return nil
}

// checkQueues fails startup for missing queues, wrong types or absent redrive configuration.
func checkQueues(ctx context.Context, client *awssqs.Client, c Config) error {
	input, e := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(c.InputQueueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameFifoQueue, types.QueueAttributeNameRedrivePolicy}})
	if e != nil {
		return errors.New("input SQS startup check failed")
	}
	var policy struct {
		Target string `json:"deadLetterTargetArn"`
		Max    string `json:"maxReceiveCount"`
	}
	if json.Unmarshal([]byte(input.Attributes["RedrivePolicy"]), &policy) != nil || policy.Target == "" {
		return errors.New("input queue requires a redrive policy")
	}
	n, e := strconv.Atoi(policy.Max)
	if e != nil || n < 1 || input.Attributes["FifoQueue"] != "true" {
		return errors.New("invalid input FIFO redrive configuration")
	}
	output, e := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(c.OutputQueueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameFifoQueue}})
	if e != nil {
		return errors.New("output SQS startup check failed")
	}
	if output.Attributes["FifoQueue"] == "true" {
		return errors.New("output queue must be standard")
	}
	return nil
}

// stop cancels work and waits for transaction/receipt cleanup before closing dependencies.
func (r *runtime) stop(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.done:
		r.log.Info("workers stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closeResources releases only resources owned by this runtime, in reverse creation order.
func (r *runtime) closeResources() {
	if r.listener != nil {
		_ = r.listener.Close()
	}
	if r.transport != nil {
		r.transport.CloseIdleConnections()
	}
	if r.pool != nil {
		r.pool.Close()
	}
}
