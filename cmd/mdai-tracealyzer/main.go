package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-tracealyzer/internal/app"
	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/emit"
	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	"github.com/mydecisive/mdai-tracealyzer/internal/observability"
	"github.com/mydecisive/mdai-tracealyzer/internal/schema"
	"github.com/mydecisive/mdai-tracealyzer/internal/sweep"
	"github.com/mydecisive/mdai-tracealyzer/internal/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	liveResponse      = "ok\n"
	readyResponse     = "ok\n"
	notReadyResponse  = "not ready\n"
	readHeaderTimeout = 5 * time.Second
	// numServers is admin + OTLP/gRPC + OTLP/HTTP — the count of serve
	// goroutines, which sets the errCh buffer and the shutdown drain count.
	numServers = 3
)

type runMode int

const (
	runModeServe runMode = iota
	runModeMigrate
)

var (
	configPathFlag = flag.String("config", "", "path to tracealyzer YAML config")
	migrateFlag    = flag.Bool("migrate", false, "check and create required GreptimeDB schema objects, then exit")
)

func main() {
	flag.Parse()
	mode := runModeServe
	if *migrateFlag {
		mode = runModeMigrate
	}
	os.Exit(mainExit(*configPathFlag, mode))
}

func mainExit(configPath string, mode runMode) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, fmt.Errorf("load config: %w", err))
		return 1
	}

	logger, err := observability.NewLogger(cfg.Service.LogLevel)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, fmt.Errorf("init logger: %w", err))
		return 1
	}
	defer func() { _ = logger.Sync() }()

	if mode == runModeMigrate {
		manager := schema.New(cfg.Emitter, logger)
		if err := manager.Migrate(ctx); err != nil {
			logger.Error("schema migration failed", zap.Error(err))
			return 1
		}
		logger.Info("schema migration completed")
		return 0
	}

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("service failed", zap.Error(err))
		return 1
	}
	return 0
}

//nolint:funlen // TODO: split into startProbes / startIngestServers / awaitServeOrSignal / finalizeShutdown.
func run(ctx context.Context, cfg *config.Config, logger *zap.Logger) error {
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	ready := app.NewReadiness("otlp_grpc", "otlp_http", "buffer")
	registry := observability.NewRegistry()
	ingestMetrics := ingest.NewMetrics(registry)
	bufferMetrics := buffer.NewMetrics(registry, ingestMetrics.MalformedDrainCounter())
	sweepMetrics := sweep.NewMetrics(registry)

	valkeyBuffer, err := buffer.NewValkeyBuffer(ctx, buffer.ValkeyOptions{
		Addr:     cfg.Buffer.ValkeyAddr,
		DB:       cfg.Buffer.ValkeyDB,
		Password: cfg.Buffer.ValkeyPassword,
		MaxTTL:   cfg.Buffer.MaxTTL.Duration(),
		Metrics:  bufferMetrics,
		Logger:   logger,
	})
	if err != nil {
		return fmt.Errorf("valkey buffer: %w", err)
	}
	defer valkeyBuffer.Close()
	ready.Mark("buffer")

	// The emitter owns an internal goroutine keyed on its own Close(ctx)
	// lifecycle; parent cancellation must not abort an in-flight flush, so its
	// worker deliberately does not inherit ctx.
	emitter, err := emit.New(cfg.Emitter, logger, registry) //nolint:contextcheck
	if err != nil {
		return fmt.Errorf("emitter: %w", err)
	}
	// Safety net for early-return paths. The main shutdown path closes the
	// emitter explicitly under shutdownCtx; a subsequent Close on the same
	// emitter blocks on the already-closed done channel and is idempotent.
	defer func() {
		closeCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), cfg.Service.ShutdownGrace.Duration(),
		)
		defer cancel()
		_ = emitter.Close(closeCtx)
	}()

	schemaManager := schema.New(cfg.Emitter, logger)
	schemaErr := waitForSchemaReady(ctx, cfg.Emitter, logger, schemaManager)
	if schemaErr != nil {
		return fmt.Errorf("schema readiness: %w", schemaErr)
	}

	sweeper, err := sweep.New(valkeyBuffer, topologyComputer{}, emitter, sweep.Config{
		QuietPeriod:    cfg.Buffer.QuietPeriod.Duration(),
		MaxTTL:         cfg.Buffer.MaxTTL.Duration(),
		Interval:       cfg.Buffer.SweepInterval.Duration(),
		WorkerPoolSize: cfg.Buffer.SweepWorkerPoolSize,
	}, sweepMetrics, logger)
	if err != nil {
		return fmt.Errorf("sweeper: %w", err)
	}

	listeners, err := openListeners(ctx, cfg)
	if err != nil {
		return err
	}

	adminServer := newAdminServer(buildAdminMux(registry, ready))
	grpcServer := ingest.NewGRPCServer(valkeyBuffer, ingestMetrics, logger)
	httpServer := ingest.NewHTTPServer(valkeyBuffer, ingestMetrics, logger)

	logger.Info("service starting",
		zap.String("metrics_endpoint", cfg.Service.MetricsEndpoint),
		zap.String("otlp_grpc_endpoint", cfg.Ingestion.OTLPGRPCEndpoint),
		zap.String("otlp_http_endpoint", cfg.Ingestion.OTLPHTTPEndpoint),
		zap.String("valkey_addr", cfg.Buffer.ValkeyAddr),
		zap.String("log_level", cfg.Service.LogLevel),
		zap.Duration("shutdown_grace", cfg.Service.ShutdownGrace.Duration()),
		zap.Strings("readiness_pending", ready.Pending()),
	)
	var sweeperWG sync.WaitGroup
	sweeperWG.Go(func() {
		if err := sweeper.Run(ctx); err != nil {
			logger.Error("sweeper exited", zap.Error(err))
		}
	})

	errCh := make(chan serveResult, numServers)
	go serve("admin", errCh, func() error { return adminServer.Serve(listeners.admin) })
	go serve("otlp_grpc", errCh, func() error { return grpcServer.Serve(listeners.grpc) })
	ready.Mark("otlp_grpc")
	go serve("otlp_http", errCh, func() error { return httpServer.Serve(listeners.http) })
	ready.Mark("otlp_http")

	var (
		serveErr  error
		remaining = numServers
	)
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case result := <-errCh:
		remaining = numServers - 1
		if result.err != nil && !errors.Is(result.err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("serve %s: %w", result.name, result.err)
		}
	}

	// Propagate shutdown to the sweeper goroutine when a server crash — not a
	// signal — triggered the wind-down; SIGTERM already cancelled ctx.
	cancelRun()

	shutdownCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), cfg.Service.ShutdownGrace.Duration(),
	)
	defer cancel()

	shutdownErr := gracefulShutdown(shutdownCtx, errCh, remaining, logger, adminServer, grpcServer, httpServer)

	// Wait for the sweeper's in-flight tick before closing the emitter so a
	// late Emit cannot land after the queue starts draining; bounded by
	// shutdownCtx so a stalled Drain cannot block exit past the grace.
	waitForSweeper(shutdownCtx, &sweeperWG, logger)
	if err := emitter.Close(shutdownCtx); err != nil {
		logger.Warn("close emitter", zap.Error(err))
	}

	if serveErr != nil {
		return serveErr
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	logger.Info("service stopped")
	return nil
}

// topologyComputer forwards all authentic roots from topology.Compute;
// multi-root traces yield one row per root. Orphans propagate even on the
// ErrNoRoot branch so the sweeper records them for all-orphan traces.
type topologyComputer struct{}

func (topologyComputer) Compute(traceID [16]byte, _ string, records map[string]buffer.SpanRecord) ([]topology.RootMetrics, int32, error) {
	spans := make(map[[8]byte]topology.Span, len(records))
	for _, r := range records {
		spans[r.SpanID] = topology.Span{
			SpanID:       r.SpanID,
			ParentSpanID: r.ParentSpanID,
			Service:      r.Service,
			Name:         r.Name,
			Kind:         r.Kind,
			StartTimeNs:  r.StartTimeNs,
			EndTimeNs:    r.EndTimeNs,
			StatusError:  r.StatusError,
			OpAttrs:      r.OpAttrs,
		}
	}
	rows, orphans := topology.Compute(traceID, spans)
	if len(rows) == 0 && len(spans) > 0 {
		return nil, orphans, sweep.ErrNoRoot
	}
	if len(rows) == 0 {
		return nil, orphans, nil
	}
	return rows, orphans, nil
}

type schemaChecker interface {
	CheckReady(ctx context.Context) error
}

func waitForSchemaReady(
	ctx context.Context,
	cfg config.Emitter,
	logger *zap.Logger,
	checker schemaChecker,
) error {
	maxAttempts := cfg.MaxRetries + 1
	attempt := 0

	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = cfg.InitialBackoff.Duration()
	eb.RandomizationFactor = 0
	eb.Multiplier = 2

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		attempt++
		logger.Info("attempt schema readiness check",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", maxAttempts),
			zap.String("endpoint", cfg.GreptimeDBSqlEndpoint),
			zap.String("database", cfg.GreptimeDBDatabase),
		)
		if err := checker.CheckReady(ctx); err != nil {
			return struct{}{}, err
		}
		logger.Info("schema ready",
			zap.Int("attempt", attempt),
			zap.String("endpoint", cfg.GreptimeDBSqlEndpoint),
			zap.String("database", cfg.GreptimeDBDatabase),
		)
		return struct{}{}, nil
	},
		backoff.WithBackOff(eb),
		backoff.WithMaxTries(uint(maxAttempts)),
		backoff.WithNotify(func(err error, next time.Duration) {
			logger.Warn("schema readiness check failed; retrying",
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAttempts),
				zap.Duration("backoff", next),
				zap.Error(err),
			)
		}),
	)
	return err
}

func buildAdminMux(registry *prometheus.Registry, ready *app.Readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz/live", liveHandler)
	mux.HandleFunc("/healthz/ready", readyHandler(ready))
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	return mux
}

type serverListeners struct {
	admin net.Listener
	grpc  net.Listener
	http  net.Listener
}

// openListeners binds the three TCP endpoints in order and closes any
// already-bound listener on a later failure so the caller does not have
// to track per-step cleanup.
func openListeners(ctx context.Context, cfg *config.Config) (serverListeners, error) {
	var lc net.ListenConfig
	admin, err := lc.Listen(ctx, "tcp", cfg.Service.MetricsEndpoint)
	if err != nil {
		return serverListeners{}, fmt.Errorf("listen on %s: %w", cfg.Service.MetricsEndpoint, err)
	}
	grpcL, err := lc.Listen(ctx, "tcp", cfg.Ingestion.OTLPGRPCEndpoint)
	if err != nil {
		_ = admin.Close()
		return serverListeners{}, fmt.Errorf("listen on %s: %w", cfg.Ingestion.OTLPGRPCEndpoint, err)
	}
	httpL, err := lc.Listen(ctx, "tcp", cfg.Ingestion.OTLPHTTPEndpoint)
	if err != nil {
		_ = admin.Close()
		_ = grpcL.Close()
		return serverListeners{}, fmt.Errorf("listen on %s: %w", cfg.Ingestion.OTLPHTTPEndpoint, err)
	}
	return serverListeners{admin: admin, grpc: grpcL, http: httpL}, nil
}

type serveResult struct {
	name string
	err  error
}

func serve(name string, errCh chan<- serveResult, serveFn func() error) {
	errCh <- serveResult{name: name, err: serveFn()}
}

type shutdowner interface {
	Shutdown(ctx context.Context) error
}

func shutdownServers(ctx context.Context, servers ...shutdowner) error {
	var errs []error
	for _, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown servers: %w", errors.Join(errs...))
	}
	return nil
}

func gracefulShutdown(ctx context.Context, errCh <-chan serveResult, n int, logger *zap.Logger, servers ...shutdowner) error {
	err := shutdownServers(ctx, servers...)
	waitForServers(ctx, errCh, n, logger)
	return err
}

func waitForSweeper(ctx context.Context, wg *sync.WaitGroup, logger *zap.Logger) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		logger.Warn("timed out waiting for sweeper shutdown", zap.Error(ctx.Err()))
	}
}

func waitForServers(ctx context.Context, errCh <-chan serveResult, n int, logger *zap.Logger) {
	for range n {
		select {
		case result := <-errCh:
			if result.err != nil && !errors.Is(result.err, http.ErrServerClosed) {
				logger.Warn("server returned during shutdown", zap.String("server", result.name), zap.Error(result.err))
			}
		case <-ctx.Done():
			logger.Warn("timed out waiting for server shutdown", zap.Error(ctx.Err()))
			return
		}
	}
}

// newAdminServer builds the admin HTTP server. BaseContext returns
// context.Background() so in-flight requests are cancelled only via
// server.Shutdown's grace deadline, not by the SIGTERM-cancelled parent ctx.
func newAdminServer(mux http.Handler) *http.Server {
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		BaseContext: func(net.Listener) context.Context {
			return context.Background()
		},
	}
}

func liveHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(liveResponse))
}

func readyHandler(r *app.Readiness) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !r.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(notReadyResponse))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(readyResponse))
	}
}
