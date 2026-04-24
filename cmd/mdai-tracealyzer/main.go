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

	"github.com/mydecisive/mdai-tracealyzer/internal/app"
	"github.com/mydecisive/mdai-tracealyzer/internal/buffer"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/emit"
	"github.com/mydecisive/mdai-tracealyzer/internal/ingest"
	"github.com/mydecisive/mdai-tracealyzer/internal/observability"
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

var configPathFlag = flag.String("config", "", "path to tracealyzer YAML config")

func main() {
	flag.Parse()
	os.Exit(mainExit(*configPathFlag))
}

func mainExit(configPath string) int {
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

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("service failed", zap.Error(err))
		return 1
	}
	return 0
}

func run(ctx context.Context, cfg *config.Config, logger *zap.Logger) error {
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	ready := app.NewReadiness("otlp_grpc", "otlp_http", "buffer")
	registry := observability.NewRegistry()
	ingestMetrics := ingest.NewMetrics(registry)
	bufferMetrics := buffer.NewMetrics(registry, ingestMetrics.MalformedDrainCounter())
	sweepMetrics := sweep.NewMetrics(registry)

	valkeyBuffer, err := buffer.NewValkeyBuffer(buffer.ValkeyOptions{
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

	emitter, err := emit.New(cfg.Emitter, logger, registry)
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
	var probeWG sync.WaitGroup
	probeWG.Go(func() {
		if err := valkeyBuffer.WaitUntilReady(ctx); err != nil {
			return
		}
		ready.Mark("buffer")
		logger.Info("valkey reachable; buffer ready")
	})

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

	shutdownErr := gracefulShutdown(shutdownCtx, errCh, remaining, &probeWG, logger, adminServer, grpcServer, httpServer)

	// Wait for the sweeper's in-flight tick before closing the emitter, so a
	// late Emit does not land after the queue has begun draining.
	sweeperWG.Wait()
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

// topologyComputer returns rows[0] for the single-root v1 contract;
// additional roots, if any, are dropped. Orphans propagate even on the
// ErrNoRoot branch so the sweeper records them for all-orphan traces.
type topologyComputer struct{}

func (topologyComputer) Compute(traceID [16]byte, _ string, records map[string]buffer.SpanRecord) (topology.RootMetrics, int32, error) {
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
	if len(rows) == 0 {
		return topology.RootMetrics{}, orphans, sweep.ErrNoRoot
	}
	return rows[0], orphans, nil
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

func gracefulShutdown(ctx context.Context, errCh <-chan serveResult, n int, probeWG *sync.WaitGroup, logger *zap.Logger, servers ...shutdowner) error {
	err := shutdownServers(ctx, servers...)
	waitForServers(ctx, errCh, n, logger)
	probeWG.Wait()
	return err
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
