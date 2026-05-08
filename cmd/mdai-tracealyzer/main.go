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
	"github.com/mydecisive/mdai-tracealyzer/internal/run"
	"github.com/mydecisive/mdai-tracealyzer/internal/schema"
	"github.com/mydecisive/mdai-tracealyzer/internal/sweep"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	liveResponse      = "ok\n"
	readyResponse     = "ok\n"
	notReadyResponse  = "not ready\n"
	readHeaderTimeout = 5 * time.Second
	probeMaxBackoff   = 30 * time.Second
	probeMultiplier   = 2.0
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

	if err := serve(ctx, cfg, logger); err != nil {
		logger.Error("service failed", zap.Error(err))
		return 1
	}
	return 0
}

func serve(ctx context.Context, cfg *config.Config, logger *zap.Logger) error {
	ready := app.NewReadiness("schema", "emitter")
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

	emitter, err := emit.New(cfg.Emitter, logger, registry)
	if err != nil {
		return fmt.Errorf("emitter: %w", err)
	}

	schemaManager := schema.New(cfg.Emitter, logger)

	sweeper, err := sweep.New(valkeyBuffer, sweep.TopologyComputer{}, emitter, sweep.Config{
		QuietPeriod:    cfg.Buffer.QuietPeriod.Duration(),
		MaxTTL:         cfg.Buffer.MaxTTL.Duration(),
		Interval:       cfg.Buffer.SweepInterval.Duration(),
		WorkerPoolSize: cfg.Buffer.SweepWorkerPoolSize,
	}, ready.WaitChan(), sweepMetrics, logger)
	if err != nil {
		return fmt.Errorf("sweeper: %w", err)
	}

	adminListener, err := openAdminListener(ctx, cfg)
	if err != nil {
		return err
	}
	admin := &adminComponent{
		server:   newAdminServer(buildAdminMux(registry, ready)),
		listener: adminListener,
	}

	grpcServer := ingest.NewGRPCServer(valkeyBuffer, cfg.Ingestion.OTLPGRPCEndpoint, ingestMetrics, logger)
	httpServer := ingest.NewHTTPServer(valkeyBuffer, cfg.Ingestion.OTLPHTTPEndpoint, ingestMetrics, logger)

	probeBackoff := run.Backoff{
		Initial:    cfg.Emitter.InitialBackoff.Duration(),
		Max:        probeMaxBackoff,
		Multiplier: probeMultiplier,
	}
	schemaProbe := run.NewProbe("schema", schemaManager.CheckReady,
		func() { ready.Mark("schema") }, probeBackoff, logger)
	// The schema probe only verifies the SQL endpoint; an independent probe
	// against the gRPC write endpoint guards readiness from a misconfigured
	// GREPTIMEDB_ENDPOINT silently dropping every emit.
	emitterProbe := run.NewProbe("emitter", emitter.HealthCheck,
		func() { ready.Mark("emitter") }, probeBackoff, logger)

	logger.Info("service starting",
		zap.String("metrics_endpoint", cfg.Service.MetricsEndpoint),
		zap.String("otlp_grpc_endpoint", cfg.Ingestion.OTLPGRPCEndpoint),
		zap.String("otlp_http_endpoint", cfg.Ingestion.OTLPHTTPEndpoint),
		zap.String("valkey_addr", cfg.Buffer.ValkeyAddr),
		zap.String("log_level", cfg.Service.LogLevel),
		zap.Duration("shutdown_grace", cfg.Service.ShutdownGrace.Duration()),
		zap.Strings("readiness_pending", ready.Pending()),
	)

	// Registration order encodes dependency order: consumers/sinks first,
	// producers last. Reverse Shutdown therefore drains the data path
	// downstream-first (sweeper → ingest → emitter). The sweeper's gate on
	// ready.WaitChan() guards destructive Drain until the schema and emitter
	// probes have both reported healthy.
	sup := run.New(cfg.Service.ShutdownGrace.Duration(), logger,
		admin,
		schemaProbe,
		emitter,
		emitterProbe,
		grpcServer,
		httpServer,
		sweeper,
	)
	sup.OnShutdown(ready.MarkShuttingDown)
	return sup.Run(ctx)
}

// adminComponent wraps the admin HTTP server with a pre-bound listener
// so /healthz/live is reachable as soon as the supervisor enters Run.
type adminComponent struct {
	server   *http.Server
	listener net.Listener

	started   bool
	serveDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

var _ run.Component = (*adminComponent)(nil)

func (*adminComponent) Name() string { return "admin" }

func (a *adminComponent) Start(_ context.Context, host run.Host) error {
	a.serveDone = make(chan struct{})
	a.started = true
	go func() {
		defer close(a.serveDone)
		err := a.server.Serve(a.listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			host.Fatal("admin", err)
		}
	}()
	return nil
}

func (a *adminComponent) Shutdown(ctx context.Context) error {
	a.closeOnce.Do(func() {
		if !a.started {
			a.closeErr = a.listener.Close()
			return
		}
		a.closeErr = a.server.Shutdown(ctx)
		<-a.serveDone
	})
	return a.closeErr
}

func buildAdminMux(registry *prometheus.Registry, ready *app.Readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz/live", liveHandler)
	mux.HandleFunc("/healthz/ready", readyHandler(ready))
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	return mux
}

// openAdminListener binds the admin endpoint up front so the readiness
// and metrics handlers are reachable while the OTLP servers complete
// their own bind-and-serve in Start.
func openAdminListener(ctx context.Context, cfg *config.Config) (net.Listener, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Service.MetricsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", cfg.Service.MetricsEndpoint, err)
	}
	return ln, nil
}

// BaseContext returns context.Background() so in-flight requests are
// cancelled only via server.Shutdown's grace deadline, not by the
// SIGTERM-cancelled parent ctx.
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
