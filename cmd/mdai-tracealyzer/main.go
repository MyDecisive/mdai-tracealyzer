package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
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
		Addr:             cfg.Buffer.ValkeyAddr,
		DB:               cfg.Buffer.ValkeyDB,
		Password:         cfg.Buffer.ValkeyPassword,
		MaxTTL:           cfg.Buffer.MaxTTL.Duration(),
		OperationTimeout: cfg.Buffer.ValkeyOperationTimeout.Duration(),
		Metrics:          bufferMetrics,
		Logger:           logger,
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

	admin, err := newListenerComponent(ctx, "admin", cfg.Service.MetricsEndpoint, buildAdminMux(registry, ready))
	if err != nil {
		return err
	}
	listeners := []run.Component{admin}
	if cfg.Service.PprofEndpoint != "" {
		pprofComponent, err := newListenerComponent(ctx, "pprof", cfg.Service.PprofEndpoint, buildPprofMux())
		if err != nil {
			return errors.Join(err, admin.Shutdown(ctx))
		}
		listeners = append(listeners, pprofComponent)
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
		zap.String("pprof_endpoint", cfg.Service.PprofEndpoint),
		zap.String("otlp_grpc_endpoint", cfg.Ingestion.OTLPGRPCEndpoint),
		zap.String("otlp_http_endpoint", cfg.Ingestion.OTLPHTTPEndpoint),
		zap.String("valkey_addr", cfg.Buffer.ValkeyAddr),
		zap.String("log_level", cfg.Service.LogLevel),
		zap.Duration("shutdown_grace", cfg.Service.ShutdownGrace.Duration()),
		zap.Strings("readiness_pending", ready.Pending()),
	)

	// Registration order is dependency order: sinks first, sources last.
	// Reverse Shutdown closes ingest before the sweeper waits on in-flight
	// Drain, so a degraded Valkey can't keep HTTP/gRPC accepting traffic
	// past the shutdown grace.
	sup := run.New(cfg.Service.ShutdownGrace.Duration(), logger, append(listeners,
		schemaProbe,
		emitter,
		emitterProbe,
		sweeper,
		grpcServer,
		httpServer,
	)...)
	sup.OnShutdown(ready.MarkShuttingDown)
	return sup.Run(ctx)
}

// listenerComponent wraps an HTTP server with a pre-bound listener so its
// handlers are reachable as soon as the supervisor enters Run, before the
// OTLP servers complete their own bind-and-serve in Start.
type listenerComponent struct {
	name     string
	server   *http.Server
	listener net.Listener

	started   bool
	serveDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

var _ run.Component = (*listenerComponent)(nil)

func newListenerComponent(ctx context.Context, name, addr string, mux http.Handler) (*listenerComponent, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}
	return &listenerComponent{name: name, server: newAdminServer(mux), listener: ln}, nil
}

func (l *listenerComponent) Name() string { return l.name }

func (l *listenerComponent) Start(_ context.Context, host run.Host) error {
	l.serveDone = make(chan struct{})
	l.started = true
	go func() {
		defer close(l.serveDone)
		err := l.server.Serve(l.listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			host.Fatal(l.name, err)
		}
	}()
	return nil
}

func (l *listenerComponent) Shutdown(ctx context.Context) error {
	l.closeOnce.Do(func() {
		if !l.started {
			l.closeErr = l.listener.Close()
			return
		}
		l.closeErr = l.server.Shutdown(ctx)
		<-l.serveDone
	})
	return l.closeErr
}

func buildAdminMux(registry *prometheus.Registry, ready *app.Readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz/live", liveHandler)
	mux.HandleFunc("/healthz/ready", readyHandler(ready))
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	return mux
}

func buildPprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
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
