package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
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
var emitSampleFileFlag = flag.String("emit-sample-file", "", "temporary: read RootMetrics JSON from file and emit it to GreptimeDB")
var emitTemplateFileFlag = flag.String("emit-template-file", "", "temporary: read one RootMetrics JSON template and emit generated rows forever")
var emitIntervalFlag = flag.Duration("emit-interval", time.Second, "temporary: sleep interval for --emit-template-file")

func main() {
	flag.Parse()
	os.Exit(mainExit(*configPathFlag, *emitSampleFileFlag, *emitTemplateFileFlag, *emitIntervalFlag))
}

func mainExit(configPath, emitSampleFile, emitTemplateFile string, emitInterval time.Duration) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if emitSampleFile != "" {
		return emitSampleMain(ctx, emitSampleFile)
	}
	if emitTemplateFile != "" {
		return emitGeneratorMain(ctx, emitTemplateFile, emitInterval)
	}

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

type rootChoice struct {
	rootID        string
	rootService   string
	rootOperation string
}

var testRootChoices = []rootChoice{
	{
		rootID:        "gateway-api::GET /checkout/:id",
		rootService:   "gateway-api",
		rootOperation: "GET /checkout/:id",
	},
	{
		rootID:        "checkout-api::POST /checkout",
		rootService:   "checkout-api",
		rootOperation: "POST /checkout",
	},
	{
		rootID:        "catalog-api::GET /catalog/:sku",
		rootService:   "catalog-api",
		rootOperation: "GET /catalog/:sku",
	},
	{
		rootID:        "payments-api::POST /payments/authorize",
		rootService:   "payments-api",
		rootOperation: "POST /payments/authorize",
	},
	{
		rootID:        "inventory-grpc-service::Inventory/ReserveItems",
		rootService:   "inventory-grpc-service",
		rootOperation: "Inventory/ReserveItems",
	},
}

func emitGeneratorMain(ctx context.Context, templatePath string, interval time.Duration) int {
	if interval <= 0 {
		_, _ = fmt.Fprintln(os.Stderr, "emit-interval must be > 0")
		return 1
	}

	logger, err := observability.NewLogger("debug")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, fmt.Errorf("init logger: %w", err))
		return 1
	}
	defer func() { _ = logger.Sync() }()

	template, err := loadSampleTemplate(templatePath)
	if err != nil {
		logger.Error("load sample template", zap.Error(err))
		return 1
	}

	cfg := temporaryEmitterConfig()
	emitter, err := emit.New(cfg, logger, observability.NewRegistry())
	if err != nil {
		logger.Error("create emitter", zap.Error(err))
		return 1
	}

	logger.Info("sample generator started",
		zap.String("template_path", templatePath),
		zap.Duration("interval", interval),
		zap.String("greptimedb_endpoint", cfg.GreptimeDBEndpoint),
		zap.String("greptimedb_database", cfg.GreptimeDBDatabase),
		zap.String("table_name", cfg.TableName),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var emitted int
	for {
		row, err := generateSampleRow(template)
		if err != nil {
			logger.Error("generate sample row", zap.Error(err))
			return closeEmitterForSample(emitter, logger)
		}

		if err := emitter.Emit(ctx, []emit.RootMetrics{row}); err != nil {
			logger.Error("emit generated sample row", zap.Error(err))
			return closeEmitterForSample(emitter, logger)
		}
		emitted++

		logger.Info("generated sample row accepted",
			zap.Int("emitted", emitted),
			zap.String("trace_id", row.TraceID),
			zap.String("root_id", row.RootID),
		)

		select {
		case <-ctx.Done():
			logger.Info("sample generator stopping", zap.Int("emitted", emitted))
			return closeEmitterForSample(emitter, logger)
		case <-ticker.C:
		}
	}
}

func emitSampleMain(ctx context.Context, samplePath string) int {
	logger, err := observability.NewLogger("debug")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, fmt.Errorf("init logger: %w", err))
		return 1
	}
	defer func() { _ = logger.Sync() }()

	rows, err := loadSampleRows(samplePath)
	if err != nil {
		logger.Error("load sample rows", zap.Error(err))
		return 1
	}

	cfg := temporaryEmitterConfig()

	emitter, err := emit.New(cfg, logger, observability.NewRegistry())
	if err != nil {
		logger.Error("create emitter", zap.Error(err))
		return 1
	}

	if err := emitter.Emit(ctx, rows); err != nil {
		logger.Error("emit sample rows", zap.Error(err))
		_ = emitter.Close(context.Background())
		return 1
	}

	if code := closeEmitterForSample(emitter, logger); code != 0 {
		return code
	}

	logger.Info("sample rows emitted",
		zap.String("sample_path", samplePath),
		zap.Int("row_count", len(rows)),
		zap.String("greptimedb_endpoint", cfg.GreptimeDBEndpoint),
		zap.String("greptimedb_database", cfg.GreptimeDBDatabase),
		zap.String("table_name", cfg.TableName),
	)
	return 0
}

func temporaryEmitterConfig() config.Emitter {
	return config.Emitter{
		GreptimeDBEndpoint: "127.0.0.1:4001",
		GreptimeDBDatabase: "mdai",
		GreptimeDBAuth:     "mdai:R2vELRu8AG0X1cOfKSzOTu1LZc8gDttc",
		TableName:          "trace_topology",
		Timeout:            config.Duration(10 * time.Second),
		MaxRetries:         3,
		InitialBackoff:     config.Duration(time.Second),
		BatchSize:          100,
		FlushInterval:      config.Duration(10000 * time.Millisecond),
		QueueCapacity:      1024,
	}
}

func closeEmitterForSample(emitter emit.Emitter, logger *zap.Logger) int {
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := emitter.Close(closeCtx); err != nil {
		logger.Error("close emitter", zap.Error(err))
		return 1
	}
	return 0
}

func loadSampleRows(path string) ([]emit.RootMetrics, error) {
	// #nosec G304 -- path is an explicit operator-provided CLI argument for local testing.
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sample file %q: %w", path, err)
	}

	var rows []emit.RootMetrics
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("parse sample file %q: %w", path, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("sample file %q contains no rows", path)
	}
	return rows, nil
}

func loadSampleTemplate(path string) (emit.RootMetrics, error) {
	// #nosec G304 -- path is an explicit operator-provided CLI argument for local testing.
	data, err := os.ReadFile(path)
	if err != nil {
		return emit.RootMetrics{}, fmt.Errorf("read sample template %q: %w", path, err)
	}

	var row emit.RootMetrics
	if err := json.Unmarshal(data, &row); err != nil {
		return emit.RootMetrics{}, fmt.Errorf("parse sample template %q: %w", path, err)
	}
	return row, nil
}

func generateSampleRow(template emit.RootMetrics) (emit.RootMetrics, error) {
	choice, err := randomRootChoice()
	if err != nil {
		return emit.RootMetrics{}, err
	}
	traceID, err := randomTraceID()
	if err != nil {
		return emit.RootMetrics{}, err
	}

	row := template
	row.TraceID = traceID
	row.RootID = choice.rootID
	row.RootService = choice.rootService
	row.RootOperation = choice.rootOperation
	return row, nil
}

func randomRootChoice() (rootChoice, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(testRootChoices))))
	if err != nil {
		return rootChoice{}, fmt.Errorf("choose root_id: %w", err)
	}
	return testRootChoices[n.Int64()], nil
}

func randomTraceID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate trace_id: %w", err)
	}
	return fmt.Sprintf("%032x", id), nil
}

func run(ctx context.Context, cfg *config.Config, logger *zap.Logger) error {
	ready := app.NewReadiness("otlp_grpc", "otlp_http", "buffer")
	registry := observability.NewRegistry()
	ingestMetrics := ingest.NewMetrics(registry)
	bufferMetrics := buffer.NewMetrics(registry, ingestMetrics.MalformedDrainCounter())

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

	shutdownCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), cfg.Service.ShutdownGrace.Duration(),
	)
	defer cancel()

	shutdownErr := gracefulShutdown(shutdownCtx, errCh, remaining, &probeWG, logger, adminServer, grpcServer, httpServer)
	if serveErr != nil {
		return serveErr
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	logger.Info("service stopped")
	return nil
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
