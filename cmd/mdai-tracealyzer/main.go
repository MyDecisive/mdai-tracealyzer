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
	"syscall"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/app"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	liveResponse      = "ok\n"
	readyResponse     = "ok\n"
	notReadyResponse  = "not ready\n"
	readHeaderTimeout = 5 * time.Second
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
		// Stderr is the only sink available pre-logger; nothing useful to do if the write fails.
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
	ready := app.NewReadiness("otlp_grpc", "otlp_http", "valkey")
	registry := observability.NewRegistry()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz/live", liveHandler)
	mux.HandleFunc("/healthz/ready", readyHandler(ready))
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", cfg.Service.MetricsEndpoint)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Service.MetricsEndpoint, err)
	}

	server := newAdminServer(mux)

	logger.Info("service starting",
		zap.String("metrics_endpoint", cfg.Service.MetricsEndpoint),
		zap.String("log_level", cfg.Service.LogLevel),
		zap.Duration("shutdown_grace", cfg.Service.ShutdownGrace.Duration()),
		zap.Strings("readiness_pending", ready.Pending()),
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), cfg.Service.ShutdownGrace.Duration(),
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown health server: %w", err)
		}

		logger.Info("service stopped")
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf("serve health endpoint: %w", err)
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
