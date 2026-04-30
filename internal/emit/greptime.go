package emit

import (
	"context"
	"fmt"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/mydecisive/mdai-tracealyzer/internal/greptimecfg"
	"go.uber.org/zap"
)

const (
	startupHealthCheckAttempts = 3
	startupHealthCheckInterval = 5 * time.Second
)

type sdkClient interface {
	Write(ctx context.Context, tables ...*table.Table) (*gpb.GreptimeResponse, error)
	HealthCheck(ctx context.Context) (*gpb.HealthCheckResponse, error)
	Close() error
}

type greptimeWriter struct {
	client sdkClient
}

//nolint:ireturn // The writer interface is the package's test seam around the GreptimeDB SDK.
func newGreptimeWriter(cfg config.Emitter, logger *zap.Logger) (writer, error) {
	client, err := newGreptimeClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &greptimeWriter{client: client}, nil
}

//nolint:ireturn // The GreptimeDB client is consumed through the sdkClient test seam.
func newGreptimeClient(cfg config.Emitter, logger *zap.Logger) (sdkClient, error) {
	return newGreptimeClientWithFactoryAndBackoff(cfg, func(clientCfg *greptime.Config) (sdkClient, error) {
		return greptime.NewClient(clientCfg)
	}, backoff.NewConstantBackOff(startupHealthCheckInterval), logger)
}

//nolint:ireturn // The GreptimeDB client is consumed through the sdkClient test seam.
func newGreptimeClientWithFactoryAndBackoff(
	cfg config.Emitter,
	factory func(*greptime.Config) (sdkClient, error),
	bo backoff.BackOff,
	logger *zap.Logger,
) (sdkClient, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	host, port, err := greptimecfg.SplitEndpoint(cfg.GreptimeDBEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse greptimedb endpoint: %w", err)
	}

	username, password := greptimecfg.ParseAuth(cfg.GreptimeDBAuth)
	clientCfg := greptime.NewConfig(host).
		WithPort(port).
		WithDatabase(cfg.GreptimeDBDatabase).
		WithAuth(username, password)

	client, err := factory(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("create greptimedb client: %w", err)
	}

	if err := checkGreptimeHealth(client, cfg, bo, logger); err != nil {
		_ = client.Close()
		return nil, err
	}

	return client, nil
}

func checkGreptimeHealth(
	client sdkClient,
	cfg config.Emitter,
	bo backoff.BackOff,
	logger *zap.Logger,
) error {
	attempt := 0

	_, err := backoff.Retry(context.Background(), func() (struct{}, error) {
		attempt++
		logger.Info("attempt GreptimeDB connection",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", startupHealthCheckAttempts),
			zap.String("endpoint", cfg.GreptimeDBEndpoint),
			zap.String("database", cfg.GreptimeDBDatabase),
		)
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout.Duration())
		defer cancel()
		if _, err := client.HealthCheck(ctx); err != nil {
			return struct{}{}, err
		}
		logger.Info("connected to GreptimeDB",
			zap.Int("attempt", attempt),
			zap.String("endpoint", cfg.GreptimeDBEndpoint),
			zap.String("database", cfg.GreptimeDBDatabase),
		)
		return struct{}{}, nil
	},
		backoff.WithBackOff(bo),
		backoff.WithMaxTries(uint(startupHealthCheckAttempts)),
	)
	if err != nil {
		return fmt.Errorf(
			"health check greptimedb after %d attempts: %w",
			startupHealthCheckAttempts, err,
		)
	}
	return nil
}

func (w *greptimeWriter) Write(ctx context.Context, batch writeBatch) error {
	tbl, err := buildTable(batch)
	if err != nil {
		return err
	}

	resp, err := w.client.Write(ctx, tbl)
	if err != nil {
		return err
	}
	return responseError(resp)
}

func (w *greptimeWriter) Close() error {
	return w.client.Close()
}

func buildTable(batch writeBatch) (*table.Table, error) {
	tbl, err := table.New(batch.Table)
	if err != nil {
		return nil, fmt.Errorf("create table %q: %w", batch.Table, err)
	}

	for _, col := range tagColumns() {
		if err := tbl.AddTagColumn(col.name, col.typ); err != nil {
			return nil, err
		}
	}
	for _, col := range fieldColumns() {
		if err := tbl.AddFieldColumn(col.name, col.typ); err != nil {
			return nil, err
		}
	}
	for _, col := range timestampColumns() {
		if err := tbl.AddTimestampColumn(col.name, col.typ); err != nil {
			return nil, err
		}
	}

	for _, r := range batch.Rows {
		if err := tbl.AddRow(
			r.RootID,
			r.TraceID,
			r.RootService,
			r.RootOperation,
			r.Breadth,
			r.ServiceHopDepth,
			r.ServiceCount,
			r.OperationCount,
			r.SpanCount,
			r.ErrorCount,
			r.RootDurationNS,
			r.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("add row for trace_id %q: %w", r.TraceID, err)
		}
	}

	return tbl, nil
}

type columnDef struct {
	name string
	typ  types.ColumnType
}

func tagColumns() []columnDef {
	return []columnDef{
		{name: "root_id", typ: types.STRING},
		{name: "trace_id", typ: types.STRING},
	}
}

func fieldColumns() []columnDef {
	return []columnDef{
		{name: "root_service", typ: types.STRING},
		{name: "root_operation", typ: types.STRING},
		{name: "breadth", typ: types.INT32},
		{name: "service_hop_depth", typ: types.INT32},
		{name: "service_count", typ: types.INT32},
		{name: "operation_count", typ: types.INT32},
		{name: "span_count", typ: types.INT32},
		{name: "error_count", typ: types.INT32},
		{name: "root_duration_ns", typ: types.INT64},
	}
}

func timestampColumns() []columnDef {
	return []columnDef{
		{name: "timestamp", typ: types.TIMESTAMP_NANOSECOND},
	}
}

func responseError(resp *gpb.GreptimeResponse) error {
	status := resp.GetHeader().GetStatus()
	if status.GetStatusCode() == 0 {
		return nil
	}
	return fmt.Errorf("greptimedb write failed: status_code=%d err_msg=%q",
		status.GetStatusCode(),
		status.GetErrMsg(),
	)
}
