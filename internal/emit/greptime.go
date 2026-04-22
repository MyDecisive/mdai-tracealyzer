package emit

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	gtcontext "github.com/GreptimeTeam/greptimedb-ingester-go/context"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"go.uber.org/zap"
)

type sdkClient interface {
	Write(ctx context.Context, tables ...*table.Table) (*gpb.GreptimeResponse, error)
	Close() error
}

type greptimeWriter struct {
	client sdkClient
}

//nolint:ireturn // The writer interface is the package's test seam around the GreptimeDB SDK.
func newGreptimeWriter(cfg config.Emitter, _ *zap.Logger) (writer, error) {
	client, err := newGreptimeClient(cfg)
	if err != nil {
		return nil, err
	}
	return &greptimeWriter{client: client}, nil
}

func newGreptimeClient(cfg config.Emitter) (*greptime.Client, error) {
	host, port, err := splitEndpoint(cfg.GreptimeDBEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse greptimedb endpoint: %w", err)
	}

	username, password := parseAuth(cfg.GreptimeDBAuth)
	clientCfg := greptime.NewConfig(host).
		WithPort(port).
		WithDatabase(cfg.GreptimeDBDatabase).
		WithAuth(username, password)

	client, err := greptime.NewClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("create greptimedb client: %w", err)
	}
	return client, nil
}

func (w *greptimeWriter) Write(ctx context.Context, batch writeBatch) error {
	tbl, err := buildTable(batch)
	if err != nil {
		return err
	}

	resp, err := w.client.Write(withAutoCreateHint(ctx, true), tbl)
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

func withAutoCreateHint(ctx context.Context, enabled bool) context.Context {
	return gtcontext.New(ctx, gtcontext.WithHint([]*gtcontext.Hint{
		{
			Key:   "auto_create_table",
			Value: strconv.FormatBool(enabled),
		},
	}))
}

func splitEndpoint(endpoint string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("port %q: %w", portStr, err)
	}
	return host, port, nil
}

func parseAuth(raw string) (username string, password string) {
	if raw == "" {
		return "", ""
	}
	if user, pass, ok := strings.Cut(raw, ":"); ok {
		return user, pass
	}
	return "", raw
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
