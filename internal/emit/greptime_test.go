package emit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"google.golang.org/grpc/metadata"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type fakeSDKClient struct {
	errs   []error
	writes []sdkWriteCall
}

type sdkWriteCall struct {
	autoCreate string
	table      string
}

func (c *fakeSDKClient) Write(ctx context.Context, tables ...*table.Table) (*gpb.GreptimeResponse, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	autoCreate := ""
	if vals := md.Get("x-greptime-hint-auto_create_table"); len(vals) > 0 {
		autoCreate = vals[0]
	}

	name, _ := tables[0].GetName()
	c.writes = append(c.writes, sdkWriteCall{
		autoCreate: autoCreate,
		table:      name,
	})

	if len(c.errs) == 0 {
		return &gpb.GreptimeResponse{}, nil
	}
	err := c.errs[0]
	c.errs = c.errs[1:]
	if err != nil {
		return nil, err
	}
	return &gpb.GreptimeResponse{}, nil
}

func (c *fakeSDKClient) Close() error {
	return nil
}

func TestGreptimeWriterWarnsAndCreatesMissingTable(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.InfoLevel)
	client := &fakeSDKClient{
		errs: []error{
			errors.New("table trace_topology not found"),
			nil,
		},
	}
	writer := &greptimeWriter{
		logger:    zap.New(core),
		client:    client,
		tableName: "trace_topology",
	}

	err := writer.Write(context.Background(), makeWriteBatch("trace_topology", sampleRows(1), time.Unix(1, 0)))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(client.writes) != 2 {
		t.Fatalf("want 2 write attempts, got %d", len(client.writes))
	}
	if client.writes[0].autoCreate != "false" {
		t.Fatalf("first write auto_create_table = %q, want false", client.writes[0].autoCreate)
	}
	if client.writes[1].autoCreate != "true" {
		t.Fatalf("second write auto_create_table = %q, want true", client.writes[1].autoCreate)
	}
	if logs.Len() != 1 {
		t.Fatalf("want 1 info log, got %d", logs.Len())
	}
}

func TestGreptimeWriterReturnsErrorWhenTableCreationFails(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.InfoLevel)
	client := &fakeSDKClient{
		errs: []error{
			errors.New("table trace_topology not found"),
			errors.New("permission denied"),
		},
	}
	writer := &greptimeWriter{
		logger:    zap.New(core),
		client:    client,
		tableName: "trace_topology",
	}

	err := writer.Write(context.Background(), makeWriteBatch("trace_topology", sampleRows(1), time.Unix(1, 0)))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "create missing table") {
		t.Fatalf("unexpected error: %v", err)
	}
	if logs.Len() != 1 {
		t.Fatalf("want 1 info log, got %d", logs.Len())
	}
}

func TestBuildTableMapsSchema(t *testing.T) {
	t.Parallel()

	tbl, err := buildTable(makeWriteBatch("trace_topology", sampleRows(1), time.Unix(1, 2)))
	if err != nil {
		t.Fatalf("buildTable: %v", err)
	}

	req, err := tbl.ToInsertRequest()
	if err != nil {
		t.Fatalf("ToInsertRequest: %v", err)
	}

	if req.TableName != "trace_topology" {
		t.Fatalf("want table trace_topology, got %q", req.TableName)
	}
	if got := len(req.Rows.Schema); got != 12 {
		t.Fatalf("want 12 columns, got %d", got)
	}

	checkColumn(t, req.Rows.Schema[0], "root_id", gpb.SemanticType_TAG, gpb.ColumnDataType_STRING)
	checkColumn(t, req.Rows.Schema[1], "trace_id", gpb.SemanticType_TAG, gpb.ColumnDataType_STRING)
	checkColumn(t, req.Rows.Schema[2], "root_service", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING)
	checkColumn(t, req.Rows.Schema[3], "root_operation", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING)
	checkColumn(t, req.Rows.Schema[11], "timestamp", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_NANOSECOND)
}

func checkColumn(t *testing.T, col *gpb.ColumnSchema, name string, semantic gpb.SemanticType, typ gpb.ColumnDataType) {
	t.Helper()
	if col.ColumnName != name {
		t.Fatalf("want column %q, got %q", name, col.ColumnName)
	}
	if col.SemanticType != semantic {
		t.Fatalf("column %q semantic type = %v, want %v", name, col.SemanticType, semantic)
	}
	if col.Datatype != typ {
		t.Fatalf("column %q datatype = %v, want %v", name, col.Datatype, typ)
	}
}
