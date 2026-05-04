package emit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
)

type fakeSDKClient struct {
	errs        []error
	resps       []*gpb.GreptimeResponse
	writes      []sdkWriteCall
	healthCalls int
	closed      bool
}

type sdkWriteCall struct {
	table string
}

func (c *fakeSDKClient) Write(ctx context.Context, tables ...*table.Table) (*gpb.GreptimeResponse, error) {
	name, _ := tables[0].GetName()
	c.writes = append(c.writes, sdkWriteCall{
		table: name,
	})

	if len(c.errs) == 0 {
		return c.nextResponse(), nil
	}
	err := c.errs[0]
	c.errs = c.errs[1:]
	if err != nil {
		return nil, err
	}
	return c.nextResponse(), nil
}

func (c *fakeSDKClient) HealthCheck(_ context.Context) (*gpb.HealthCheckResponse, error) {
	c.healthCalls++
	return &gpb.HealthCheckResponse{}, nil
}

//nolint:revive // Keep the receiver name consistent with other fakeSDKClient methods.
func (c *fakeSDKClient) Close() error {
	c.closed = true
	return nil
}

func (c *fakeSDKClient) nextResponse() *gpb.GreptimeResponse {
	if len(c.resps) == 0 {
		return &gpb.GreptimeResponse{}
	}
	resp := c.resps[0]
	c.resps = c.resps[1:]
	return resp
}

func TestGreptimeWriterWritesConfiguredTable(t *testing.T) {
	t.Parallel()

	client := &fakeSDKClient{}
	writer := &greptimeWriter{
		client: client,
	}

	err := writer.Write(context.Background(), makeWriteBatch(sampleRows(1), time.Unix(1, 0)))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(client.writes) != 1 {
		t.Fatalf("want 1 write attempt, got %d", len(client.writes))
	}
	if client.writes[0].table != "trace_root_topology" {
		t.Fatalf("write table = %q, want trace_root_topology", client.writes[0].table)
	}
}

func TestNewGreptimeClientReturnsFactoryClient(t *testing.T) {
	t.Parallel()

	client := &fakeSDKClient{}
	got, err := newGreptimeClientWithFactory(testGreptimeConfig(), func(*greptime.Config) (sdkClient, error) {
		return client, nil
	})
	if err != nil {
		t.Fatalf("newGreptimeClientWithFactory: %v", err)
	}
	if got != client {
		t.Fatal("expected returned client to be the factory client")
	}
	if client.healthCalls != 0 {
		t.Fatalf("startup health check should be gone; got %d calls", client.healthCalls)
	}
}

func TestGreptimeWriterReturnsTransportError(t *testing.T) {
	t.Parallel()

	client := &fakeSDKClient{
		errs: []error{errors.New("permission denied")},
	}
	writer := &greptimeWriter{
		client: client,
	}

	err := writer.Write(context.Background(), makeWriteBatch(sampleRows(1), time.Unix(1, 0)))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGreptimeWriterReturnsResponseStatusError(t *testing.T) {
	t.Parallel()

	client := &fakeSDKClient{
		resps: []*gpb.GreptimeResponse{
			{
				Header: &gpb.ResponseHeader{
					Status: &gpb.Status{
						StatusCode: 1234,
						ErrMsg:     "bad schema",
					},
				},
			},
		},
	}
	writer := &greptimeWriter{
		client: client,
	}

	err := writer.Write(context.Background(), makeWriteBatch(sampleRows(1), time.Unix(1, 0)))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "status_code=1234") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildTableMapsSchema(t *testing.T) {
	t.Parallel()

	tbl, err := buildTable(makeWriteBatch(sampleRows(1), time.Unix(1, 2)))
	if err != nil {
		t.Fatalf("buildTable: %v", err)
	}

	req, err := tbl.ToInsertRequest()
	if err != nil {
		t.Fatalf("ToInsertRequest: %v", err)
	}

	if req.GetTableName() != "trace_root_topology" {
		t.Fatalf("want table trace_root_topology, got %q", req.GetTableName())
	}
	schema := req.GetRows().GetSchema()
	if got := len(schema); got != 12 {
		t.Fatalf("want 12 columns, got %d", got)
	}

	checkColumn(t, schema[0], "root_id", gpb.SemanticType_TAG, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[1], "trace_id", gpb.SemanticType_TAG, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[2], "root_service", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[3], "root_operation", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[11], "timestamp", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_NANOSECOND)
}

func checkColumn(t *testing.T, col *gpb.ColumnSchema, name string, semantic gpb.SemanticType, typ gpb.ColumnDataType) {
	t.Helper()
	if col.GetColumnName() != name {
		t.Fatalf("want column %q, got %q", name, col.GetColumnName())
	}
	if col.GetSemanticType() != semantic {
		t.Fatalf("column %q semantic type = %v, want %v", name, col.GetSemanticType(), semantic)
	}
	if col.GetDatatype() != typ {
		t.Fatalf("column %q datatype = %v, want %v", name, col.GetDatatype(), typ)
	}
}

func testGreptimeConfig() config.Emitter {
	cfg := testEmitterConfig()
	cfg.GreptimeDBEndpoint = "127.0.0.1:4001"
	cfg.GreptimeDBDatabase = "mdai"
	cfg.GreptimeDBAuth = "mdai:secret"
	return cfg
}
