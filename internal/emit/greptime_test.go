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
)

type fakeSDKClient struct {
	errs   []error
	resps  []*gpb.GreptimeResponse
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
		return c.nextResponse(), nil
	}
	err := c.errs[0]
	c.errs = c.errs[1:]
	if err != nil {
		return nil, err
	}
	return c.nextResponse(), nil
}

func (_ *fakeSDKClient) Close() error {
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

func TestGreptimeWriterAlwaysUsesAutoCreateTable(t *testing.T) {
	t.Parallel()

	client := &fakeSDKClient{}
	writer := &greptimeWriter{
		client: client,
	}

	err := writer.Write(context.Background(), makeWriteBatch("trace_root_topology", sampleRows(1), time.Unix(1, 0)))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(client.writes) != 1 {
		t.Fatalf("want 1 write attempt, got %d", len(client.writes))
	}
	if client.writes[0].autoCreate != "true" {
		t.Fatalf("write auto_create_table = %q, want true", client.writes[0].autoCreate)
	}
	if client.writes[0].table != "trace_root_topology" {
		t.Fatalf("write table = %q, want trace_root_topology", client.writes[0].table)
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

	err := writer.Write(context.Background(), makeWriteBatch("trace_root_topology", sampleRows(1), time.Unix(1, 0)))
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

	err := writer.Write(context.Background(), makeWriteBatch("trace_root_topology", sampleRows(1), time.Unix(1, 0)))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "status_code=1234") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildTableMapsSchema(t *testing.T) {
	t.Parallel()

	tbl, err := buildTable(makeWriteBatch("trace_root_topology", sampleRows(1), time.Unix(1, 2)))
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
