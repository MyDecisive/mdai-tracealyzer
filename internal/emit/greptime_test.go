package emit

import (
	"context"
	"errors"
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/stretchr/testify/require"
)

type fakeSDKClient struct {
	errs        []error
	resps       []*gpb.GreptimeResponse
	writes      []sdkWriteCall
	healthErr   error
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
	if c.healthErr != nil {
		return nil, c.healthErr
	}
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
	require.NoError(t, err, "Write")

	require.Len(t, client.writes, 1)
	require.Equal(t, "trace_root_topology", client.writes[0].table, "write table")
}

func TestNewGreptimeClientReturnsFactoryClient(t *testing.T) {
	t.Parallel()

	client := &fakeSDKClient{}
	got, err := newGreptimeClient(testGreptimeConfig(), func(*greptime.Config) (sdkClient, error) {
		return client, nil
	})
	require.NoError(t, err, "newGreptimeClient")
	require.Same(t, client, got, "expected returned client to be the factory client")
	require.Zero(t, client.healthCalls, "startup health check should be gone")
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
	require.ErrorContains(t, err, "permission denied")
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
	require.ErrorContains(t, err, "status_code=1234")
}

func TestGreptimeWriterHealthCheckWrapsError(t *testing.T) {
	t.Parallel()

	client := &fakeSDKClient{
		healthErr: errors.New("connection refused"),
	}
	writer := &greptimeWriter{
		client: client,
	}

	err := writer.HealthCheck(context.Background())
	require.ErrorContains(t, err, "greptimedb health check", "error missing context")
	require.ErrorIs(t, err, client.healthErr, "wrapped error does not unwrap to the SDK error")
}

func TestBuildTableMapsSchema(t *testing.T) {
	t.Parallel()

	tbl, err := buildTable(makeWriteBatch(sampleRows(1), time.Unix(1, 2)))
	require.NoError(t, err, "buildTable")

	req, err := tbl.ToInsertRequest()
	require.NoError(t, err, "ToInsertRequest")

	require.Equal(t, "trace_root_topology", req.GetTableName())
	schema := req.GetRows().GetSchema()
	require.Len(t, schema, 13)

	checkColumn(t, schema[0], "root_id", gpb.SemanticType_TAG, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[1], "trace_id", gpb.SemanticType_TAG, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[2], "root_service", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[3], "root_operation", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING)
	checkColumn(t, schema[11], "span_bytes_total", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64)
	checkColumn(t, schema[12], "timestamp", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_NANOSECOND)
}

func checkColumn(t *testing.T, col *gpb.ColumnSchema, name string, semantic gpb.SemanticType, typ gpb.ColumnDataType) {
	t.Helper()
	require.Equal(t, name, col.GetColumnName())
	require.Equal(t, semantic, col.GetSemanticType(), "column %q semantic type", name)
	require.Equal(t, typ, col.GetDatatype(), "column %q datatype", name)
}

func testGreptimeConfig() config.Emitter {
	cfg := testEmitterConfig()
	cfg.GreptimeDBEndpoint = "127.0.0.1:4001"
	cfg.GreptimeDBDatabase = "mdai"
	cfg.GreptimeDBAuth = "mdai:secret"
	return cfg
}
