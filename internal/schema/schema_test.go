package schema

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"go.uber.org/zap"
)

type fakeSQLClient struct {
	healthErr  error
	handleErrs []error
	requests   []string
	closed     bool
}

func (c *fakeSQLClient) HealthCheck(context.Context) (*gpb.HealthCheckResponse, error) {
	if c.healthErr != nil {
		return nil, c.healthErr
	}
	return &gpb.HealthCheckResponse{}, nil
}

func (c *fakeSQLClient) Handle(_ context.Context, req *gpb.GreptimeRequest) (*gpb.GreptimeResponse, error) {
	c.requests = append(c.requests, req.GetQuery().GetSql())
	if len(c.handleErrs) > 0 {
		err := c.handleErrs[0]
		c.handleErrs = c.handleErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &gpb.GreptimeResponse{
		Header: &gpb.ResponseHeader{
			Status: &gpb.Status{},
		},
	}, nil
}

func (c *fakeSQLClient) Close() error {
	c.closed = true
	return nil
}

func TestManagerMigrateAppliesStatementsInOrder(t *testing.T) {
	t.Parallel()

	client := &fakeSQLClient{}
	m := testManager(client)

	if err := m.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	want := []string{createSourceTableSQL, createSinkTableSQL, createFlowSQL}
	if len(client.requests) != len(want) {
		t.Fatalf("want %d requests, got %d", len(want), len(client.requests))
	}
	for i := range want {
		if client.requests[i] != want[i] {
			t.Fatalf("request %d mismatch\nwant: %q\n got: %q", i, want[i], client.requests[i])
		}
	}
	if !client.closed {
		t.Fatal("expected client close")
	}
}

func TestManagerCheckReadyChecksAllObjects(t *testing.T) {
	t.Parallel()

	client := &fakeSQLClient{}
	m := testManager(client)

	if err := m.CheckReady(context.Background()); err != nil {
		t.Fatalf("CheckReady: %v", err)
	}
	want := []string{
		"DESC TABLE trace_root_topology",
		"DESC TABLE trace_root_topology_1m",
		"DESC FLOW trace_root_topology_1m_flow",
	}
	if len(client.requests) != len(want) {
		t.Fatalf("want %d requests, got %d", len(want), len(client.requests))
	}
	for i := range want {
		if client.requests[i] != want[i] {
			t.Fatalf("request %d mismatch\nwant: %q\n got: %q", i, want[i], client.requests[i])
		}
	}
}

func TestManagerConnectFailsOnHealthCheck(t *testing.T) {
	t.Parallel()

	client := &fakeSQLClient{healthErr: errors.New("unreachable")}
	m := testManager(client)

	err := m.CheckReady(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "health check greptimedb") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !client.closed {
		t.Fatal("expected client close on health check error")
	}
}

func TestManagerMigrateFailsOnStatementError(t *testing.T) {
	t.Parallel()

	client := &fakeSQLClient{
		handleErrs: []error{nil, errors.New("ddl failed")},
	}
	m := testManager(client)

	err := m.Migrate(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "apply trace_root_topology_1m") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testManager(client *fakeSQLClient) *Manager {
	return &Manager{
		cfg:    testEmitterConfig(),
		logger: zap.NewNop(),
		factory: func(*greptime.Config) (sqlClient, error) {
			return client, nil
		},
	}
}

func testEmitterConfig() config.Emitter {
	return config.Emitter{
		GreptimeDBEndpoint: "127.0.0.1:4001",
		GreptimeDBDatabase: "mdai",
		GreptimeDBAuth:     "mdai:secret",
		Timeout:            config.Duration(5 * time.Second),
		TableName:          "trace_root_topology",
	}
}
