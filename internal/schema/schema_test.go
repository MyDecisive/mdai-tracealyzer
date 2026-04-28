package schema

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"go.uber.org/zap"
)

type fakeRows struct {
	closed bool
}

func (r *fakeRows) Close() error {
	r.closed = true
	return nil
}

func (*fakeRows) Next() bool { return false }

func (*fakeRows) Err() error { return nil }

type fakeSQLConn struct {
	pingErr   error
	execErrs  []error
	queryErrs []error
	execs     []string
	queries   []string
	closed    bool
}

func (c *fakeSQLConn) PingContext(context.Context) error {
	return c.pingErr
}

func (c *fakeSQLConn) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	c.execs = append(c.execs, query)
	if len(c.execErrs) > 0 {
		err := c.execErrs[0]
		c.execErrs = c.execErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (c *fakeSQLConn) QueryContext(_ context.Context, query string, _ ...any) (rowSet, error) {
	c.queries = append(c.queries, query)
	if len(c.queryErrs) > 0 {
		err := c.queryErrs[0]
		c.queryErrs = c.queryErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &fakeRows{}, nil
}

func (c *fakeSQLConn) Close() error {
	c.closed = true
	return nil
}

func TestManagerMigrateAppliesStatementsInOrder(t *testing.T) {
	t.Parallel()

	conn := &fakeSQLConn{}
	m := testManager(conn)

	if err := m.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	want := []string{createSourceTableSQL("14d"), createSinkTableSQL, createFlowSQL}
	if len(conn.execs) != len(want) {
		t.Fatalf("want %d statements, got %d", len(want), len(conn.execs))
	}
	for i := range want {
		if conn.execs[i] != want[i] {
			t.Fatalf("statement %d mismatch\nwant: %q\n got: %q", i, want[i], conn.execs[i])
		}
	}
	if !conn.closed {
		t.Fatal("expected connection close")
	}
}

func TestManagerCheckReadyChecksAllObjects(t *testing.T) {
	t.Parallel()

	conn := &fakeSQLConn{}
	m := testManager(conn)

	if err := m.CheckReady(context.Background()); err != nil {
		t.Fatalf("CheckReady: %v", err)
	}
	want := []string{
		"SHOW CREATE TABLE trace_root_topology",
		"SHOW CREATE TABLE trace_root_topology_1m",
		"SHOW CREATE FLOW trace_root_topology_1m_flow",
	}
	if len(conn.queries) != len(want) {
		t.Fatalf("want %d queries, got %d", len(want), len(conn.queries))
	}
	for i := range want {
		if conn.queries[i] != want[i] {
			t.Fatalf("query %d mismatch\nwant: %q\n got: %q", i, want[i], conn.queries[i])
		}
	}
}

func TestManagerConnectFailsOnPing(t *testing.T) {
	t.Parallel()

	conn := &fakeSQLConn{pingErr: errors.New("unreachable")}
	m := testManager(conn)

	err := m.CheckReady(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "ping greptimedb sql endpoint") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !conn.closed {
		t.Fatal("expected connection close on ping error")
	}
}

func TestManagerMigrateFailsOnStatementError(t *testing.T) {
	t.Parallel()

	conn := &fakeSQLConn{execErrs: []error{nil, errors.New("ddl failed")}}
	m := testManager(conn)

	err := m.Migrate(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "apply trace_root_topology_1m") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildPostgresDSN(t *testing.T) {
	t.Parallel()

	dsn, err := buildPostgresDSN(testEmitterConfig())
	if err != nil {
		t.Fatalf("buildPostgresDSN: %v", err)
	}
	want := "postgres://mdai:secret@127.0.0.1:4003/mdai?sslmode=disable"
	if dsn != want {
		t.Fatalf("dsn = %q, want %q", dsn, want)
	}
}

func testManager(conn *fakeSQLConn) *Manager {
	return &Manager{
		cfg:    testEmitterConfig(),
		logger: zap.NewNop(),
		open: func(string) (sqlConn, error) {
			return conn, nil
		},
	}
}

func testEmitterConfig() config.Emitter {
	return config.Emitter{
		GreptimeDBEndpoint:    "127.0.0.1:4001",
		GreptimeDBSqlEndpoint: "127.0.0.1:4003",
		GreptimeDBDatabase:    "mdai",
		GreptimeDBAuth:        "mdai:secret",
		Timeout:               config.Duration(5 * time.Second),
		TableName:             "trace_root_topology",
		TableTTL:              "14d",
	}
}
