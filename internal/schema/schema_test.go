package schema

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/stretchr/testify/require"
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

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (fakeResult) RowsAffected() (int64, error) {
	return 0, nil
}

func (c *fakeSQLConn) PingContext(context.Context) error {
	return c.pingErr
}

//nolint:ireturn
func (c *fakeSQLConn) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	c.execs = append(c.execs, query)
	if len(c.execErrs) > 0 {
		err := c.execErrs[0]
		c.execErrs = c.execErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return fakeResult{}, nil
}

//nolint:ireturn // Test fake must satisfy the production rowSet interface seam.
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

	require.NoError(t, m.Migrate(context.Background()))
	want := []string{createSourceTableSQL("14d"), createSinkTableSQL(), createFlowSQL()}
	require.Equal(t, want, conn.execs, "statements")
	require.True(t, conn.closed, "expected connection close")
}

func TestManagerCheckReadyChecksAllObjects(t *testing.T) {
	t.Parallel()

	conn := &fakeSQLConn{}
	m := testManager(conn)

	require.NoError(t, m.CheckReady(context.Background()))
	want := []string{
		"SHOW CREATE TABLE trace_root_topology",
		"SHOW CREATE TABLE trace_root_topology_1m",
		"SHOW CREATE FLOW trace_root_topology_1m_flow",
	}
	require.Equal(t, want, conn.queries, "queries")
}

func TestManagerConnectFailsOnPing(t *testing.T) {
	t.Parallel()

	conn := &fakeSQLConn{pingErr: errors.New("unreachable")}
	m := testManager(conn)

	err := m.CheckReady(context.Background())
	require.ErrorContains(t, err, "ping greptimedb sql endpoint")
	require.True(t, conn.closed, "expected connection close on ping error")
}

func TestManagerMigrateFailsOnStatementError(t *testing.T) {
	t.Parallel()

	conn := &fakeSQLConn{execErrs: []error{nil, errors.New("ddl failed")}}
	m := testManager(conn)

	err := m.Migrate(context.Background())
	require.ErrorContains(t, err, "apply trace_root_topology_1m")
}

func TestBuildPostgresDSN(t *testing.T) {
	t.Parallel()

	dsn, err := buildPostgresDSN(testEmitterConfig())
	require.NoError(t, err)
	want := "postgres://mdai:secret@127.0.0.1:4003/mdai?sslmode=disable" //nolint:gosec // test fixture, not a real credential.
	require.Equal(t, want, dsn)
}

func TestBuildPostgresDSN_EmptyAuth(t *testing.T) {
	t.Parallel()

	cfg := testEmitterConfig()
	cfg.GreptimeDBAuth = ""

	dsn, err := buildPostgresDSN(cfg)
	require.NoError(t, err)
	want := "postgres://127.0.0.1:4003/mdai?sslmode=disable"
	require.Equal(t, want, dsn)
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
		GreptimeDBAuth:        "mdai=secret",
		Timeout:               config.Duration(5 * time.Second),
		TableTTL:              "14d",
	}
}
