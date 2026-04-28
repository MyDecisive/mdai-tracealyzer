package schema

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"go.uber.org/zap"
)

const (
	sourceTableName = "trace_root_topology"
	sinkTableName   = "trace_root_topology_1m"
	flowName        = "trace_root_topology_1m_flow"
)

const (
	createSinkTableSQL = `CREATE TABLE IF NOT EXISTS trace_root_topology_1m (
  time_window       TIMESTAMP(9) TIME INDEX,
  root_id           STRING,
  breadth_sketch    BINARY,
  depth_sketch      BINARY,
  duration_sketch   BINARY,
  trace_count       BIGINT,
  error_count_total BIGINT,
  PRIMARY KEY (root_id)
)`

	createFlowSQL = `CREATE FLOW IF NOT EXISTS trace_root_topology_1m_flow
SINK TO trace_root_topology_1m
AS
SELECT
  date_bin('1 minute', timestamp)               AS time_window,
  root_id,
  uddsketch_state(128, 0.01, breadth)           AS breadth_sketch,
  uddsketch_state(128, 0.01, service_hop_depth) AS depth_sketch,
  uddsketch_state(128, 0.01, root_duration_ns)  AS duration_sketch,
  count(*)                                      AS trace_count,
  sum(error_count)                              AS error_count_total
FROM trace_root_topology
GROUP BY time_window, root_id`
)

type rowSet interface {
	Close() error
	Next() bool
	Err() error
}

type sqlConn interface {
	PingContext(ctx context.Context) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (rowSet, error)
	Close() error
}

type opener func(dsn string) (sqlConn, error)

type Manager struct {
	cfg    config.Emitter
	logger *zap.Logger
	open   opener
}

func New(cfg config.Emitter, logger *zap.Logger) *Manager {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{
		cfg:    cfg,
		logger: logger,
		open:   openPostgresDB,
	}
}

func (m *Manager) Migrate(ctx context.Context) error {
	conn, err := m.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range migrationStatements(m.cfg) {
		m.logger.Info("apply schema statement", zap.String("object", stmt.name))
		if err := execStatement(ctx, conn, stmt.sql); err != nil {
			return fmt.Errorf("apply %s: %w", stmt.name, err)
		}
	}
	return nil
}

func (m *Manager) CheckReady(ctx context.Context) error {
	conn, err := m.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range readinessStatements() {
		m.logger.Info("check schema object", zap.String("object", stmt.name))
		if err := queryStatement(ctx, conn, stmt.sql); err != nil {
			return fmt.Errorf("check %s: %w", stmt.name, err)
		}
	}
	return nil
}

type statement struct {
	name string
	sql  string
}

func migrationStatements(cfg config.Emitter) []statement {
	return []statement{
		{name: sourceTableName, sql: createSourceTableSQL(cfg.TableTTL)},
		{name: sinkTableName, sql: createSinkTableSQL},
		{name: flowName, sql: createFlowSQL},
	}
}

func readinessStatements() []statement {
	return []statement{
		{name: sourceTableName, sql: fmt.Sprintf("SHOW CREATE TABLE %s", sourceTableName)},
		{name: sinkTableName, sql: fmt.Sprintf("SHOW CREATE TABLE %s", sinkTableName)},
		{name: flowName, sql: fmt.Sprintf("SHOW CREATE FLOW %s", flowName)},
	}
}

func createSourceTableSQL(ttl string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS trace_root_topology (
  "timestamp"       TIMESTAMP(9) TIME INDEX,
  root_id           STRING,
  trace_id          STRING,
  root_service      STRING,
  root_operation    STRING,
  breadth           INT,
  service_hop_depth INT,
  service_count     INT,
  operation_count   INT,
  span_count        INT,
  error_count       INT,
  root_duration_ns  BIGINT,
  PRIMARY KEY (root_id, trace_id)
) WITH (ttl='%s')`, ttl)
}

func (m *Manager) connect(ctx context.Context) (sqlConn, error) {
	dsn, err := buildPostgresDSN(m.cfg)
	if err != nil {
		return nil, err
	}

	conn, err := m.open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open greptimedb sql client: %w", err)
	}

	healthCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout.Duration())
	defer cancel()
	if err := conn.PingContext(healthCtx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping greptimedb sql endpoint: %w", err)
	}
	return conn, nil
}

func execStatement(ctx context.Context, conn sqlConn, query string) error {
	_, err := conn.ExecContext(ctx, query)
	return err
}

func queryStatement(ctx context.Context, conn sqlConn, query string) error {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		break
	}
	return rows.Err()
}

type sqlDB struct {
	*sql.DB
}

func (db sqlDB) QueryContext(ctx context.Context, query string, args ...any) (rowSet, error) {
	return db.DB.QueryContext(ctx, query, args...)
}

func openPostgresDB(dsn string) (sqlConn, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return sqlDB{DB: db}, nil
}

func buildPostgresDSN(cfg config.Emitter) (string, error) {
	host, port, err := splitEndpoint(cfg.GreptimeDBSqlEndpoint)
	if err != nil {
		return "", fmt.Errorf("parse greptimedb sql endpoint: %w", err)
	}

	username, password := parseAuth(cfg.GreptimeDBAuth)
	u := &url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   "/" + cfg.GreptimeDBDatabase,
	}
	if username != "" || password != "" {
		u.User = url.UserPassword(username, password)
	}

	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String(), nil
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

func parseAuth(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}
	if user, pass, ok := strings.Cut(raw, ":"); ok {
		return user, pass
	}
	return "", raw
}
