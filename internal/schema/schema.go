package schema

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/options"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request/header"
	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	sourceTableName = "trace_root_topology"
	sinkTableName   = "trace_root_topology_1m"
	flowName        = "trace_root_topology_1m_flow"
)

const (
	createSourceTableSQL = `CREATE TABLE IF NOT EXISTS trace_root_topology (
  "timestamp"         TIMESTAMP(9) TIME INDEX,
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
)`

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
WHERE timestamp >= now() - INTERVAL '2 minutes'
  AND timestamp <  now() - INTERVAL '1 minute'
GROUP BY time_window, root_id`
)

type sqlClient interface {
	HealthCheck(ctx context.Context) (*gpb.HealthCheckResponse, error)
	Handle(ctx context.Context, req *gpb.GreptimeRequest) (*gpb.GreptimeResponse, error)
	Close() error
}

type sqlFactory func(*greptime.Config) (sqlClient, error)

type Manager struct {
	cfg     config.Emitter
	logger  *zap.Logger
	factory sqlFactory
}

func New(cfg config.Emitter, logger *zap.Logger) *Manager {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{
		cfg:    cfg,
		logger: logger,
		factory: func(clientCfg *greptime.Config) (sqlClient, error) {
			return newSQLClient(clientCfg)
		},
	}
}

func (m *Manager) Migrate(ctx context.Context) error {
	client, err := m.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	for _, stmt := range migrationStatements() {
		m.logger.Info("apply schema statement", zap.String("object", stmt.name))
		if err := execSQL(ctx, client, stmt.sql); err != nil {
			return fmt.Errorf("apply %s: %w", stmt.name, err)
		}
	}
	return nil
}

func (m *Manager) CheckReady(ctx context.Context) error {
	client, err := m.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	for _, stmt := range readinessStatements() {
		m.logger.Info("check schema object", zap.String("object", stmt.name))
		if err := execSQL(ctx, client, stmt.sql); err != nil {
			return fmt.Errorf("check %s: %w", stmt.name, err)
		}
	}
	return nil
}

type statement struct {
	name string
	sql  string
}

func migrationStatements() []statement {
	return []statement{
		{name: sourceTableName, sql: createSourceTableSQL},
		{name: sinkTableName, sql: createSinkTableSQL},
		{name: flowName, sql: createFlowSQL},
	}
}

func readinessStatements() []statement {
	return []statement{
		{name: sourceTableName, sql: fmt.Sprintf("DESC TABLE %s", sourceTableName)},
		{name: sinkTableName, sql: fmt.Sprintf("DESC TABLE %s", sinkTableName)},
		{name: flowName, sql: fmt.Sprintf("DESC FLOW %s", flowName)},
	}
}

func (m *Manager) connect(ctx context.Context) (sqlClient, error) {
	host, port, err := splitEndpoint(m.cfg.GreptimeDBEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse greptimedb endpoint: %w", err)
	}

	username, password := parseAuth(m.cfg.GreptimeDBAuth)
	clientCfg := greptime.NewConfig(host).
		WithPort(port).
		WithDatabase(m.cfg.GreptimeDBDatabase).
		WithAuth(username, password)

	client, err := m.factory(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("create greptimedb client: %w", err)
	}

	healthCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout.Duration())
	defer cancel()
	if _, err := client.HealthCheck(healthCtx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("health check greptimedb: %w", err)
	}
	return client, nil
}

func execSQL(ctx context.Context, client sqlClient, sql string) error {
	req := &gpb.GreptimeRequest{
		Request: &gpb.GreptimeRequest_Query{
			Query: &gpb.QueryRequest{
				Query: &gpb.QueryRequest_Sql{Sql: sql},
			},
		},
	}
	resp, err := client.Handle(ctx, req)
	if err != nil {
		return err
	}
	status := resp.GetHeader().GetStatus()
	if status.GetStatusCode() == 0 {
		return nil
	}
	return fmt.Errorf("status_code=%d err_msg=%q", status.GetStatusCode(), status.GetErrMsg())
}

type grpcSQLClient struct {
	conn   *grpc.ClientConn
	db     gpb.GreptimeDatabaseClient
	health gpb.HealthCheckClient
	header *gpb.RequestHeader
}

func newSQLClient(cfg *greptime.Config) (sqlClient, error) {
	conn, err := grpc.NewClient(
		fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		options.NewUserAgentOption("schema").Build(),
		options.NewTlsOption(true).Build(),
	)
	if err != nil {
		return nil, err
	}

	header_, err := header.New(cfg.Database).WithAuth(cfg.Username, cfg.Password).Build()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &grpcSQLClient{
		conn:   conn,
		db:     gpb.NewGreptimeDatabaseClient(conn),
		health: gpb.NewHealthCheckClient(conn),
		header: header_,
	}, nil
}

func (c *grpcSQLClient) HealthCheck(ctx context.Context) (*gpb.HealthCheckResponse, error) {
	return c.health.HealthCheck(ctx, &gpb.HealthCheckRequest{})
}

func (c *grpcSQLClient) Handle(ctx context.Context, req *gpb.GreptimeRequest) (*gpb.GreptimeResponse, error) {
	req.Header = c.header
	return c.db.Handle(ctx, req)
}

func (c *grpcSQLClient) Close() error {
	return c.conn.Close()
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
