package config_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_DefaultsWhenNoPath(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load("")
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0:4317", cfg.Ingestion.OTLPGRPCEndpoint)
	assert.Equal(t, "0.0.0.0:4318", cfg.Ingestion.OTLPHTTPEndpoint)
	assert.Equal(t, "localhost:6379", cfg.Buffer.ValkeyAddr)
	assert.Equal(t, 0, cfg.Buffer.ValkeyDB)
	assert.Equal(t, 60*time.Second, cfg.Buffer.QuietPeriod.Duration())
	assert.Equal(t, 10*time.Minute, cfg.Buffer.MaxTTL.Duration())
	assert.Equal(t, 5*time.Second, cfg.Buffer.SweepInterval.Duration())
	assert.Equal(t, 10*time.Second, cfg.Buffer.ValkeyOperationTimeout.Duration())
	assert.Equal(t, runtime.NumCPU(), cfg.Buffer.SweepWorkerPoolSize)
	assert.Equal(t, "greptimedb:4001", cfg.Emitter.GreptimeDBEndpoint)
	assert.Equal(t, "greptimedb:4003", cfg.Emitter.GreptimeDBSqlEndpoint)
	assert.Equal(t, "public", cfg.Emitter.GreptimeDBDatabase)
	assert.Equal(t, "14d", cfg.Emitter.TableTTL)
	assert.Equal(t, 3, cfg.Emitter.MaxRetries)
	assert.Equal(t, 10*time.Second, cfg.Emitter.Timeout.Duration())
	assert.Equal(t, time.Second, cfg.Emitter.InitialBackoff.Duration())
	assert.Equal(t, 100, cfg.Emitter.BatchSize)
	assert.Equal(t, time.Second, cfg.Emitter.FlushInterval.Duration())
	assert.Equal(t, 1024, cfg.Emitter.QueueCapacity)
	assert.Equal(t, "info", cfg.Service.LogLevel)
	assert.Equal(t, "0.0.0.0:9090", cfg.Service.MetricsEndpoint)
	assert.Equal(t, "127.0.0.1:6060", cfg.Service.PprofEndpoint)
	assert.Equal(t, 30*time.Second, cfg.Service.ShutdownGrace.Duration())
}

func TestLoad_YAMLOverridesDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0:14317", cfg.Ingestion.OTLPGRPCEndpoint)
	assert.Equal(t, "valkey.internal:6379", cfg.Buffer.ValkeyAddr)
	assert.Equal(t, 2, cfg.Buffer.ValkeyDB)
	assert.Equal(t, 4*time.Second, cfg.Buffer.ValkeyOperationTimeout.Duration())
	assert.Equal(t, 45*time.Second, cfg.Buffer.QuietPeriod.Duration())
	assert.Equal(t, 8*time.Minute, cfg.Buffer.MaxTTL.Duration())
	assert.Equal(t, "greptimedb.internal:4001", cfg.Emitter.GreptimeDBEndpoint)
	assert.Equal(t, "greptimedb.internal:4003", cfg.Emitter.GreptimeDBSqlEndpoint)
	assert.Equal(t, "21d", cfg.Emitter.TableTTL)
	assert.Equal(t, 5, cfg.Emitter.MaxRetries)
	assert.Equal(t, 500*time.Millisecond, cfg.Emitter.InitialBackoff.Duration())
	assert.Equal(t, 250, cfg.Emitter.BatchSize)
	assert.Equal(t, 2*time.Second, cfg.Emitter.FlushInterval.Duration())
	assert.Equal(t, 4096, cfg.Emitter.QueueCapacity)
	assert.Equal(t, "debug", cfg.Service.LogLevel)
	assert.Empty(t, cfg.Service.PprofEndpoint)
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	t.Setenv("BUFFER_VALKEY_ADDR", "env-valkey:6379")
	t.Setenv("BUFFER_QUIET_PERIOD", "90s")
	t.Setenv("BUFFER_VALKEY_OPERATION_TIMEOUT", "7s")
	t.Setenv("SERVICE_LOG_LEVEL", "warn")
	t.Setenv("EMITTER_MAX_RETRIES", "7")
	t.Setenv("EMITTER_QUEUE_CAPACITY", "512")
	t.Setenv("EMITTER_TABLE_TTL", "30d")
	t.Setenv("EMITTER_GREPTIMEDB_SQL_ENDPOINT", "env-greptime-sql:4003")

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	require.NoError(t, err)

	assert.Equal(t, "env-valkey:6379", cfg.Buffer.ValkeyAddr)
	assert.Equal(t, 90*time.Second, cfg.Buffer.QuietPeriod.Duration())
	assert.Equal(t, 7*time.Second, cfg.Buffer.ValkeyOperationTimeout.Duration())
	assert.Equal(t, "warn", cfg.Service.LogLevel)
	assert.Equal(t, 7, cfg.Emitter.MaxRetries)
	assert.Equal(t, 512, cfg.Emitter.QueueCapacity)
	assert.Equal(t, "30d", cfg.Emitter.TableTTL)
	assert.Equal(t, "env-greptime-sql:4003", cfg.Emitter.GreptimeDBSqlEndpoint)
}

func TestValidate_RejectsZeroValkeyOperationTimeout(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
buffer:
  valkey_operation_timeout: "0s"
`))

	_, err := config.Load(path)
	require.ErrorContains(t, err, "buffer.valkey_operation_timeout must be > 0")
}

func TestLoad_SecretsOnlyFromEnv(t *testing.T) {
	valkeyPassword := testEnvValue(t, "valkey")
	greptimeAuth := testEnvValue(t, "greptime")

	t.Setenv("BUFFER_VALKEY_PASSWORD", valkeyPassword)
	t.Setenv("EMITTER_GREPTIMEDB_AUTH", greptimeAuth)

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	require.NoError(t, err)

	assert.Equal(t, valkeyPassword, cfg.Buffer.ValkeyPassword)
	assert.Equal(t, greptimeAuth, cfg.Emitter.GreptimeDBAuth)
}

func TestLoad_YAMLSecretKeyRejected(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
buffer:
  valkey_password: "should-not-parse"
`))

	_, err := config.Load(path)
	require.ErrorContains(t, err, "parse config file")
	require.ErrorContains(t, err, "valkey_password", "error must name the offending field")
}

func TestLoad_UnknownFieldRejected(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
buffer:
  quit_period: "30s"
`))

	_, err := config.Load(path)
	require.ErrorContains(t, err, "parse config file")
	require.ErrorContains(t, err, "quit_period", "error must name the offending field")
}

func TestLoad_MissingFile(t *testing.T) {
	t.Parallel()

	_, err := config.Load(filepath.Join("testdata", "does-not-exist.yaml"))
	require.ErrorContains(t, err, "read config file")
}

func TestLoad_BadDuration(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
buffer:
  quiet_period: "not-a-duration"
`))

	_, err := config.Load(path)
	require.ErrorContains(t, err, "parse config file")
}

func TestValidate_RejectsBadConfig(t *testing.T) {
	t.Setenv("INGESTION_OTLP_GRPC_ENDPOINT", "")
	t.Setenv("SERVICE_LOG_LEVEL", "trace")

	path := writeTempYAML(t, []byte(`
ingestion:
  otlp_grpc_endpoint: ""
buffer:
  quiet_period: "10m"
  max_ttl: "1m"
service:
  log_level: "trace"
`))

	_, err := config.Load(path)
	require.Error(t, err)
	msg := err.Error()
	for _, want := range []string{
		"ingestion.otlp_grpc_endpoint is required",
		"buffer.max_ttl must be greater than buffer.quiet_period",
		`service.log_level "trace" is invalid`,
	} {
		assert.Contains(t, msg, want)
	}
}

func TestValidate_RejectsBadEmitterBatchConfig(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
emitter:
  batch_size: 0
  flush_interval: "0s"
  queue_capacity: -1
`))

	_, err := config.Load(path)
	require.Error(t, err)

	msg := err.Error()
	for _, want := range []string{
		"emitter.batch_size must be > 0",
		"emitter.flush_interval must be > 0",
		"emitter.queue_capacity must be > 0",
	} {
		assert.Contains(t, msg, want)
	}
}

func TestValidate_RejectsBadEmitterTableTTL(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
emitter:
  table_ttl: "fortnight"
`))

	_, err := config.Load(path)
	require.ErrorContains(t, err, "emitter.table_ttl")
}

func TestValidate_RejectsMissingEmitterSQLEndpoint(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
emitter:
  greptimedb_sql_endpoint: ""
`))

	_, err := config.Load(path)
	require.ErrorContains(t, err, "emitter.greptimedb_sql_endpoint is required")
}

func TestLoad_EmitterTableTTLSupportsDaysUnit(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
emitter:
  table_ttl: "14d"
`))

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, "14d", cfg.Emitter.TableTTL)
}

func writeTempYAML(t *testing.T, body []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, body, 0o600), "write temp yaml")
	return path
}

func testEnvValue(t *testing.T, name string) string {
	t.Helper()
	return "unit-test-" + name + "-" + strings.ReplaceAll(t.Name(), "/", "-")
}
