package config_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/internal/config"
)

func TestLoad_DefaultsWhenNoPath(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	assertEqual(t, "0.0.0.0:4317", cfg.Ingestion.OTLPGRPCEndpoint)
	assertEqual(t, "0.0.0.0:4318", cfg.Ingestion.OTLPHTTPEndpoint)
	assertEqual(t, "localhost:6379", cfg.Buffer.ValkeyAddr)
	assertEqual(t, 0, cfg.Buffer.ValkeyDB)
	assertDuration(t, 60*time.Second, cfg.Buffer.QuietPeriod)
	assertDuration(t, 10*time.Minute, cfg.Buffer.MaxTTL)
	assertDuration(t, 5*time.Second, cfg.Buffer.SweepInterval)
	assertEqual(t, runtime.NumCPU(), cfg.Buffer.SweepWorkerPoolSize)
	assertEqual(t, "greptimedb:4001", cfg.Emitter.GreptimeDBEndpoint)
	assertEqual(t, "greptimedb:4003", cfg.Emitter.GreptimeDBSqlEndpoint)
	assertEqual(t, "public", cfg.Emitter.GreptimeDBDatabase)
	assertEqual(t, "14d", cfg.Emitter.TableTTL)
	assertEqual(t, 3, cfg.Emitter.MaxRetries)
	assertDuration(t, 10*time.Second, cfg.Emitter.Timeout)
	assertDuration(t, time.Second, cfg.Emitter.InitialBackoff)
	assertEqual(t, 100, cfg.Emitter.BatchSize)
	assertDuration(t, time.Second, cfg.Emitter.FlushInterval)
	assertEqual(t, 1024, cfg.Emitter.QueueCapacity)
	assertEqual(t, "info", cfg.Service.LogLevel)
	assertEqual(t, "0.0.0.0:9090", cfg.Service.MetricsEndpoint)
	assertDuration(t, 30*time.Second, cfg.Service.ShutdownGrace)
}

func TestLoad_YAMLOverridesDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	assertEqual(t, "0.0.0.0:14317", cfg.Ingestion.OTLPGRPCEndpoint)
	assertEqual(t, "valkey.internal:6379", cfg.Buffer.ValkeyAddr)
	assertEqual(t, 2, cfg.Buffer.ValkeyDB)
	assertDuration(t, 45*time.Second, cfg.Buffer.QuietPeriod)
	assertDuration(t, 8*time.Minute, cfg.Buffer.MaxTTL)
	assertEqual(t, "greptimedb.internal:4001", cfg.Emitter.GreptimeDBEndpoint)
	assertEqual(t, "greptimedb.internal:4003", cfg.Emitter.GreptimeDBSqlEndpoint)
	assertEqual(t, "21d", cfg.Emitter.TableTTL)
	assertEqual(t, 5, cfg.Emitter.MaxRetries)
	assertDuration(t, 500*time.Millisecond, cfg.Emitter.InitialBackoff)
	assertEqual(t, 250, cfg.Emitter.BatchSize)
	assertDuration(t, 2*time.Second, cfg.Emitter.FlushInterval)
	assertEqual(t, 4096, cfg.Emitter.QueueCapacity)
	assertEqual(t, "debug", cfg.Service.LogLevel)
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	t.Setenv("BUFFER_VALKEY_ADDR", "env-valkey:6379")
	t.Setenv("BUFFER_QUIET_PERIOD", "90s")
	t.Setenv("SERVICE_LOG_LEVEL", "warn")
	t.Setenv("EMITTER_MAX_RETRIES", "7")
	t.Setenv("EMITTER_QUEUE_CAPACITY", "512")
	t.Setenv("EMITTER_TABLE_TTL", "30d")
	t.Setenv("EMITTER_GREPTIMEDB_SQL_ENDPOINT", "env-greptime-sql:4003")

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	assertEqual(t, "env-valkey:6379", cfg.Buffer.ValkeyAddr)
	assertDuration(t, 90*time.Second, cfg.Buffer.QuietPeriod)
	assertEqual(t, "warn", cfg.Service.LogLevel)
	assertEqual(t, 7, cfg.Emitter.MaxRetries)
	assertEqual(t, 512, cfg.Emitter.QueueCapacity)
	assertEqual(t, "30d", cfg.Emitter.TableTTL)
	assertEqual(t, "env-greptime-sql:4003", cfg.Emitter.GreptimeDBSqlEndpoint)
}

func TestLoad_SecretsOnlyFromEnv(t *testing.T) {
	valkeyPassword := testEnvValue(t, "valkey")
	greptimeAuth := testEnvValue(t, "greptime")

	t.Setenv("BUFFER_VALKEY_PASSWORD", valkeyPassword)
	t.Setenv("EMITTER_GREPTIMEDB_AUTH", greptimeAuth)

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	assertEqual(t, valkeyPassword, cfg.Buffer.ValkeyPassword)
	assertEqual(t, greptimeAuth, cfg.Emitter.GreptimeDBAuth)
}

func TestLoad_YAMLSecretKeyRejected(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
buffer:
  valkey_password: "should-not-parse"
`))

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected parse error for secret key in YAML")
	}
	if !strings.Contains(err.Error(), "parse config file") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "valkey_password") {
		t.Fatalf("expected error to name the offending field; got: %v", err)
	}
}

func TestLoad_UnknownFieldRejected(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
buffer:
  quit_period: "30s"
`))

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected parse error for misspelled field")
	}
	if !strings.Contains(err.Error(), "parse config file") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "quit_period") {
		t.Fatalf("expected error to name the offending field; got: %v", err)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	t.Parallel()

	_, err := config.Load(filepath.Join("testdata", "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "read config file") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoad_BadDuration(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
buffer:
  quiet_period: "not-a-duration"
`))

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "parse config file") {
		t.Fatalf("unexpected error: %v", err)
	}
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
	if err == nil {
		t.Fatal("expected validation error")
	}
	msg := err.Error()
	for _, want := range []string{
		"ingestion.otlp_grpc_endpoint is required",
		"buffer.max_ttl must be greater than buffer.quiet_period",
		`service.log_level "trace" is invalid`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("missing %q in error: %s", want, msg)
		}
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
	if err == nil {
		t.Fatal("expected validation error")
	}

	msg := err.Error()
	for _, want := range []string{
		"emitter.batch_size must be > 0",
		"emitter.flush_interval must be > 0",
		"emitter.queue_capacity must be > 0",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("missing %q in error: %s", want, msg)
		}
	}
}

func TestValidate_RejectsBadEmitterTableTTL(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
emitter:
  table_ttl: "fortnight"
`))

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "emitter.table_ttl") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_RejectsMissingEmitterSQLEndpoint(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
emitter:
  greptimedb_sql_endpoint: ""
`))

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "emitter.greptimedb_sql_endpoint is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoad_EmitterTableTTLSupportsDaysUnit(t *testing.T) {
	t.Parallel()

	path := writeTempYAML(t, []byte(`
emitter:
  table_ttl: "14d"
`))

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertEqual(t, "14d", cfg.Emitter.TableTTL)
}

func writeTempYAML(t *testing.T, body []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return path
}

func testEnvValue(t *testing.T, name string) string {
	t.Helper()
	return "unit-test-" + name + "-" + strings.ReplaceAll(t.Name(), "/", "-")
}

func assertEqual[T comparable](t *testing.T, want, got T) {
	t.Helper()
	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func assertDuration(t *testing.T, want time.Duration, got config.Duration) {
	t.Helper()
	if want != got.Duration() {
		t.Errorf("want %v, got %v", want, got.Duration())
	}
}
