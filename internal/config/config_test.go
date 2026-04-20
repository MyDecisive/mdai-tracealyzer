package config_test

import (
	"os"
	"path/filepath"
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
	assertEqual(t, "greptimedb:4001", cfg.Emitter.GreptimeDBEndpoint)
	assertEqual(t, "public", cfg.Emitter.GreptimeDBDatabase)
	assertEqual(t, "trace_topology", cfg.Emitter.TableName)
	assertEqual(t, 3, cfg.Emitter.MaxRetries)
	assertDuration(t, 10*time.Second, cfg.Emitter.Timeout)
	assertDuration(t, time.Second, cfg.Emitter.InitialBackoff)
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
	assertEqual(t, 5, cfg.Emitter.MaxRetries)
	assertDuration(t, 500*time.Millisecond, cfg.Emitter.InitialBackoff)
	assertEqual(t, "debug", cfg.Service.LogLevel)
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	t.Setenv("TRACEALYZER_BUFFER_VALKEY_ADDR", "env-valkey:6379")
	t.Setenv("TRACEALYZER_BUFFER_QUIET_PERIOD", "90s")
	t.Setenv("TRACEALYZER_SERVICE_LOG_LEVEL", "warn")
	t.Setenv("TRACEALYZER_EMITTER_MAX_RETRIES", "7")

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	assertEqual(t, "env-valkey:6379", cfg.Buffer.ValkeyAddr)
	assertDuration(t, 90*time.Second, cfg.Buffer.QuietPeriod)
	assertEqual(t, "warn", cfg.Service.LogLevel)
	assertEqual(t, 7, cfg.Emitter.MaxRetries)
}

func TestLoad_SecretsOnlyFromEnv(t *testing.T) {
	t.Setenv("TRACEALYZER_BUFFER_VALKEY_PASSWORD", "s3cret")
	t.Setenv("TRACEALYZER_EMITTER_GREPTIMEDB_AUTH", "tok3n")

	cfg, err := config.Load(filepath.Join("testdata", "valid.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	assertEqual(t, "s3cret", cfg.Buffer.ValkeyPassword)
	assertEqual(t, "tok3n", cfg.Emitter.GreptimeDBAuth)
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
	t.Setenv("TRACEALYZER_INGESTION_OTLP_GRPC_ENDPOINT", "")
	t.Setenv("TRACEALYZER_SERVICE_LOG_LEVEL", "trace")

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

func writeTempYAML(t *testing.T, body []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return path
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
