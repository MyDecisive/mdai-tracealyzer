package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v3"
)

const envPrefix = "tracealyzer"

const (
	defaultQuietPeriod    = 60 * time.Second
	defaultMaxTTL         = 10 * time.Minute
	defaultSweepInterval  = 5 * time.Second
	defaultEmitterTimeout = 10 * time.Second
	defaultMaxRetries     = 3
	defaultInitialBackoff = time.Second
	defaultBatchSize      = 100
	defaultFlushInterval  = time.Second
	defaultQueueCapacity  = 1024
	defaultShutdownGrace  = 30 * time.Second
)

// Config is the root configuration for the service.
type Config struct {
	Ingestion Ingestion `yaml:"ingestion"`
	Buffer    Buffer    `yaml:"buffer"`
	Emitter   Emitter   `yaml:"emitter"`
	Service   Service   `yaml:"service"`
}

// Ingestion configures the OTLP receivers.
type Ingestion struct {
	OTLPGRPCEndpoint string `envconfig:"OTLP_GRPC_ENDPOINT" yaml:"otlp_grpc_endpoint"`
	OTLPHTTPEndpoint string `envconfig:"OTLP_HTTP_ENDPOINT" yaml:"otlp_http_endpoint"`
}

// Buffer configures the Valkey-backed span buffer and sweep cadence.
type Buffer struct {
	ValkeyAddr     string   `envconfig:"VALKEY_ADDR"     yaml:"valkey_addr"`
	ValkeyDB       int      `envconfig:"VALKEY_DB"       yaml:"valkey_db"`
	ValkeyPassword string   `envconfig:"VALKEY_PASSWORD" yaml:"-"`
	QuietPeriod    Duration `envconfig:"QUIET_PERIOD"    yaml:"quiet_period"`
	MaxTTL         Duration `envconfig:"MAX_TTL"         yaml:"max_ttl"`
	SweepInterval  Duration `envconfig:"SWEEP_INTERVAL"  yaml:"sweep_interval"`
}

// Emitter configures the GreptimeDB ingester.
type Emitter struct {
	GreptimeDBEndpoint string   `envconfig:"GREPTIMEDB_ENDPOINT" yaml:"greptimedb_endpoint"`
	GreptimeDBDatabase string   `envconfig:"GREPTIMEDB_DATABASE" yaml:"greptimedb_database"`
	GreptimeDBAuth     string   `envconfig:"GREPTIMEDB_AUTH"     yaml:"-"`
	TableName          string   `envconfig:"TABLE_NAME"          yaml:"table_name"`
	Timeout            Duration `envconfig:"TIMEOUT"             yaml:"timeout"`
	MaxRetries         int      `envconfig:"MAX_RETRIES"         yaml:"max_retries"`
	InitialBackoff     Duration `envconfig:"INITIAL_BACKOFF"     yaml:"initial_backoff"`
	BatchSize          int      `envconfig:"BATCH_SIZE"          yaml:"batch_size"`
	FlushInterval      Duration `envconfig:"FLUSH_INTERVAL"      yaml:"flush_interval"`
	QueueCapacity      int      `envconfig:"QUEUE_CAPACITY"      yaml:"queue_capacity"`
}

// Service configures cross-cutting runtime settings.
type Service struct {
	LogLevel        string   `envconfig:"LOG_LEVEL"        yaml:"log_level"`
	MetricsEndpoint string   `envconfig:"METRICS_ENDPOINT" yaml:"metrics_endpoint"`
	ShutdownGrace   Duration `envconfig:"SHUTDOWN_GRACE"   yaml:"shutdown_grace"`
}

// Load reads the YAML file at path (if non-empty), then applies
// environment-variable overrides, then validates the result. Built-in
// defaults fill anything left unset.
func Load(path string) (*Config, error) {
	cfg := defaults()

	if path != "" {
		// #nosec G304 -- path comes from an operator-controlled CLI flag.
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %q: %w", path, err)
		}
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("parse config file %q: %w", path, err)
		}
	}

	if err := envconfig.Process(envPrefix, &cfg); err != nil {
		return nil, fmt.Errorf("apply env overrides: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

// Validate checks all required fields and rejects unusable values.
func (c *Config) Validate() error {
	var errs []error
	errs = append(errs, c.Ingestion.validate()...)
	errs = append(errs, c.Buffer.validate()...)
	errs = append(errs, c.Emitter.validate()...)
	errs = append(errs, c.Service.validate()...)
	return errors.Join(errs...)
}

func (i *Ingestion) validate() []error {
	var errs []error
	if i.OTLPGRPCEndpoint == "" {
		errs = append(errs, errors.New("ingestion.otlp_grpc_endpoint is required"))
	}
	if i.OTLPHTTPEndpoint == "" {
		errs = append(errs, errors.New("ingestion.otlp_http_endpoint is required"))
	}
	return errs
}

func (b *Buffer) validate() []error {
	var errs []error
	if b.ValkeyAddr == "" {
		errs = append(errs, errors.New("buffer.valkey_addr is required"))
	}
	if b.ValkeyDB < 0 {
		errs = append(errs, errors.New("buffer.valkey_db must be >= 0"))
	}
	if b.QuietPeriod.Duration() <= 0 {
		errs = append(errs, errors.New("buffer.quiet_period must be > 0"))
	}
	if b.MaxTTL.Duration() <= 0 {
		errs = append(errs, errors.New("buffer.max_ttl must be > 0"))
	}
	if b.SweepInterval.Duration() <= 0 {
		errs = append(errs, errors.New("buffer.sweep_interval must be > 0"))
	}
	if b.MaxTTL.Duration() > 0 && b.QuietPeriod.Duration() > 0 &&
		b.MaxTTL.Duration() <= b.QuietPeriod.Duration() {
		errs = append(errs, errors.New("buffer.max_ttl must be greater than buffer.quiet_period"))
	}
	return errs
}

func (e *Emitter) validate() []error {
	var errs []error
	if e.GreptimeDBEndpoint == "" {
		errs = append(errs, errors.New("emitter.greptimedb_endpoint is required"))
	}
	if e.GreptimeDBDatabase == "" {
		errs = append(errs, errors.New("emitter.greptimedb_database is required"))
	}
	if e.TableName == "" {
		errs = append(errs, errors.New("emitter.table_name is required"))
	}
	if e.Timeout.Duration() <= 0 {
		errs = append(errs, errors.New("emitter.timeout must be > 0"))
	}
	if e.MaxRetries < 0 {
		errs = append(errs, errors.New("emitter.max_retries must be >= 0"))
	}
	if e.InitialBackoff.Duration() <= 0 {
		errs = append(errs, errors.New("emitter.initial_backoff must be > 0"))
	}
	if e.BatchSize <= 0 {
		errs = append(errs, "emitter.batch_size must be > 0")
	}
	if e.FlushInterval.Duration() <= 0 {
		errs = append(errs, "emitter.flush_interval must be > 0")
	}
	if e.QueueCapacity <= 0 {
		errs = append(errs, "emitter.queue_capacity must be > 0")
	}
	return errs
}

func (s *Service) validate() []error {
	var errs []error
	switch strings.ToLower(s.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf(
			"service.log_level %q is invalid (expected debug|info|warn|error)",
			s.LogLevel,
		))
	}
	if s.MetricsEndpoint == "" {
		errs = append(errs, errors.New("service.metrics_endpoint is required"))
	}
	if s.ShutdownGrace.Duration() <= 0 {
		errs = append(errs, errors.New("service.shutdown_grace must be > 0"))
	}
	return errs
}

func defaults() Config {
	return Config{
		Ingestion: Ingestion{
			OTLPGRPCEndpoint: "0.0.0.0:4317",
			OTLPHTTPEndpoint: "0.0.0.0:4318",
		},
		Buffer: Buffer{
			ValkeyAddr:     "localhost:6379",
			ValkeyDB:       0,
			ValkeyPassword: "",
			QuietPeriod:    Duration(defaultQuietPeriod),
			MaxTTL:         Duration(defaultMaxTTL),
			SweepInterval:  Duration(defaultSweepInterval),
		},
		Emitter: Emitter{
			GreptimeDBEndpoint: "greptimedb:4001",
			GreptimeDBDatabase: "public",
			GreptimeDBAuth:     "",
			TableName:          "trace_topology",
			Timeout:            Duration(defaultEmitterTimeout),
			MaxRetries:         defaultMaxRetries,
			InitialBackoff:     Duration(defaultInitialBackoff),
			BatchSize:          defaultBatchSize,
			FlushInterval:      Duration(defaultFlushInterval),
			QueueCapacity:      defaultQueueCapacity,
		},
		Service: Service{
			LogLevel:        "info",
			MetricsEndpoint: "0.0.0.0:9090",
			ShutdownGrace:   Duration(defaultShutdownGrace),
		},
	}
}
