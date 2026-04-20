// Package observability holds the zap logger and Prometheus registry shared
// between the binary entry point and the service's subsystems.
package observability

import (
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger builds a *zap.Logger whose level comes from the service config.
// Debug level selects the development (console) encoder; every other level
// selects the production (JSON) encoder.
func NewLogger(level string) (*zap.Logger, error) {
	if level == "" {
		return nil, errors.New("parse log level: empty")
	}
	lvl, err := zapcore.ParseLevel(strings.ToLower(level))
	if err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", level, err)
	}

	var cfg zap.Config
	if lvl == zapcore.DebugLevel {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	cfg.Level = zap.NewAtomicLevelAt(lvl)

	logger, err := cfg.Build()
	if err != nil {
		return nil, fmt.Errorf("build logger: %w", err)
	}
	return logger, nil
}
