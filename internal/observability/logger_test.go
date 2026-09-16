package observability_test

import (
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/observability"
	"github.com/stretchr/testify/require"
)

func TestNewLogger_ValidLevels(t *testing.T) {
	t.Parallel()

	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			t.Parallel()

			logger, err := observability.NewLogger(level)
			require.NoError(t, err)
			require.NotNil(t, logger)
			logger.Info("smoke test")
			_ = logger.Sync()
		})
	}
}

func TestNewLogger_UppercaseLevel(t *testing.T) {
	t.Parallel()

	logger, err := observability.NewLogger("INFO")
	require.NoError(t, err)
	require.NotNil(t, logger)
}

func TestNewLogger_InvalidLevel(t *testing.T) {
	t.Parallel()

	_, err := observability.NewLogger("trace")
	require.Error(t, err, "expected error for invalid level")
}

func TestNewLogger_EmptyLevel(t *testing.T) {
	t.Parallel()

	_, err := observability.NewLogger("")
	require.Error(t, err, "expected error for empty level")
}
