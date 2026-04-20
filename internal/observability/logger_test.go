package observability_test

import (
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/observability"
)

func TestNewLogger_ValidLevels(t *testing.T) {
	t.Parallel()

	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			t.Parallel()

			logger, err := observability.NewLogger(level)
			if err != nil {
				t.Fatalf("NewLogger(%q): %v", level, err)
			}
			if logger == nil {
				t.Fatalf("NewLogger(%q): nil logger", level)
			}
			logger.Info("smoke test")
			_ = logger.Sync()
		})
	}
}

func TestNewLogger_UppercaseLevel(t *testing.T) {
	t.Parallel()

	logger, err := observability.NewLogger("INFO")
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	if logger == nil {
		t.Fatal("nil logger")
	}
}

func TestNewLogger_InvalidLevel(t *testing.T) {
	t.Parallel()

	_, err := observability.NewLogger("trace")
	if err == nil {
		t.Fatal("expected error for invalid level")
	}
}

func TestNewLogger_EmptyLevel(t *testing.T) {
	t.Parallel()

	_, err := observability.NewLogger("")
	if err == nil {
		t.Fatal("expected error for empty level")
	}
}
