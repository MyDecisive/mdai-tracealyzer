package config

import (
	"fmt"
	"time"
)

// Duration wraps time.Duration so both yaml.v3 and envconfig can parse values
// like "60s" via encoding.TextUnmarshaler. time.Duration itself implements
// neither, so a wrapper is required for the YAML path.
//
//nolint:recvcheck // UnmarshalText must mutate (pointer receiver); the Duration accessor is nil-safe as a value receiver.
type Duration time.Duration

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// UnmarshalText parses a Go duration string (time.ParseDuration).
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", string(text), err)
	}
	*d = Duration(v)
	return nil
}
