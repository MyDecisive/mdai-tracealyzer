package config

import "testing"

func TestValidateTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "seconds", raw: "60s"},
		{name: "hours", raw: "1h"},
		{name: "days", raw: "14d"},
		{name: "compound", raw: "1d2h3m4s"},
		{name: "zero allowed", raw: "0d"},
		{name: "bad unit", raw: "1w", wantErr: true},
		{name: "missing unit", raw: "14", wantErr: true},
		{name: "negative", raw: "-1d", wantErr: true},
		{name: "empty", raw: "", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateTTL(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("validateTTL(%q): %v", tt.raw, err)
			}
		})
	}
}
