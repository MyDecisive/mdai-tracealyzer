package greptimecfg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		wantUser string
		wantPass string
	}{
		{
			name:     "empty",
			raw:      "",
			wantUser: "",
			wantPass: "",
		},
		{
			name:     "username and password",
			raw:      "mdai=secret",
			wantUser: "mdai",
			wantPass: "secret",
		},
		{
			name:     "password contains separator",
			raw:      "mdai=pass=word",
			wantUser: "mdai",
			wantPass: "pass=word",
		},
		{
			name:     "no separator",
			raw:      "secret",
			wantUser: "",
			wantPass: "secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotUser, gotPass := ParseAuth(tt.raw)
			require.Equal(t, tt.wantUser, gotUser, "ParseAuth(%q) user", tt.raw)
			require.Equal(t, tt.wantPass, gotPass, "ParseAuth(%q) password", tt.raw)
		})
	}
}
