package greptimecfg

import "testing"

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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotUser, gotPass := ParseAuth(tt.raw)
			if gotUser != tt.wantUser || gotPass != tt.wantPass {
				t.Fatalf("ParseAuth(%q) = (%q, %q), want (%q, %q)", tt.raw, gotUser, gotPass, tt.wantUser, tt.wantPass)
			}
		})
	}
}
