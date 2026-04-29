package greptimecfg

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

const (
	BaseName        = "trace_root_topology"
	SourceTableName = BaseName
	SinkTableName   = BaseName + "_1m"
	FlowName        = BaseName + "_1m_flow"
)

func SplitEndpoint(endpoint string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("port %q: %w", portStr, err)
	}
	return host, port, nil
}

// ParseAuth splits the configured GreptimeDB auth string into username and password.
// The current deployment convention uses "=" as the separator.
func ParseAuth(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}
	if user, pass, ok := strings.Cut(raw, "="); ok {
		return user, pass
	}
	return "", raw
}
