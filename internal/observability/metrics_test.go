package observability_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mydecisive/mdai-tracealyzer/internal/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRegistry_ServesGoCollectorMetrics(t *testing.T) {
	t.Parallel()

	reg := observability.NewRegistry()
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, http.NoBody)
	require.NoError(t, err)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	for _, want := range []string{"go_goroutines", "process_cpu_seconds_total"} {
		assert.Contains(t, string(body), want)
	}
}
