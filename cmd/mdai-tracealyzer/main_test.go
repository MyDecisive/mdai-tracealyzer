package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAdminServer_BaseContextNotCancellable(t *testing.T) {
	t.Parallel()

	srv := newAdminServer(http.NewServeMux())
	require.NotNil(t, srv.BaseContext)
	require.Nil(t, srv.BaseContext(nil).Done(), "BaseContext must not propagate cancellation")
}

func TestBuildPprofMux_ServesIndex(t *testing.T) {
	t.Parallel()

	mux := buildPprofMux()
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
		assert.Equal(t, http.StatusOK, rec.Code, "GET %s on pprof mux", path)
	}
}
