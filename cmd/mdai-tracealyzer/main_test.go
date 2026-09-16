package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewAdminServer_BaseContextNotCancellable(t *testing.T) {
	t.Parallel()

	srv := newAdminServer(http.NewServeMux())
	if srv.BaseContext == nil {
		t.Fatal("BaseContext is nil")
	}
	if done := srv.BaseContext(nil).Done(); done != nil {
		t.Fatal("BaseContext must not propagate cancellation; got Done() != nil")
	}
}

func TestBuildPprofMux_ServesIndex(t *testing.T) {
	t.Parallel()

	mux := buildPprofMux()
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s on pprof mux: got %d, want %d", path, rec.Code, http.StatusOK)
		}
	}
}
