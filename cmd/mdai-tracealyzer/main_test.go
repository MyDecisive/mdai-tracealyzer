package main

import (
	"net/http"
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
