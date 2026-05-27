package main

import (
	"context"
	"net/http"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
)

func runPayments(service string, logger *common.Logger) error {
	httpClient := common.NewTracedHTTPClient(service)
	gatewayURL := common.Getenv("GATEWAY_URL", "http://gateway-api:8080")

	mux := http.NewServeMux()
	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/deep", deepForwardHandler(httpClient, logger, gatewayURL, "payments.deep_chain"))
	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/authorize", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		if r.URL.Query().Get("fail") == "true" {
			return nil, &common.HTTPError{Status: http.StatusInternalServerError, Message: "payments authorization failed"}
		}
		rollback := r.URL.Query().Get("rollback") == "true"
		status := "authorized"
		if rollback {
			status = "declined"
		}
		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"status":     status,
		}, nil
	})

	addr := ":" + common.Getenv("PORT", "8080")
	return http.ListenAndServe(addr, mux)
}
