package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
)

func runInventoryHTTP(service string, logger *common.Logger) error {
	httpClient := common.NewTracedHTTPClient(service)
	catalogURL := common.Getenv("CATALOG_URL", "http://catalog-api:8080")

	mux := http.NewServeMux()

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/availability", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"warehouse":  "east",
			"available":  42,
			"status":     "available",
		}, nil
	})

	common.RegisterJSONRoute(mux, service, logger, http.MethodPost, "/reserve", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		var payload map[string]any
		if r.Body != nil {
			defer r.Body.Close()
			_ = json.NewDecoder(r.Body).Decode(&payload)
		}
		return map[string]any{
			"request_id":     meta.RequestID,
			"scenario":       meta.Scenario,
			"warehouse":      "east",
			"status":         "reserved",
			"reservation_id": common.ReservationID(meta.RequestID),
			"reserved":       true,
			"payload":        payload,
		}, nil
	})

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/deep-check", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		catalog, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, catalogURL+"/catalog", "inventory.fetch_catalog", meta, nil, nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"warehouse":  "east",
			"available":  42,
			"status":     "available",
			"catalog":    catalog,
		}, nil
	})

	addr := ":" + common.Getenv("PORT", "8080")
	return http.ListenAndServe(addr, mux)
}
