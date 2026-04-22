package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/vika/global_ratio_mode/test-stand/demo/internal/common"
)

func main() {
	service := common.Getenv("DD_SERVICE", "inventory-http-api")
	stopTracer := common.StartTracer(service)
	defer stopTracer()

	logger := common.NewLogger(service)
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

	addr := ":" + common.Getenv("PORT", "8080")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
