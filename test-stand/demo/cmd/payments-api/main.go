package main

import (
	"context"
	"log"
	"net/http"

	"github.com/vika/global_ratio_mode/test-stand/demo/internal/common"
)

func main() {
	service := common.Getenv("DD_SERVICE", "payments-api")
	stopTracer := common.StartTracer(service)
	defer stopTracer()

	logger := common.NewLogger(service)
	mux := http.NewServeMux()

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/authorize", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
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
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
