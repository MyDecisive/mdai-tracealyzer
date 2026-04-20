package main

import (
	"context"
	"log"
	"net/http"

	"github.com/vika/global_ratio_mode/test-stand/demo/internal/common"
)

func main() {
	service := common.Getenv("DD_SERVICE", "catalog-api")
	stopTracer := common.StartTracer(service)
	defer stopTracer()

	logger := common.NewLogger(service)
	mux := http.NewServeMux()

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/catalog", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"items": []map[string]any{
				{"sku": "coffee", "stock": 42},
				{"sku": "mug", "stock": 11},
			},
		}, nil
	})

	addr := ":" + common.Getenv("PORT", "8080")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
