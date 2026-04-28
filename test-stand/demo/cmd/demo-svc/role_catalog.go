package main

import (
	"context"
	"net/http"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
)

func runCatalog(service string, logger *common.Logger) error {
	dsn := common.Getenv("POSTGRES_DSN", "")
	var pg *common.Postgres
	if dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var err error
		pg, err = common.NewPostgres(ctx, service, dsn)
		if err != nil {
			return err
		}
		defer pg.Close(context.Background())
	}

	mux := http.NewServeMux()
	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/catalog", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		source := r.URL.Query().Get("source")
		var items []map[string]any

		if source == "db" {
			if pg == nil {
				return nil, &common.HTTPError{Status: http.StatusServiceUnavailable, Message: "postgres not configured; POSTGRES_DSN env is empty"}
			}
			rows, err := pg.Query(ctx, "SELECT items", "SELECT sku, stock FROM items")
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				var sku string
				var stock int
				if err := rows.Scan(&sku, &stock); err != nil {
					return nil, err
				}
				items = append(items, map[string]any{"sku": sku, "stock": stock})
			}
			if err := rows.Err(); err != nil {
				return nil, err
			}
		} else {
			items = []map[string]any{
				{"sku": "coffee", "stock": 42},
				{"sku": "mug", "stock": 11},
			}
		}

		response := map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"items":      items,
		}
		if source != "" {
			response["source"] = source
		}
		return response, nil
	})

	addr := ":" + common.Getenv("PORT", "8080")
	return http.ListenAndServe(addr, mux)
}
