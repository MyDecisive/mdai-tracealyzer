package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/pb"
)

func runGateway(service string, logger *common.Logger) error {
	httpClient := common.NewTracedHTTPClient(service)
	grpcConn, inventoryClient, err := common.NewInventoryClient(service, common.Getenv("INVENTORY_GRPC_TARGET", "inventory-grpc-service:50051"))
	if err != nil {
		return err
	}
	defer grpcConn.Close()

	catalogURL := common.Getenv("CATALOG_URL", "http://catalog-api:8080")
	checkoutURL := common.Getenv("CHECKOUT_URL", "http://checkout-api:8080")
	inventoryHTTPURL := common.Getenv("INVENTORY_HTTP_URL", "http://inventory-http-api:8080")
	paymentsURL := common.Getenv("PAYMENTS_URL", "http://payments-api:8080")
	redisAddr := common.Getenv("REDIS_ADDR", "")

	var cache *common.Redis
	if redisAddr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cache, err = common.NewRedis(ctx, service, redisAddr)
		if err != nil {
			return err
		}
		defer cache.Close()
	}

	mux := http.NewServeMux()

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/browse", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		cached := r.URL.Query().Get("cache") == "true"
		source := r.URL.Query().Get("source")

		response := map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/browse",
		}

		if cached && cache != nil {
			val, hit, err := cache.Get(ctx, "catalog:items")
			if err == nil && hit {
				response["cache"] = "hit"
				response["catalog"] = json.RawMessage(val)
				return response, nil
			}
			response["cache"] = "miss"
		}

		extraQuery := map[string]string{}
		if source != "" {
			extraQuery["source"] = source
		}
		catalog, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, catalogURL+"/catalog", "gateway.fetch_catalog", meta, extraQuery, nil)
		if err != nil {
			return nil, err
		}
		response["catalog"] = catalog
		if source != "" {
			response["source"] = source
		}

		if cached && cache != nil {
			if encoded, err := json.Marshal(catalog); err == nil {
				_ = cache.Set(ctx, "catalog:items", string(encoded))
			}
		}

		return response, nil
	})

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/inventory", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		transport := r.URL.Query().Get("transport")
		if transport == "" {
			transport = "http"
		}

		catalog, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, catalogURL+"/catalog", "gateway.fetch_catalog", meta, nil, nil)
		if err != nil {
			return nil, err
		}

		var inventory map[string]any
		if transport == "grpc" {
			grpcCtx := common.WithRequestMetadata(ctx, meta.RequestID, meta.Scenario)
			reply, err := inventoryClient.CheckAvailability(grpcCtx, &pb.InventoryRequest{
				RequestId: meta.RequestID,
				Scenario:  meta.Scenario,
				SKU:       "coffee",
				Quantity:  1,
			})
			if err != nil {
				return nil, err
			}
			logger.Info(ctx, "downstream call completed", map[string]any{
				"event":       "downstream_call_completed",
				"request_id":  meta.RequestID,
				"scenario":    meta.Scenario,
				"operation":   "gateway.fetch_inventory",
				"transport":   "grpc",
				"grpc.method": "InventoryService/CheckAvailability",
			})
			inventory = map[string]any{
				"request_id": meta.RequestID,
				"scenario":   meta.Scenario,
				"success":    reply.Success,
				"status":     reply.Status,
				"warehouse":  reply.Warehouse,
				"available":  reply.Available,
			}
		} else {
			inventory, err = common.JSONRequest(ctx, httpClient, logger, http.MethodGet, inventoryHTTPURL+"/availability", "gateway.fetch_inventory", meta, map[string]string{
				"transport": transport,
			}, nil)
			if err != nil {
				return nil, err
			}
		}

		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/inventory",
			"transport":  transport,
			"catalog":    catalog,
			"inventory":  inventory,
		}, nil
	})

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/checkout", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		transport := r.URL.Query().Get("transport")
		if transport == "" {
			transport = "http"
		}
		rollback := r.URL.Query().Get("rollback") == "true"
		fail := r.URL.Query().Get("fail") == "true"
		notify := r.URL.Query().Get("notify")

		checkoutQuery := map[string]string{
			"transport": transport,
			"rollback":  common.BoolString(rollback),
			"fail":      common.BoolString(fail),
		}
		if notify != "" {
			checkoutQuery["notify"] = notify
		}
		checkout, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, checkoutURL+"/checkout", "gateway.start_checkout", meta, checkoutQuery, nil)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/checkout",
			"transport":  transport,
			"rollback":   rollback,
			"fail":       fail,
			"notify":     notify,
			"checkout":   checkout,
		}, nil
	})

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/wide", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		calls := map[string]func(context.Context) (any, error){
			"catalog": func(ctx context.Context) (any, error) {
				return common.JSONRequest(ctx, httpClient, logger, http.MethodGet, catalogURL+"/catalog", "gateway.fetch_catalog", meta, nil, nil)
			},
			"inventory_http": func(ctx context.Context) (any, error) {
				return common.JSONRequest(ctx, httpClient, logger, http.MethodGet, inventoryHTTPURL+"/availability", "gateway.fetch_inventory_http", meta, map[string]string{
					"transport": "http",
				}, nil)
			},
			"inventory_grpc": func(ctx context.Context) (any, error) {
				grpcCtx := common.WithRequestMetadata(ctx, meta.RequestID, meta.Scenario)
				reply, err := inventoryClient.CheckAvailability(grpcCtx, &pb.InventoryRequest{
					RequestId: meta.RequestID,
					Scenario:  meta.Scenario,
					SKU:       "coffee",
					Quantity:  1,
				})
				if err != nil {
					return nil, err
				}
				logger.Info(ctx, "downstream call completed", map[string]any{
					"event":       "downstream_call_completed",
					"request_id":  meta.RequestID,
					"scenario":    meta.Scenario,
					"operation":   "gateway.fetch_inventory_grpc",
					"transport":   "grpc",
					"grpc.method": "InventoryService/CheckAvailability",
				})
				return map[string]any{
					"success":   reply.Success,
					"status":    reply.Status,
					"warehouse": reply.Warehouse,
					"available": reply.Available,
				}, nil
			},
			"payments": func(ctx context.Context) (any, error) {
				return common.JSONRequest(ctx, httpClient, logger, http.MethodGet, paymentsURL+"/authorize", "gateway.preauthorize_payment", meta, nil, nil)
			},
		}

		results := common.ParallelCalls(ctx, calls)
		response := map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/wide",
		}
		for name, result := range results {
			if result.Err != nil {
				return nil, result.Err
			}
			response[name] = result.Value
		}
		return response, nil
	})

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/deep", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		checkout, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, checkoutURL+"/deep", "gateway.deep_chain", meta, nil, nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/deep",
			"checkout":   checkout,
		}, nil
	})

	addr := ":" + common.Getenv("PORT", "8080")
	return http.ListenAndServe(addr, mux)
}
