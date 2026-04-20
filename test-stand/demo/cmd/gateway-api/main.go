package main

import (
	"context"
	"log"
	"net/http"

	"github.com/vika/global_ratio_mode/test-stand/demo/internal/common"
	"github.com/vika/global_ratio_mode/test-stand/demo/internal/pb"
)

func main() {
	service := common.Getenv("DD_SERVICE", "gateway-api")
	stopTracer := common.StartTracer(service)
	defer stopTracer()

	logger := common.NewLogger(service)
	httpClient := common.NewTracedHTTPClient(service)
	grpcConn, inventoryClient, err := common.NewInventoryClient(service, common.Getenv("INVENTORY_GRPC_TARGET", "inventory-grpc-service:50051"))
	if err != nil {
		log.Fatal(err)
	}
	defer grpcConn.Close()

	catalogURL := common.Getenv("CATALOG_URL", "http://catalog-api:8080")
	checkoutURL := common.Getenv("CHECKOUT_URL", "http://checkout-api:8080")
	inventoryHTTPURL := common.Getenv("INVENTORY_HTTP_URL", "http://inventory-http-api:8080")

	mux := http.NewServeMux()

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/browse", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		catalog, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, catalogURL+"/catalog", "gateway.fetch_catalog", meta, nil, nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/browse",
			"catalog":    catalog,
		}, nil
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

		checkout, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, checkoutURL+"/checkout", "gateway.start_checkout", meta, map[string]string{
			"transport": transport,
			"rollback":  common.BoolString(rollback),
		}, nil)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/checkout",
			"transport":  transport,
			"rollback":   rollback,
			"checkout":   checkout,
		}, nil
	})

	addr := ":" + common.Getenv("PORT", "8080")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
