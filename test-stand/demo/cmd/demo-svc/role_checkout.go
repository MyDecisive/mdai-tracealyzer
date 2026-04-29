package main

import (
	"context"
	"net/http"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/pb"
)

func runCheckout(service string, logger *common.Logger) error {
	httpClient := common.NewTracedHTTPClient(service)
	grpcConn, inventoryClient, err := common.NewInventoryClient(service, common.Getenv("INVENTORY_GRPC_TARGET", "inventory-grpc-service:50051"))
	if err != nil {
		return err
	}
	defer grpcConn.Close()

	paymentsURL := common.Getenv("PAYMENTS_URL", "http://payments-api:8080")
	inventoryHTTPURL := common.Getenv("INVENTORY_HTTP_URL", "http://inventory-http-api:8080")
	kafkaBrokers := common.Getenv("KAFKA_BROKERS", "")
	joinedTopic := common.Getenv("KAFKA_TOPIC_JOINED", "checkout-completed-joined")
	detachedTopic := common.Getenv("KAFKA_TOPIC_DETACHED", "checkout-completed-detached")

	var joinedProducer, detachedProducer *common.KafkaProducer
	if kafkaBrokers != "" {
		joinedProducer = common.NewKafkaProducer(service, kafkaBrokers, joinedTopic)
		detachedProducer = common.NewKafkaProducer(service, kafkaBrokers, detachedTopic)
		defer joinedProducer.Close()
		defer detachedProducer.Close()
	}

	mux := http.NewServeMux()
	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/checkout", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		transport := r.URL.Query().Get("transport")
		if transport == "" {
			transport = "http"
		}
		rollback := r.URL.Query().Get("rollback") == "true"
		fail := r.URL.Query().Get("fail") == "true"
		notify := r.URL.Query().Get("notify")

		if rollback && transport != "grpc" {
			return nil, &common.HTTPError{Status: http.StatusBadRequest, Message: "rollback scenarios require grpc transport"}
		}

		var inventory map[string]any
		if transport == "grpc" {
			grpcCtx := common.WithRequestMetadata(ctx, meta.RequestID, meta.Scenario)
			reply, err := inventoryClient.ReserveItems(grpcCtx, &pb.InventoryRequest{
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
				"operation":   "checkout.reserve_inventory",
				"transport":   "grpc",
				"grpc.method": "InventoryService/ReserveItems",
			})
			inventory = map[string]any{
				"request_id":     reply.RequestId,
				"scenario":       reply.Scenario,
				"success":        reply.Success,
				"status":         reply.Status,
				"reservation_id": reply.ReservationId,
				"warehouse":      reply.Warehouse,
				"available":      reply.Available,
			}
		} else {
			inventory, err = common.JSONRequest(ctx, httpClient, logger, http.MethodPost, inventoryHTTPURL+"/reserve", "checkout.reserve_inventory", meta, map[string]string{
				"transport": transport,
			}, map[string]any{
				"sku":      "coffee",
				"quantity": 1,
			})
			if err != nil {
				return nil, err
			}
		}

		payment, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, paymentsURL+"/authorize", "checkout.authorize_payment", meta, map[string]string{
			"rollback": common.BoolString(rollback),
			"fail":     common.BoolString(fail),
		}, nil)
		if err != nil {
			return nil, err
		}

		response := map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"transport":  transport,
			"inventory":  inventory,
			"payment":    payment,
			"status":     "completed",
		}

		if payment["status"] == "declined" {
			response["status"] = "declined"
			if transport == "grpc" {
				grpcCtx := common.WithRequestMetadata(ctx, meta.RequestID, meta.Scenario)
				reply, err := inventoryClient.ReleaseReservation(grpcCtx, &pb.ReleaseReservationRequest{
					RequestId:     meta.RequestID,
					Scenario:      meta.Scenario,
					ReservationId: inventory["reservation_id"].(string),
				})
				if err != nil {
					return nil, err
				}
				logger.Info(ctx, "downstream call completed", map[string]any{
					"event":       "downstream_call_completed",
					"request_id":  meta.RequestID,
					"scenario":    meta.Scenario,
					"operation":   "checkout.release_inventory",
					"transport":   "grpc",
					"grpc.method": "InventoryService/ReleaseReservation",
				})
				response["rollback"] = map[string]any{
					"request_id":     reply.RequestId,
					"scenario":       reply.Scenario,
					"success":        reply.Success,
					"status":         reply.Status,
					"reservation_id": reply.ReservationId,
				}
				response["status"] = "rolled_back"
			}
		}

		if notify != "" && response["status"] == "completed" {
			producer := joinedProducer
			if notify == "detached" {
				producer = detachedProducer
			}
			if producer == nil {
				return nil, &common.HTTPError{Status: http.StatusServiceUnavailable, Message: "kafka not configured; KAFKA_BROKERS env is empty"}
			}
			event := map[string]any{
				"request_id": meta.RequestID,
				"scenario":   meta.Scenario,
				"sku":        "coffee",
				"quantity":   1,
			}
			if err := producer.Publish(ctx, meta.RequestID, event); err != nil {
				return nil, err
			}
			response["notified"] = notify
		}

		return response, nil
	})

	common.RegisterJSONRoute(mux, service, logger, http.MethodGet, "/deep", func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		inventory, err := common.JSONRequest(ctx, httpClient, logger, http.MethodGet, inventoryHTTPURL+"/deep-check", "checkout.deep_inventory_check", meta, nil, nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/deep",
			"inventory":  inventory,
		}, nil
	})

	addr := ":" + common.Getenv("PORT", "8080")
	return http.ListenAndServe(addr, mux)
}
