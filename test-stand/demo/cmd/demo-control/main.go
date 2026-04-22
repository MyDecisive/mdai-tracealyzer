package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/vika/global_ratio_mode/test-stand/demo/internal/common"
)

type emitRoute struct {
	Path     string
	Query    map[string]string
	Scenario string
}

type emitRequest struct {
	Scenario string `json:"scenario"`
}

func main() {
	service := "demo-control"

	logger := common.NewLogger(service)
	client := &http.Client{Timeout: 5 * time.Second}
	gatewayURL := common.Getenv("GATEWAY_URL", "http://gateway-api:8080")

	scenarios := map[string]emitRoute{
		"browse":                 {Path: "/browse", Query: map[string]string{}, Scenario: "browse"},
		"inventory-http":         {Path: "/inventory", Query: map[string]string{"transport": "http"}, Scenario: "inventory-http"},
		"inventory-grpc":         {Path: "/inventory", Query: map[string]string{"transport": "grpc"}, Scenario: "inventory-grpc"},
		"checkout-http":          {Path: "/checkout", Query: map[string]string{"transport": "http", "rollback": "false"}, Scenario: "checkout-http"},
		"checkout-grpc":          {Path: "/checkout", Query: map[string]string{"transport": "grpc", "rollback": "false"}, Scenario: "checkout-grpc"},
		"checkout-rollback-grpc": {Path: "/checkout", Query: map[string]string{"transport": "grpc", "rollback": "true"}, Scenario: "checkout-rollback-grpc"},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/emit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		var payload emitRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": false,
				"message": "invalid JSON body",
			})
			return
		}

		route, ok := scenarios[payload.Scenario]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success":             false,
				"message":             "unsupported scenario",
				"supported_scenarios": []string{"browse", "inventory-http", "inventory-grpc", "checkout-http", "checkout-grpc", "checkout-rollback-grpc"},
			})
			return
		}

		requestID := common.NewRequestID()
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, gatewayURL+route.Path, nil)
		if err != nil {
			log.Fatal(err)
		}

		query := req.URL.Query()
		query.Set("scenario", route.Scenario)
		for key, value := range route.Query {
			query.Set(key, value)
		}
		req.URL.RawQuery = query.Encode()
		req.Header.Set("X-Request-ID", requestID)

		resp, err := client.Do(req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			logger.Info(context.Background(), "scenario emission failed", map[string]any{
				"event":      "emit_failed",
				"request_id": requestID,
				"scenario":   route.Scenario,
				"outcome":    "error",
			})
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"request_id": requestID,
				"scenario":   route.Scenario,
				"success":    false,
				"message":    err.Error(),
			})
			return
		}
		defer resp.Body.Close()

		var summary map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
			logger.Info(context.Background(), "scenario emission failed", map[string]any{
				"event":      "emit_failed",
				"request_id": requestID,
				"scenario":   route.Scenario,
				"outcome":    "error",
			})
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"request_id": requestID,
				"scenario":   route.Scenario,
				"success":    false,
				"message":    "failed to decode gateway response",
			})
			return
		}

		logger.Info(context.Background(), "scenario emission completed", map[string]any{
			"event":      "emit_completed",
			"request_id": requestID,
			"scenario":   route.Scenario,
			"outcome":    "success",
		})

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"request_id": requestID,
				"scenario":   route.Scenario,
				"success":    false,
				"summary":    summary,
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"request_id": requestID,
			"scenario":   route.Scenario,
			"success":    true,
			"summary":    summary,
		})
	})

	addr := ":" + common.Getenv("PORT", "8080")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
