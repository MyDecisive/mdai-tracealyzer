package main

import (
	"context"
	"net/http"
	"strconv"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
)

const (
	defaultDeepDepth = 8
	maxDeepDepth     = 32
)

func parseDepth(raw string) int {
	if raw == "" {
		return defaultDeepDepth
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return defaultDeepDepth
	}
	if v > maxDeepDepth {
		return maxDeepDepth
	}
	return v
}

func deepForwardHandler(client *http.Client, logger *common.Logger, nextURL, operation string) common.HandlerFunc {
	return func(ctx context.Context, r *http.Request, meta common.RequestMeta) (any, error) {
		depth := parseDepth(r.URL.Query().Get("depth"))
		response := map[string]any{
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      "/deep",
			"depth":      depth,
		}
		if depth <= 1 {
			return response, nil
		}
		next, err := common.JSONRequest(ctx, client, logger, http.MethodGet, nextURL+"/deep", operation, meta, map[string]string{
			"depth": strconv.Itoa(depth - 1),
		}, nil)
		if err != nil {
			return nil, err
		}
		response["next"] = next
		return response, nil
	}
}
