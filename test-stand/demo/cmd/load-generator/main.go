package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
)

type scenarioRoute struct {
	Path  string
	Query map[string]string
}

var scenarioRoutes = map[string]scenarioRoute{
	"browse":                  {Path: "/browse"},
	"inventory-http":          {Path: "/inventory", Query: map[string]string{"transport": "http"}},
	"inventory-grpc":          {Path: "/inventory", Query: map[string]string{"transport": "grpc"}},
	"checkout-http":           {Path: "/checkout", Query: map[string]string{"transport": "http", "rollback": "false"}},
	"checkout-grpc":           {Path: "/checkout", Query: map[string]string{"transport": "grpc", "rollback": "false"}},
	"checkout-rollback-grpc":  {Path: "/checkout", Query: map[string]string{"transport": "grpc", "rollback": "true"}},
	"checkout-http-error":     {Path: "/checkout", Query: map[string]string{"transport": "http", "fail": "true"}},
	"checkout-grpc-error":     {Path: "/checkout", Query: map[string]string{"transport": "grpc"}},
	"wide":                    {Path: "/wide"},
	"deep":                    {Path: "/deep"},
	"checkout-async-joined":   {Path: "/checkout", Query: map[string]string{"transport": "grpc", "notify": "joined"}},
	"checkout-async-detached": {Path: "/checkout", Query: map[string]string{"transport": "grpc", "notify": "detached"}},
	"catalog-db":              {Path: "/browse", Query: map[string]string{"source": "db"}},
	"browse-cached":           {Path: "/browse", Query: map[string]string{"cache": "true"}},
}

type weightedScenario struct {
	Name   string
	Weight int
}

type mix struct {
	scenarios []weightedScenario
	total     int
}

func parseMix(spec string) (mix, error) {
	var m mix
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return mix{}, fmt.Errorf("invalid MIX entry %q (want name:weight)", entry)
		}
		name := strings.TrimSpace(parts[0])
		if _, ok := scenarioRoutes[name]; !ok {
			return mix{}, fmt.Errorf("unknown scenario %q", name)
		}
		weight, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || weight <= 0 {
			return mix{}, fmt.Errorf("invalid weight in %q", entry)
		}
		m.scenarios = append(m.scenarios, weightedScenario{Name: name, Weight: weight})
		m.total += weight
	}
	if len(m.scenarios) == 0 {
		return mix{}, fmt.Errorf("MIX is empty")
	}
	return m, nil
}

func (m mix) pick(r *rand.Rand) string {
	roll := r.Intn(m.total)
	for _, s := range m.scenarios {
		if roll < s.Weight {
			return s.Name
		}
		roll -= s.Weight
	}
	return m.scenarios[len(m.scenarios)-1].Name
}

func parseDuration(name, fallback string) (time.Duration, error) {
	value := common.Getenv(name, fallback)
	if value == "" || value == "0" {
		return 0, nil
	}
	return time.ParseDuration(value)
}

func main() {
	service := "load-generator"
	logger := common.NewLogger(service)

	gatewayURL := common.Getenv("GATEWAY_URL", "http://gateway-api:8080")
	client := &http.Client{Timeout: 30 * time.Second}

	interval, err := parseDuration("INTERVAL", "1s")
	if err != nil {
		logger.Info(context.Background(), "invalid INTERVAL", map[string]any{"event": "config_invalid", "error": err.Error()})
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	startDelay, err := parseDuration("START_DELAY", "5s")
	if err != nil {
		logger.Info(context.Background(), "invalid START_DELAY", map[string]any{"event": "config_invalid", "error": err.Error()})
		return
	}
	duration, err := parseDuration("DURATION", "0")
	if err != nil {
		logger.Info(context.Background(), "invalid DURATION", map[string]any{"event": "config_invalid", "error": err.Error()})
		return
	}
	concurrency := common.GetenvInt("CONCURRENCY", 1)
	if concurrency < 1 {
		concurrency = 1
	}

	mixSpec := common.Getenv("MIX", "browse:1,inventory-grpc:1,checkout-grpc:1,checkout-rollback-grpc:1,checkout-http-error:1,checkout-grpc-error:1,wide:2,deep:2,checkout-async-joined:1,checkout-async-detached:1,catalog-db:1,browse-cached:1")
	m, err := parseMix(mixSpec)
	if err != nil {
		logger.Info(context.Background(), "invalid MIX", map[string]any{"event": "config_invalid", "error": err.Error()})
		return
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if duration > 0 {
		var cancel context.CancelFunc
		rootCtx, cancel = context.WithTimeout(rootCtx, duration)
		defer cancel()
	}

	logger.Info(rootCtx, "load-generator starting", map[string]any{
		"event":       "load_generator_started",
		"gateway_url": gatewayURL,
		"interval":    interval.String(),
		"concurrency": concurrency,
		"duration":    duration.String(),
		"mix":         mixSpec,
	})

	if startDelay > 0 {
		select {
		case <-time.After(startDelay):
		case <-rootCtx.Done():
			return
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				emit(rootCtx, logger, client, gatewayURL, m.pick(r), workerID)
				select {
				case <-rootCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	wg.Wait()

	logger.Info(context.Background(), "load-generator stopped", map[string]any{
		"event": "load_generator_stopped",
	})
}

func emit(ctx context.Context, logger *common.Logger, client *http.Client, gatewayURL, scenario string, workerID int) {
	route, ok := scenarioRoutes[scenario]
	if !ok {
		logger.Info(ctx, "unknown scenario", map[string]any{
			"event":     "emit_failed",
			"worker_id": workerID,
			"scenario":  scenario,
		})
		return
	}

	parsed, err := url.Parse(gatewayURL + route.Path)
	if err != nil {
		logger.Info(ctx, "invalid gateway url", map[string]any{
			"event":     "emit_failed",
			"worker_id": workerID,
			"scenario":  scenario,
			"error":     err.Error(),
		})
		return
	}
	q := parsed.Query()
	q.Set("scenario", scenario)
	for k, v := range route.Query {
		q.Set(k, v)
	}
	parsed.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		logger.Info(ctx, "emit request build failed", map[string]any{
			"event":     "emit_failed",
			"worker_id": workerID,
			"scenario":  scenario,
			"error":     err.Error(),
		})
		return
	}
	req.Header.Set("X-Request-ID", common.NewRequestID())

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logger.Info(ctx, "emit request failed", map[string]any{
			"event":     "emit_failed",
			"worker_id": workerID,
			"scenario":  scenario,
			"error":     err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	logger.Info(ctx, "emit completed", map[string]any{
		"event":       "emit_completed",
		"worker_id":   workerID,
		"scenario":    scenario,
		"status_code": resp.StatusCode,
	})
}
