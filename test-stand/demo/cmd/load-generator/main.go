package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/scenarios"
)

const (
	workers    = 8
	defaultRPS = 10

	loadProfileFull   = "full"
	loadProfileDemo   = "demo"
	loadProfileCustom = "custom"

	demoMixSpec = "browse:50,inventory-grpc:20,checkout-grpc:15,wide:5,deep:5,checkout-async-joined:3,checkout-grpc-error:1,checkout-http-error:1"
)

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
		if err := scenarios.Validate(name); err != nil {
			return mix{}, err
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

func fullMixSpec() string {
	names := scenarios.Names()
	parts := make([]string, 0, len(names))
	for _, n := range names {
		weight := 1
		if n == "wide" || n == "deep" {
			weight = 2
		}
		parts = append(parts, fmt.Sprintf("%s:%d", n, weight))
	}
	return strings.Join(parts, ",")
}

func resolveMixSpec(profile, customSpec string) (string, error) {
	switch profile {
	case loadProfileFull:
		return fullMixSpec(), nil
	case loadProfileDemo:
		return demoMixSpec, nil
	case loadProfileCustom:
		if strings.TrimSpace(customSpec) == "" {
			return "", fmt.Errorf("LOAD_PROFILE=custom requires MIX")
		}
		return customSpec, nil
	default:
		return "", fmt.Errorf("invalid LOAD_PROFILE %q (want full, demo, or custom)", profile)
	}
}

func applyErrorRate(m mix, pct int) (mix, error) {
	if pct < 0 || pct > 100 {
		return mix{}, fmt.Errorf("ERROR_RATE_PCT must be 0..100, got %d", pct)
	}
	var errSum, nonErrSum int
	for _, s := range m.scenarios {
		def, _ := scenarios.Get(s.Name)
		if def.IsError {
			errSum += s.Weight
		} else {
			nonErrSum += s.Weight
		}
	}
	if errSum == 0 && pct > 0 {
		return mix{}, fmt.Errorf("ERROR_RATE_PCT=%d but mix has no error scenarios", pct)
	}
	if nonErrSum == 0 && pct < 100 {
		return mix{}, fmt.Errorf("ERROR_RATE_PCT=%d but mix has no non-error scenarios", pct)
	}
	scaled := mix{scenarios: make([]weightedScenario, 0, len(m.scenarios))}
	for _, s := range m.scenarios {
		def, _ := scenarios.Get(s.Name)
		var weight int
		if def.IsError {
			weight = s.Weight * pct * nonErrSum
		} else {
			weight = s.Weight * (100 - pct) * errSum
		}
		if weight == 0 {
			continue
		}
		scaled.scenarios = append(scaled.scenarios, weightedScenario{Name: s.Name, Weight: weight})
		scaled.total += weight
	}
	if scaled.total == 0 {
		return mix{}, fmt.Errorf("ERROR_RATE_PCT=%d produced an empty mix", pct)
	}
	return scaled, nil
}

func parseDuration(name, fallback string) (time.Duration, error) {
	value := common.Getenv(name, fallback)
	if value == "" || value == "0" {
		return 0, nil
	}
	return time.ParseDuration(value)
}

func main() {
	once := flag.String("once", "", "emit a single scenario by name and exit")
	listFlag := flag.Bool("list", false, "list scenario names and exit")
	flag.Parse()

	if *listFlag {
		for _, n := range scenarios.Names() {
			fmt.Println(n)
		}
		return
	}

	service := "load-generator"
	logger := common.NewLogger(service, common.VerbosityFromEnv())
	common.StartHeartbeat(logger)
	gatewayURL := common.Getenv("GATEWAY_URL", "http://gateway-api:8080")
	client := &http.Client{Timeout: 30 * time.Second}

	if *once != "" {
		os.Exit(runOnce(logger, client, gatewayURL, *once))
	}

	if err := runLoop(logger, client, gatewayURL); err != nil {
		logger.Error(context.Background(), "load-generator error", map[string]any{"event": "load_generator_error", "error": err.Error()})
		os.Exit(1)
	}
}

func runOnce(logger *common.Logger, client *http.Client, gatewayURL, name string) int {
	scenario, ok := scenarios.Get(name)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", name)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := emit(ctx, logger, client, gatewayURL, scenario, 0); err != nil {
		fmt.Fprintf(os.Stderr, "emit failed: %v\n", err)
		return 1
	}
	return 0
}

func runLoop(logger *common.Logger, client *http.Client, gatewayURL string) error {
	rps := common.GetenvInt("RPS", defaultRPS)
	if rps <= 0 {
		return fmt.Errorf("RPS must be > 0, got %d", rps)
	}
	effectiveWorkers := workers
	if rps < effectiveWorkers {
		effectiveWorkers = rps
	}
	interval := time.Second * time.Duration(effectiveWorkers) / time.Duration(rps)

	profile := common.Getenv("LOAD_PROFILE", loadProfileFull)
	mixSpec, err := resolveMixSpec(profile, os.Getenv("MIX"))
	if err != nil {
		return err
	}
	m, err := parseMix(mixSpec)
	if err != nil {
		return fmt.Errorf("invalid MIX: %w", err)
	}
	if value := strings.TrimSpace(os.Getenv("ERROR_RATE_PCT")); value != "" {
		pct, parseErr := strconv.Atoi(value)
		if parseErr != nil {
			return fmt.Errorf("invalid ERROR_RATE_PCT %q: %w", value, parseErr)
		}
		m, err = applyErrorRate(m, pct)
		if err != nil {
			return err
		}
	}

	startDelay, err := parseDuration("START_DELAY", "5s")
	if err != nil {
		return fmt.Errorf("invalid START_DELAY: %w", err)
	}
	duration, err := parseDuration("DURATION", "0")
	if err != nil {
		return fmt.Errorf("invalid DURATION: %w", err)
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
		"rps":         rps,
		"workers":     effectiveWorkers,
		"interval":    interval.String(),
		"profile":     profile,
		"duration":    duration.String(),
		"mix":         mixSpec,
	})

	if startDelay > 0 {
		select {
		case <-time.After(startDelay):
		case <-rootCtx.Done():
			return nil
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < effectiveWorkers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				name := m.pick(r)
				scenario, ok := scenarios.Get(name)
				if !ok {
					continue
				}
				if err := emit(rootCtx, logger, client, gatewayURL, scenario, workerID); err != nil && rootCtx.Err() == nil {
					logger.Warn(rootCtx, "emit failed", map[string]any{
						"event":     "emit_failed",
						"worker_id": workerID,
						"scenario":  name,
						"error":     err.Error(),
					})
				}
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
	return nil
}

func emit(ctx context.Context, logger *common.Logger, client *http.Client, gatewayURL string, scenario scenarios.Scenario, workerID int) error {
	target, err := scenario.URL(gatewayURL)
	if err != nil {
		return fmt.Errorf("build url: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-Request-ID", common.NewRequestID())

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer resp.Body.Close()

	logger.Info(ctx, "emit completed", map[string]any{
		"event":       "emit_completed",
		"worker_id":   workerID,
		"scenario":    scenario.Name,
		"status_code": resp.StatusCode,
	})
	return nil
}
