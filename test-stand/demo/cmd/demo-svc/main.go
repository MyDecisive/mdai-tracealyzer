package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
)

type roleFunc func(service string, logger *common.Logger) error

var roles = map[string]roleFunc{
	"catalog":        runCatalog,
	"checkout":       runCheckout,
	"gateway":        runGateway,
	"payments":       runPayments,
	"inventory-http": runInventoryHTTP,
	"inventory-grpc": runInventoryGRPC,
	"notifier":       runNotifier,
}

func main() {
	role := common.Getenv("DEMO_ROLE", "")
	if role == "" {
		fmt.Fprintln(os.Stderr, "DEMO_ROLE is required")
		os.Exit(1)
	}
	fn, ok := roles[role]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown DEMO_ROLE %q\n", role)
		os.Exit(1)
	}

	service := common.Getenv("DD_SERVICE", role)
	stopTracer := common.StartTracer(service)
	defer stopTracer()

	logger := common.NewLogger(service)
	if err := fn(service, logger); err != nil {
		logger.Info(context.Background(), "role exited", map[string]any{
			"event": "role_exited",
			"role":  role,
			"error": err.Error(),
		})
		os.Exit(1)
	}
}
