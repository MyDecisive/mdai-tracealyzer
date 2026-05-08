package run

import "context"

// Component lifecycle. Start launches background work that does not depend
// on the Start ctx and returns; post-Start failures escalate via host.Fatal.
// Shutdown owns the component-specific stop mechanism — context cancel,
// http.Server.Shutdown, grpc.Server.GracefulStop, or equivalent — then waits
// for the background work to exit. Shutdown is idempotent and safe to call
// without a prior Start. Shutdown honours ctx unless the implementation owns
// destructive in-flight work whose interruption would lose data.
type Component interface {
	Name() string
	Start(ctx context.Context, host Host) error
	Shutdown(ctx context.Context) error
}
