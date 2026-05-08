package run

import "context"

// Component lifecycle. Start spawns long-running goroutines rooted on
// context.WithCancel(context.Background()) and returns; the goroutines must
// observe a component-owned stop signal and escalate any post-Start failure
// via host.Fatal. Shutdown signals, waits, drains, and closes; it is
// idempotent and safe to call without a prior Start. Shutdown honours ctx
// unless the implementation owns destructive in-flight work whose
// interruption would lose data.
type Component interface {
	Name() string
	Start(ctx context.Context, host Host) error
	Shutdown(ctx context.Context) error
}
