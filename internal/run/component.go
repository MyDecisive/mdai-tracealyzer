package run

import "context"

// Component is a long-lived subsystem managed by Supervisor.
//
// Start blocks until the component fails or ctx is cancelled. Returning nil
// means clean exit on cancellation; returning a non-nil error is fatal and
// triggers shutdown of the entire supervisor. A Start that completes its
// work and returns nil before ctx cancels (a fire-and-exit probe) is valid;
// the supervisor keeps running for the remaining components.
//
// Stop is invoked by the supervisor after Start has unblocked, in reverse
// registration order, under a fresh context bounded by the shutdown grace.
// Stop must be safe to call when Start has already cleaned up on its own.
type Component interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}
