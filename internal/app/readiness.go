package app

import (
	"slices"
	"sync"
	"sync/atomic"
)

// Readiness tracks a fixed set of named gates. Ready returns true once every
// gate passed to NewReadiness has been marked via Mark. Marking an unknown or
// already-marked gate is a no-op. The read path is lock-free so the readiness
// probe handler can poll it cheaply.
type Readiness struct {
	ready atomic.Bool

	mu      sync.Mutex
	gates   map[string]bool
	pending int
}

// NewReadiness creates a Readiness that waits for the given gates. With no
// gates, it is ready immediately.
func NewReadiness(gates ...string) *Readiness {
	r := &Readiness{
		gates:   make(map[string]bool, len(gates)),
		pending: len(gates),
	}
	for _, g := range gates {
		r.gates[g] = false
	}
	if r.pending == 0 {
		r.ready.Store(true)
	}
	return r
}

// Mark records that a gate has satisfied its precondition. Unknown or
// already-marked gates are ignored.
func (r *Readiness) Mark(gate string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	marked, known := r.gates[gate]
	if !known || marked {
		return
	}
	r.gates[gate] = true
	r.pending--
	if r.pending == 0 {
		r.ready.Store(true)
	}
}

// Ready reports whether every declared gate has been marked.
func (r *Readiness) Ready() bool {
	return r.ready.Load()
}

// Pending returns the set of gates not yet marked, sorted alphabetically.
// Intended for low-frequency diagnostic paths (startup logs, shutdown checks);
// the hot probe path uses Ready. Returns nil when every gate is marked.
func (r *Readiness) Pending() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending == 0 {
		return nil
	}
	out := make([]string, 0, r.pending)
	for gate, marked := range r.gates {
		if !marked {
			out = append(out, gate)
		}
	}
	slices.Sort(out)
	return out
}
