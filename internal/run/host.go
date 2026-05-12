package run

// Host lets a Component escalate post-Start fatals to the supervisor. Fatal
// is non-blocking and shutdown-safe: it never waits on the supervisor and is
// safe to call after the shutdown phase has begun.
type Host interface {
	Fatal(component string, err error)
}
