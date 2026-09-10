package runtime

import "testing"

// TestErrorIsRetryable covers the distinction StartAll depends on.
//
// Enabled desks in StatusError used to be skipped unconditionally on every
// process start. That was right for a deterministic failure (a bad strategy
// config fails identically next boot) but wrong for the far more common case:
// a tick-error halt, which fires after just 10 consecutive failed ticks — only
// ~10 seconds of a dependency outage. Any engine restart or stale index long
// enough to notice halted every desk on the platform, and they all then stayed
// dark until an admin re-enabled each one by hand, long after the condition
// that stopped them had passed.
func TestErrorIsRetryable(t *testing.T) {
	t.Run("tick-error halt is retryable", func(t *testing.T) {
		msg := tickErrorHaltPrefix + "engine 404 Not Found: no mark price for underlying"
		if !errorIsRetryable(msg) {
			t.Fatalf("tick-error halt should be retried on the next start: %q", msg)
		}
	})

	t.Run("deterministic startup failures are not retryable", func(t *testing.T) {
		// These come from Start()'s own failure paths, not the tick loop: a
		// strategy that won't build, or persisted state that won't decode.
		// Retrying them just crash-loops and overwrites the recorded error.
		for _, msg := range []string{
			"investment must be a positive number",
			"spreadBps must be a positive number",
			"persisted state unreadable: unexpected EOF",
			"persisted state corrupt: invalid character 'x'",
		} {
			if errorIsRetryable(msg) {
				t.Fatalf("deterministic failure must not be retried: %q", msg)
			}
		}
	})

	t.Run("fails closed on an unrecognized message", func(t *testing.T) {
		// A future error path that doesn't carry the prefix must be skipped and
		// logged, never silently crash-looped.
		for _, msg := range []string{"", "some brand new failure mode"} {
			if errorIsRetryable(msg) {
				t.Fatalf("unrecognized error must default to non-retryable: %q", msg)
			}
		}
	})

	t.Run("prefix must match at the start, not anywhere", func(t *testing.T) {
		// Guards against a substring check: a config error that merely quotes
		// the halt text must not be mistaken for a real halt.
		msg := `config rejected: value looked like "` + tickErrorHaltPrefix + `"`
		if errorIsRetryable(msg) {
			t.Fatalf("prefix must anchor to the start of the message: %q", msg)
		}
	})
}
