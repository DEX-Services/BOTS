//go:build smoke

package engine

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestWSClientSmoke dials the real deployed matching engine and confirms the
// connect + event delivery path works end-to-end. Build-tagged out of normal
// `go test ./...` runs since it depends on network access to a live service;
// run explicitly with: go test -tags smoke ./internal/engine/ -run Smoke -v
func TestWSClientSmoke(t *testing.T) {
	secret := os.Getenv("WS_INTERNAL_SECRET_TEST")
	c, err := NewWSClient("https://matching-engine-w45t.onrender.com", secret)
	if err != nil {
		t.Fatalf("NewWSClient: %v", err)
	}
	connected := make(chan struct{}, 1)
	events := 0
	c.OnReconnect(func() {
		fmt.Println("RECONNECTED (or first connect)")
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	unsub := c.OnEvent(func(evt WSEvent) {
		events++
		if events <= 5 {
			acct := "<nil order>"
			if evt.Order != nil {
				acct = evt.Order.AccountID
			}
			fmt.Printf("EVENT #%d: type=%s symbol=%s account=%v\n", events, evt.Type, evt.Symbol, acct)
		}
	})
	defer unsub()
	c.Start()
	defer c.Stop()

	select {
	case <-connected:
		fmt.Println("connect confirmed")
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for connect")
	}

	time.Sleep(10 * time.Second)
	fmt.Printf("total events received: %d\n", events)
	if events == 0 {
		t.Fatal("expected at least one event from the live engine's active market-maker traffic")
	}
}
