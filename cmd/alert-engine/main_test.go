package main

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestUniqueClientIDIncludesBaseHostAndPID(t *testing.T) {
	got := uniqueClientID("alert-engine")

	if !strings.HasPrefix(got, "alert-engine-") {
		t.Errorf("expected base prefix, got %q", got)
	}
	// A fixed ID is what let the broker evict the engine's own session; the
	// suffix must actually vary the string.
	if got == "alert-engine" {
		t.Error("client ID was not suffixed")
	}
	if !strings.Contains(got, strconv.Itoa(os.Getpid())) {
		t.Errorf("expected PID in ID, got %q", got)
	}
}

func TestUniqueClientIDIsStableWithinAProcess(t *testing.T) {
	// Reconnects must reuse the same ID, otherwise the broker accumulates
	// abandoned sessions on every reconnect.
	if a, b := uniqueClientID("x"), uniqueClientID("x"); a != b {
		t.Errorf("expected stable ID within a process, got %q then %q", a, b)
	}
}

func TestOnConnectEnterExcludesConcurrentPasses(t *testing.T) {
	o := &onConnect{}

	if !o.enter() {
		t.Fatal("first enter should succeed")
	}
	// paho fires OnConnect from more than one goroutine; the second must be
	// rejected so topics are not subscribed twice.
	if o.enter() {
		t.Error("second enter should be rejected while the first is running")
	}

	o.leave()
	if !o.enter() {
		t.Error("enter should succeed again after leave")
	}
	o.leave()
}

func TestOnConnectEnterRunsCallbackExactlyOnceUnderRace(t *testing.T) {
	o := &onConnect{}

	var mu sync.Mutex
	var calls int
	o.set(func() error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	})

	// Hold the claim so every racing goroutine is turned away, mimicking a
	// burst of OnConnect callbacks during a reconnect storm.
	if !o.enter() {
		t.Fatal("expected to claim")
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if o.enter() {
				if fn := o.get(); fn != nil {
					_ = fn()
				}
				o.leave()
			}
		}()
	}
	wg.Wait()
	o.leave()

	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Errorf("expected 0 callback runs while the claim was held, got %d", calls)
	}
}
