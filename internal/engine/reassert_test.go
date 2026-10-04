package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trv-enterprises/trv-marshal/internal/config"
)

const bridgeWrite = "zigbee2mqtt/bridge/request/device/write"

func reassertEngine(c *fakeClient, entries ...config.Reassert) *Engine {
	return New(&config.Config{AlertTopic: "sensors/alerts", Reassert: entries}, c, "")
}

// Everything goes out on the first pass, then each entry again only once its
// own interval has passed.
func TestReassertPublishesAtStartThenOnEachInterval(t *testing.T) {
	c := &fakeClient{open: true}
	e := reassertEngine(c,
		config.Reassert{Name: "hall", Topic: bridgeWrite, Payload: `{"id":"hall"}`, EveryMinutes: 60},
		config.Reassert{Name: "desk", Topic: "plug/desk/set", Payload: "ON", EveryMinutes: 10},
	)
	start := time.Now()

	e.reassert(start)
	if got := c.sentTo(bridgeWrite); len(got) != 1 || got[0] != `{"id":"hall"}` {
		t.Fatalf("first pass, hall: %v", got)
	}
	if got := c.sentTo("plug/desk/set"); len(got) != 1 || got[0] != "ON" {
		t.Fatalf("first pass, desk: %v", got)
	}

	// The loop ticks far more often than anything is due.
	for _, after := range []time.Duration{30 * time.Second, 5 * time.Minute, 9*time.Minute + 59*time.Second} {
		e.reassert(start.Add(after))
	}
	if n := len(c.topics(func(f *fakeClient) []string { return f.published })); n != 2 {
		t.Fatalf("published %d times before anything was due again, want 2", n)
	}

	e.reassert(start.Add(10 * time.Minute))
	if desk, hall := len(c.sentTo("plug/desk/set")), len(c.sentTo(bridgeWrite)); desk != 2 || hall != 1 {
		t.Fatalf("at 10m: desk %d (want 2), hall %d (want 1)", desk, hall)
	}
	e.reassert(start.Add(60 * time.Minute))
	if desk, hall := len(c.sentTo("plug/desk/set")), len(c.sentTo(bridgeWrite)); desk != 3 || hall != 2 {
		t.Fatalf("at 60m: desk %d (want 3), hall %d (want 2)", desk, hall)
	}
}

// The interval is how long drift may last. A publish that fails must be
// tried again on the next tick, not after another whole interval.
func TestReassertRetriesAFailedPublishOnTheNextTick(t *testing.T) {
	c := &fakeClient{open: true, publishErr: errors.New("broker said no")}
	e := reassertEngine(c, config.Reassert{Name: "hall", Topic: bridgeWrite, Payload: "p", EveryMinutes: 60})
	start := time.Now()

	e.reassert(start)
	e.reassert(start.Add(reassertTick))
	if n := len(c.sentTo(bridgeWrite)); n != 2 {
		t.Fatalf("attempted %d times while failing, want one per tick (2)", n)
	}

	c.mu.Lock()
	c.publishErr = nil
	c.mu.Unlock()
	e.reassert(start.Add(2 * reassertTick))
	e.reassert(start.Add(3 * reassertTick))
	if n := len(c.sentTo(bridgeWrite)); n != 3 {
		t.Fatalf("attempted %d times, want 3: once it succeeds it waits out the interval", n)
	}
}

// While the client is down nothing is attempted (each publish would wait out
// its timeout), and everything is still due when it comes back.
func TestReassertWaitsForTheConnection(t *testing.T) {
	c := &fakeClient{open: false}
	e := reassertEngine(c, config.Reassert{Name: "hall", Topic: bridgeWrite, Payload: "p", EveryMinutes: 60})
	start := time.Now()

	e.reassert(start)
	e.reassert(start.Add(reassertTick))
	if n := len(c.sentTo(bridgeWrite)); n != 0 {
		t.Fatalf("published %d times while disconnected", n)
	}
	c.mu.Lock()
	c.open = true
	c.mu.Unlock()
	e.reassert(start.Add(2 * reassertTick))
	if n := len(c.sentTo(bridgeWrite)); n != 1 {
		t.Fatalf("published %d times after reconnecting, want 1", n)
	}
}

// A recovery swaps in a new client. The schedule belongs to the engine, so
// it carries on, and goes out through the new client.
func TestReassertUsesTheReplacementClient(t *testing.T) {
	old, replacement := &fakeClient{open: true}, &fakeClient{open: true}
	e := reassertEngine(old, config.Reassert{Name: "hall", Topic: bridgeWrite, Payload: "p", EveryMinutes: 60})
	start := time.Now()
	e.reassert(start)
	e.swapClient(replacement)
	e.reassert(start.Add(time.Hour))
	if o, r := len(old.sentTo(bridgeWrite)), len(replacement.sentTo(bridgeWrite)); o != 1 || r != 1 {
		t.Fatalf("old client %d, replacement %d: want 1 and 1", o, r)
	}
}

// A reload publishes every entry again on the next tick, changed or not, and
// stops publishing one that was removed.
func TestReloadReassertsEverything(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	write := func(reassert string) {
		t.Helper()
		yaml := `
mqtt: {broker: "tcp://x:1883", client_id: "t"}
alert_topic: sensors/alerts
rules:
  - name: garage
    topic: "zigbee2mqtt/garage"
    condition: {field: contact, operator: eq, value: false}
    alert: {duration_minutes: 30, severity: warning, message: "open"}
` + reassert
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`
reassert:
  - {name: hall, topic: "` + bridgeWrite + `", payload: "old", every_minutes: 60}
  - {name: gone, topic: "plug/x/set", payload: "ON", every_minutes: 60}
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	c := &fakeClient{open: true}
	e := New(cfg, c, path)
	start := time.Now()
	e.reassert(start)
	if hall, gone := len(c.sentTo(bridgeWrite)), len(c.sentTo("plug/x/set")); hall != 1 || gone != 1 {
		t.Fatalf("before reload: hall %d, gone %d", hall, gone)
	}

	write(`
reassert:
  - {name: hall, topic: "` + bridgeWrite + `", payload: "new", every_minutes: 60}
`)
	if err := e.Reload(); err != nil {
		t.Fatal(err)
	}
	e.reassert(start.Add(reassertTick))
	if got := c.sentTo(bridgeWrite); len(got) != 2 || got[1] != "new" {
		t.Fatalf("after reload, hall: %v, want the new payload published at once", got)
	}
	e.reassert(start.Add(2 * time.Hour))
	if n := len(c.sentTo("plug/x/set")); n != 1 {
		t.Fatalf("removed entry was published %d times, want only the one before the reload", n)
	}
}

// With no entries the loop has nothing to do and touches nothing.
func TestReassertWithNothingConfigured(t *testing.T) {
	c := &fakeClient{open: true}
	e := newTestEngine(c)
	e.reassert(time.Now())
	if n := len(c.topics(func(f *fakeClient) []string { return f.published })); n != 0 {
		t.Fatalf("published %d messages with nothing configured", n)
	}
}
