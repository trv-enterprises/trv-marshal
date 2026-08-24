package engine

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/config"
)

// fakeClient models the parts of paho's Client that the recovery path touches,
// including the behaviour that caused the 2026-08-23 outage: Disconnect()
// returns before the status has settled, and Connect() rejects any attempt made
// during that window.
type fakeClient struct {
	mqtt.Client

	mu sync.Mutex

	open      bool // status == connected
	settling  bool // status == disconnecting (Disconnect returned, teardown ongoing)
	connectN  int
	subscribe func() mqtt.Token

	// settleAfter is how many IsConnectionOpen polls elapse before the
	// teardown completes. Zero means it settles immediately.
	settleAfter int
	polls       int

	// neverSettles keeps the client stuck in `disconnecting` forever.
	neverSettles bool

	// connectErr is returned by Connect when non-nil and the client is not
	// settled -- mirroring errStatusMustBeDisconnected.
	connectErrWhileSettling error
}

func (f *fakeClient) IsConnectionOpen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls++
	if f.settling && !f.neverSettles && f.polls >= f.settleAfter {
		f.settling = false
	}
	return f.open
}

func (f *fakeClient) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open || f.settling
}

func (f *fakeClient) Disconnect(quiesce uint) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open = false
	f.settling = true
	f.polls = 0
}

func (f *fakeClient) Connect() mqtt.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connectN++
	if f.settling && f.connectErrWhileSettling != nil {
		return &fakeToken{err: f.connectErrWhileSettling}
	}
	f.open = true
	f.settling = false
	return &fakeToken{}
}

func (f *fakeClient) Subscribe(topic string, qos byte, cb mqtt.MessageHandler) mqtt.Token {
	if f.subscribe != nil {
		return f.subscribe()
	}
	return &fakeToken{}
}

func (f *fakeClient) connectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connectN
}

func (f *fakeClient) isOpen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open
}

type fakeToken struct {
	mqtt.Token
	err error
}

func (t *fakeToken) Wait() bool                     { return true }
func (t *fakeToken) WaitTimeout(time.Duration) bool { return true }
func (t *fakeToken) Error() error                   { return t.err }
func (t *fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func newTestEngine(c mqtt.Client) *Engine {
	return New(&config.Config{AlertTopic: "sensors/alerts"}, c, "")
}

// The regression test for the outage. paho's Disconnect() hands back control
// while the status is still `disconnecting`; a Connect() issued in that window
// is rejected outright. The old code took that rejection as terminal, so the
// engine stayed offline until someone restarted the container by hand.
func TestReconnectWaitsOutDisconnectBeforeConnecting(t *testing.T) {
	c := &fakeClient{
		open:                    true,
		settleAfter:             3, // a few polls of `disconnecting` first
		connectErrWhileSettling: errors.New("status can only transition to connecting from disconnected"),
	}
	e := newTestEngine(c)

	e.reconnect()

	if !c.isOpen() {
		t.Fatal("engine did not reconnect: client still closed")
	}
	if got := c.connectCount(); got != 1 {
		t.Fatalf("Connect() called %d times, want exactly 1 (a rejected attempt means the wait did not work)", got)
	}
}

// A teardown that never settles must not be treated as a permanent failure.
// reconnect() returns so the next heartbeat can try again, rather than
// blocking the heartbeat loop or burning a Connect() that is certain to fail.
func TestReconnectGivesUpWhenDisconnectNeverSettles(t *testing.T) {
	t.Parallel() // this one just waits out disconnectWait; don't serialise it

	c := &fakeClient{open: true, neverSettles: true}
	e := newTestEngine(c)

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.reconnect()
	}()

	select {
	case <-done:
	case <-time.After(disconnectWait + 5*time.Second):
		t.Fatal("reconnect() blocked past its own timeout")
	}

	if got := c.connectCount(); got != 0 {
		t.Fatalf("Connect() called %d times while still disconnecting, want 0", got)
	}
}

// The second half of the outage: recovery was gated on `stalled`, which is only
// ever true while connected. Once a failed attempt left the client
// disconnected, no condition could fire again. Recovery must also trigger on a
// plain loss of connection.
func TestUnhealthyTriggersOnDisconnectedNotOnlyStalled(t *testing.T) {
	// Disconnected and never stalled: lastMsgAt is recent, so the old
	// `stalled`-only gate would not have fired at all.
	c := &fakeClient{open: false, settleAfter: 1}
	e := newTestEngine(c)

	e.mu.Lock()
	e.lastMsgAt = time.Now()
	e.mu.Unlock()

	connected := c.IsConnected()
	stalled := connected && time.Since(time.Now()) > stallTimeout

	if stalled {
		t.Fatal("precondition: this case must not be stalled")
	}
	if stalled || !connected {
		e.reconnect()
	} else {
		t.Fatal("recovery condition did not fire for a disconnected client")
	}

	if !c.isOpen() {
		t.Fatal("engine did not recover from a plain disconnect")
	}
}

// reconnect() resets lastMsgAt so a recovery that itself fails silently does
// not immediately re-trigger the watchdog on the very next tick.
func TestReconnectResetsMessageAgeSoRetriesAreSpaced(t *testing.T) {
	c := &fakeClient{open: true, settleAfter: 1}
	e := newTestEngine(c)

	e.mu.Lock()
	e.lastMsgAt = time.Now().Add(-2 * stallTimeout)
	e.mu.Unlock()

	e.reconnect()

	e.mu.Lock()
	age := time.Since(e.lastMsgAt)
	e.mu.Unlock()

	if age > time.Minute {
		t.Fatalf("lastMsgAt not reset (age %s); watchdog would re-fire immediately", age)
	}
}

// A resubscribe failure must not be reported as a successful recovery: the
// client would be connected but deaf, which is the same silent failure the
// heartbeat exists to catch.
func TestReconnectReportsResubscribeFailure(t *testing.T) {
	c := &fakeClient{open: true, settleAfter: 1}
	c.subscribe = func() mqtt.Token { return &fakeToken{err: errors.New("subscribe refused")} }

	cfg := &config.Config{
		AlertTopic: "sensors/alerts",
		Rules: []config.Rule{
			{Name: "r", Topic: "zigbee2mqtt/thing"},
		},
	}
	e := New(cfg, c, "")

	// Should not panic and should leave the client connected; the failure is
	// surfaced through the log, and the next heartbeat retries.
	e.reconnect()

	if !c.isOpen() {
		t.Fatal("client should still be connected after a resubscribe failure")
	}
}

// The health file is what the container healthcheck reads, so its contents
// must track the heartbeat's verdict exactly -- an "ok" written while the
// engine is deaf would defeat the whole mechanism.
func TestWriteHealthReflectsVerdict(t *testing.T) {
	dir := t.TempDir()
	orig := healthPath
	healthPath = dir + "/health"
	defer func() { healthPath = orig }()

	e := newTestEngine(&fakeClient{open: true})

	e.writeHealth(true)
	if got := readFile(t, healthPath); got != "ok\n" {
		t.Fatalf("healthy verdict wrote %q, want %q", got, "ok\n")
	}

	e.writeHealth(false)
	if got := readFile(t, healthPath); got != "unhealthy\n" {
		t.Fatalf("unhealthy verdict wrote %q, want %q", got, "unhealthy\n")
	}

	// No leftover temp file: the healthcheck globs nothing, but a stray
	// .tmp would mean the rename did not happen.
	if _, err := os.Stat(healthPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind; write-then-rename did not complete")
	}
}

// A health file that cannot be written must not take down an otherwise
// working engine -- the healthcheck reads a missing file as unhealthy, which
// is the safe direction, but the process must survive.
func TestWriteHealthSurvivesUnwritablePath(t *testing.T) {
	orig := healthPath
	healthPath = "/nonexistent-dir-that-should-not-exist/health"
	defer func() { healthPath = orig }()

	e := newTestEngine(&fakeClient{open: true})
	e.writeHealth(true) // must not panic
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading %s: %v", p, err)
	}
	return string(b)
}
