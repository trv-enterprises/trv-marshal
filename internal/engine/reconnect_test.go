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

	// connectFails makes every Connect() fail, modelling a replacement client
	// that cannot reach the broker.
	connectFails error
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
	if f.connectFails != nil {
		return &fakeToken{err: f.connectFails}
	}
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

// The regression test for the outage. A wedged paho client can refuse
// Connect() with "status can only transition to connecting from disconnected",
// and no exported method reliably drives it back to a state that accepts one.
// Recovery must therefore build a REPLACEMENT client, not retry the old one.
func TestReconnectReplacesTheClientRatherThanRetryingIt(t *testing.T) {
	wedged := &fakeClient{
		open:                    true,
		neverSettles:            true, // never returns to a connectable state
		connectErrWhileSettling: errors.New("status can only transition to connecting from disconnected"),
	}
	fresh := &fakeClient{}

	e := newTestEngine(wedged)
	e.SetClientFactory(func() mqtt.Client { return fresh })

	e.reconnect()

	if got := wedged.connectCount(); got != 0 {
		t.Fatalf("Connect() called %d times on the wedged client, want 0 -- it can never succeed", got)
	}
	if !fresh.isOpen() {
		t.Fatal("replacement client was not connected")
	}
	e.mu.Lock()
	adopted := e.client == mqtt.Client(fresh)
	e.mu.Unlock()
	if !adopted {
		t.Fatal("engine did not adopt the replacement client")
	}
}

// A replacement that fails to connect must not be adopted: the engine should
// keep pointing at something known and try again on the next heartbeat.
func TestReconnectKeepsOldClientWhenReplacementFails(t *testing.T) {
	old := &fakeClient{open: true}
	bad := &fakeClient{connectFails: errors.New("dial tcp: connection refused")}

	e := newTestEngine(old)
	e.SetClientFactory(func() mqtt.Client { return bad })

	e.reconnect()

	e.mu.Lock()
	stillOld := e.client == mqtt.Client(old)
	e.mu.Unlock()
	if !stillOld {
		t.Fatal("engine adopted a client that never connected")
	}
}

// A factory that cannot build a client at all must not panic or adopt nil.
func TestReconnectSurvivesNilFromFactory(t *testing.T) {
	old := &fakeClient{open: true}
	e := newTestEngine(old)
	e.SetClientFactory(func() mqtt.Client { return nil })

	e.reconnect() // must not panic

	e.mu.Lock()
	stillOld := e.client == mqtt.Client(old)
	e.mu.Unlock()
	if !stillOld {
		t.Fatal("engine dropped its client for a nil replacement")
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
	fresh := &fakeClient{}
	e := newTestEngine(c)
	e.SetClientFactory(func() mqtt.Client { return fresh })

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

	if !fresh.isOpen() {
		t.Fatal("engine did not recover from a plain disconnect")
	}
}

// reconnect() resets lastMsgAt so a recovery that itself fails silently does
// not immediately re-trigger the watchdog on the very next tick.
func TestReconnectResetsMessageAgeSoRetriesAreSpaced(t *testing.T) {
	c := &fakeClient{open: true, settleAfter: 1}
	e := newTestEngine(c)
	e.SetClientFactory(func() mqtt.Client { return &fakeClient{} })

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
	fresh := &fakeClient{}
	fresh.subscribe = func() mqtt.Token { return &fakeToken{err: errors.New("subscribe refused")} }

	cfg := &config.Config{
		AlertTopic: "sensors/alerts",
		Rules: []config.Rule{
			{Name: "r", Topic: "zigbee2mqtt/thing"},
		},
	}
	e := New(cfg, c, "")
	e.SetClientFactory(func() mqtt.Client { return fresh })

	// Should not panic. The replacement connects, then resubscribe fails --
	// the client stays connected but deaf, which the next heartbeat catches.
	e.reconnect()

	if !fresh.isOpen() {
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

// The health verdict must sour well before the recovery threshold. Keying it
// on `stalled` (20m) is why a twenty-minute deafness reported healthy the whole
// time on 2026-08-25 -- the container looked fine while receiving nothing.
func TestHealthThresholdIsTighterThanTheRecoveryThreshold(t *testing.T) {
	if unhealthyTimeout >= stallTimeout {
		t.Fatalf("unhealthyTimeout (%s) must be shorter than stallTimeout (%s), "+
			"otherwise a deaf engine reports healthy right up to the moment it recovers",
			unhealthyTimeout, stallTimeout)
	}
}

// The verdict the heartbeat writes, evaluated exactly as heartbeatLoop does.
func healthVerdict(connected bool, lastMsgAt, startedAt time.Time) bool {
	silent := !lastMsgAt.IsZero() && time.Since(lastMsgAt) > unhealthyTimeout
	neverHeard := lastMsgAt.IsZero() && time.Since(startedAt) > unhealthyTimeout
	return connected && !silent && !neverHeard
}

func TestHealthVerdictCases(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name      string
		connected bool
		lastMsgAt time.Time
		startedAt time.Time
		want      bool
	}{
		{"connected and recently heard", true, now.Add(-time.Minute), now.Add(-time.Hour), true},
		{"disconnected", false, now.Add(-time.Minute), now.Add(-time.Hour), false},
		{"connected but silent past the threshold", true, now.Add(-unhealthyTimeout - time.Minute), now.Add(-time.Hour), false},
		{"silent but still inside the threshold", true, now.Add(-unhealthyTimeout + time.Minute), now.Add(-time.Hour), true},
		// A fresh process that has heard nothing yet is not a fault: a quiet
		// house is quiet, and failing here would fail its own boot.
		{"never heard anything, just started", true, time.Time{}, now.Add(-time.Minute), true},
		{"never heard anything, up a long time", true, time.Time{}, now.Add(-time.Hour), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := healthVerdict(tc.connected, tc.lastMsgAt, tc.startedAt); got != tc.want {
				t.Fatalf("verdict = %v, want %v", got, tc.want)
			}
		})
	}
}
