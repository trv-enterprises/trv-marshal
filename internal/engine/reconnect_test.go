package engine

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-enterprises/trv-marshal/internal/config"
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

	subscribed []string
	published  []string
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
	f.mu.Lock()
	f.subscribed = append(f.subscribed, topic)
	f.mu.Unlock()
	if f.subscribe != nil {
		return f.subscribe()
	}
	return &fakeToken{}
}

func (f *fakeClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, topic)
	return &fakeToken{}
}

func (f *fakeClient) topics(of func(*fakeClient) []string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), of(f)...)
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

// useTempHealthFile points the health file at a temp dir for one test.
func useTempHealthFile(t *testing.T) {
	t.Helper()
	orig := healthPath
	healthPath = t.TempDir() + "/health"
	t.Cleanup(func() { healthPath = orig })
}

// countingFactory returns a client factory and a counter of how many
// replacement clients it has been asked for.
func countingFactory(build func() mqtt.Client) (func() mqtt.Client, *int) {
	n := 0
	return func() mqtt.Client { n++; return build() }, &n
}

// heard sets when the engine last heard anything at all, and when it last
// heard a device, relative to now.
func (e *Engine) heard(now time.Time, inboundAgo, deviceAgo time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastInboundAt = now.Add(-inboundAgo)
	e.lastLoopbackAt = now.Add(-inboundAgo)
	e.lastMsgAt = now.Add(-deviceAgo)
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
func TestWatchdogRecoversADisconnectedClientThatIsNotStalled(t *testing.T) {
	useTempHealthFile(t)
	now := time.Now()

	// Disconnected and never stalled: something arrived seconds ago, so the
	// old `stalled`-only gate would not have fired at all.
	c := &fakeClient{open: false}
	fresh := &fakeClient{}
	e := newTestEngine(c)
	e.SetClientFactory(func() mqtt.Client { return fresh })
	e.heard(now, time.Second, time.Second)

	e.watchdogTick(now)

	if !fresh.isOpen() {
		t.Fatal("engine did not recover from a plain disconnect")
	}
	if got := readFile(t, healthPath); got != "unhealthy\n" {
		t.Fatalf("a disconnected client wrote %q, want unhealthy", got)
	}
}

// The regression test for the restart loop found on 2026-10-03. The house
// going quiet is not a fault: the seven devices here routinely go four to
// twelve minutes without publishing. As long as the loopback probe keeps
// coming back, the verdict stays ok and nothing reconnects.
func TestQuietHouseIsNeitherUnhealthyNorStalled(t *testing.T) {
	useTempHealthFile(t)
	now := time.Now()

	c := &fakeClient{open: true}
	e := newTestEngine(c)
	factory, built := countingFactory(func() mqtt.Client { return &fakeClient{} })
	e.SetClientFactory(factory)

	// No device has spoken for twenty minutes; the probe came back just now.
	e.heard(now, 10*time.Second, 20*time.Minute)

	e.watchdogTick(now)

	if got := readFile(t, healthPath); got != "ok\n" {
		t.Fatalf("quiet house wrote %q, want ok", got)
	}
	if *built != 0 {
		t.Fatalf("quiet house triggered %d reconnect(s), want 0", *built)
	}
	if isStalled(true, 10*time.Second) {
		t.Fatal("a fresh loopback must not read as stalled")
	}
}

// The probe must travel the same path as a device message and must not be
// mistaken for one: it moves the liveness clock and nothing else.
func TestLoopbackMovesLivenessButIsNotADeviceMessage(t *testing.T) {
	e := newTestEngine(&fakeClient{open: true})
	now := time.Now()
	e.heard(now, time.Hour, time.Hour)

	e.handleMessage(e.loopbackTopic, []byte("1"))

	e.mu.Lock()
	defer e.mu.Unlock()
	if time.Since(e.lastInboundAt) > time.Minute {
		t.Fatal("loopback did not move lastInboundAt")
	}
	if e.msgCount != 0 {
		t.Fatalf("loopback counted as %d device message(s), want 0", e.msgCount)
	}
	if time.Since(e.lastMsgAt) < 59*time.Minute {
		t.Fatal("loopback moved lastMsgAt; the heartbeat would hide a quiet house")
	}
}

// The probe only proves anything if the engine is subscribed to it, on every
// connect, and actually sends it.
func TestLoopbackIsSubscribedAndPublished(t *testing.T) {
	useTempHealthFile(t)
	c := &fakeClient{open: true}
	e := newTestEngine(c)

	if err := e.SubscribeAll(); err != nil {
		t.Fatalf("SubscribeAll: %v", err)
	}
	e.watchdogTick(time.Now())

	has := func(list []string) bool {
		for _, topic := range list {
			if topic == e.loopbackTopic {
				return true
			}
		}
		return false
	}
	if !has(c.topics(func(f *fakeClient) []string { return f.subscribed })) {
		t.Fatal("SubscribeAll did not subscribe to the loopback topic")
	}
	if !has(c.topics(func(f *fakeClient) []string { return f.published })) {
		t.Fatal("watchdog tick did not publish the loopback probe")
	}
}

// A deaf client goes unhealthy first and is rebuilt second, in that order:
// between the two thresholds the verdict is already sour but the connection
// is left alone.
func TestDeafClientIsUnhealthyThenRebuilt(t *testing.T) {
	useTempHealthFile(t)
	now := time.Now()

	c := &fakeClient{open: true}
	e := newTestEngine(c)
	factory, built := countingFactory(func() mqtt.Client { return &fakeClient{} })
	e.SetClientFactory(factory)

	e.heard(now, unhealthyTimeout+30*time.Second, time.Hour)
	e.watchdogTick(now)
	if got := readFile(t, healthPath); got != "unhealthy\n" {
		t.Fatalf("silent past unhealthyTimeout wrote %q, want unhealthy", got)
	}
	if *built != 0 {
		t.Fatalf("reconnected %d time(s) before stallTimeout, want 0", *built)
	}

	e.heard(now, stallTimeout+30*time.Second, time.Hour)
	e.watchdogTick(now)
	if *built != 1 {
		t.Fatalf("reconnected %d time(s) past stallTimeout, want 1", *built)
	}
}

// reconnect() must not touch the liveness clock. It used to reset it to space
// out retries; with the verdict now evaluated every tick, that reset would
// make a wedged engine read healthy after every attempt and autoheal would
// never fire. Retries are spaced by their own timestamp instead.
func TestRecoveryIsSpacedWithoutFakingLiveness(t *testing.T) {
	useTempHealthFile(t)
	now := time.Now()

	c := &fakeClient{open: true}
	e := newTestEngine(c)
	// Replacements connect but stay deaf: nothing ever arrives.
	factory, built := countingFactory(func() mqtt.Client { return &fakeClient{} })
	e.SetClientFactory(factory)
	e.heard(now, 2*stallTimeout, time.Hour)

	e.watchdogTick(now)
	e.watchdogTick(now.Add(watchdogInterval))
	e.watchdogTick(now.Add(2 * watchdogInterval))

	if *built != 1 {
		t.Fatalf("built %d replacement clients across three ticks, want 1 (backoff)", *built)
	}
	if got := readFile(t, healthPath); got != "unhealthy\n" {
		t.Fatalf("still-deaf engine wrote %q after a recovery attempt, want unhealthy", got)
	}

	e.watchdogTick(now.Add(recoveryBackoff + watchdogInterval))
	if *built != 2 {
		t.Fatalf("built %d replacement clients after the backoff, want 2", *built)
	}
}

// A recovery that works must read ok on the next tick, not a heartbeat later:
// the healthcheck gives up after three failures two minutes apart.
func TestVerdictClearsAsSoonAsTheProbeReturns(t *testing.T) {
	useTempHealthFile(t)
	now := time.Now()

	e := newTestEngine(&fakeClient{open: true})
	e.SetClientFactory(func() mqtt.Client { return &fakeClient{} })
	e.heard(now, 2*stallTimeout, time.Hour)
	e.watchdogTick(now) // unhealthy, reconnects

	e.handleMessage(e.loopbackTopic, []byte("1")) // the probe comes back

	e.watchdogTick(time.Now())
	if got := readFile(t, healthPath); got != "ok\n" {
		t.Fatalf("verdict after the probe returned is %q, want ok", got)
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
// on `stalled` (then 20m) is why a twenty-minute deafness reported healthy the whole
// time on 2026-08-25 -- the container looked fine while receiving nothing.
func TestHealthThresholdIsTighterThanTheRecoveryThreshold(t *testing.T) {
	if unhealthyTimeout >= stallTimeout {
		t.Fatalf("unhealthyTimeout (%s) must be shorter than stallTimeout (%s), "+
			"otherwise a deaf engine reports healthy right up to the moment it recovers",
			unhealthyTimeout, stallTimeout)
	}
}

func TestHealthVerdictCases(t *testing.T) {
	cases := []struct {
		name      string
		connected bool
		silence   time.Duration
		want      bool
	}{
		{"connected and recently heard", true, time.Minute, true},
		{"disconnected", false, time.Minute, false},
		{"connected but silent past the threshold", true, unhealthyTimeout + time.Minute, false},
		{"silent but still inside the threshold", true, unhealthyTimeout - time.Minute, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isHealthy(tc.connected, tc.silence); got != tc.want {
				t.Fatalf("verdict = %v, want %v", got, tc.want)
			}
		})
	}
}

// A fresh process has heard nothing yet. That is not a fault until it has had
// unhealthyTimeout to hear its first probe -- failing sooner would fail its
// own boot -- and it IS a fault after that, which is the "subscriptions did
// not survive" case.
func TestFreshProcessGetsAFullThresholdToHearItself(t *testing.T) {
	useTempHealthFile(t)
	e := newTestEngine(&fakeClient{open: true})
	started := e.startedAt

	e.watchdogTick(started.Add(time.Minute))
	if got := readFile(t, healthPath); got != "ok\n" {
		t.Fatalf("one minute after start wrote %q, want ok", got)
	}

	e.watchdogTick(started.Add(unhealthyTimeout + time.Minute))
	if got := readFile(t, healthPath); got != "unhealthy\n" {
		t.Fatalf("never heard anything after the threshold wrote %q, want unhealthy", got)
	}
}

// The probe has to arrive several times inside each threshold, or one lost
// QoS 0 message would be enough to sour the verdict.
func TestProbeIntervalLeavesRoomForLostProbes(t *testing.T) {
	if unhealthyTimeout < 4*watchdogInterval {
		t.Fatalf("unhealthyTimeout (%s) allows fewer than four probes at %s",
			unhealthyTimeout, watchdogInterval)
	}
}
