# CLAUDE.md — Marshal

Guidance for working on the alert/automation engine. For *writing rules*, read
[`README.md`](README.md) instead — that is the rule-authoring reference and the
only configuration interface this service has.

## What it is

A Go service that subscribes to MQTT topics, evaluates one-field conditions,
and either raises alerts or issues commands. It has no UI, no HTTP API, and no
database. Rules in YAML are the entire configuration surface.

Two distinct jobs, deliberately kept separate in the code:

- **Alerting is level-triggered** — a condition must *hold* for
  `duration_minutes` before it fires. Driven by the periodic sweep.
- **Actuation is edge-triggered** — a condition *transition* publishes a
  command immediately. Driven by inbound messages.

Conflating the two is the most likely way to break this service.

## Package layout

| Package | Responsibility |
|---|---|
| `cmd/marshal/` | Wiring: config load, MQTT connect, OnConnect hook, signal handling |
| `internal/config/` | Rule parsing and validation |
| `internal/evaluator/` | Condition evaluation (one field, one operator) |
| `internal/state/` | Per-rule state: when a condition became true, last alert |
| `internal/alerter/` | Level-triggered alert emission |
| `internal/actuator/` | Edge-triggered commands + ownership arbitration |
| `internal/engine/` | Message dispatch, sweep loop, heartbeat, reconnect |

`internal/engine/` holds the connection lifecycle — the exact code behind every
outage so far, and until 2026-08-24 the only package with no tests at all. It
now has `reconnect_test.go`. Extend it rather than assuming a change here is
safe — and note the lesson from 2026-08-25: a fake that models the paho
behaviour you *assume* will happily validate a fix that cannot work. When the
library's semantics are load-bearing, verify them against a real broker before
designing around them.

## MQTT reconnect semantics (paho) — read before touching the connection code

Three silent outages (2026-08-22, 2026-08-23, 2026-08-25) came from this area.
Library-level notes that apply beyond this service live in the global
`paho-mqtt-go-pitfalls` memory.

All three had the same signature: process up, container healthy, zero commands
published, because as far as the engine knew nothing had happened. The failure
is invisible unless you are looking for *absence*.

- **`IsConnected()` reports intent, not socket health.** With auto-reconnect
  enabled it keeps returning true while paho believes it owns a connection, so
  a client evicted by the broker reports healthy indefinitely. Prolonged
  inbound silence is the real signal — measured on the loopback probe, never
  on device traffic (see *The loopback probe and the watchdog* below).
- **`Disconnect()` is a trap in a recovery path.** It marks the session
  user-requested, which suppresses auto-reconnect, and it transitions status
  asynchronously. A `Connect()` immediately after it is rejected with
  `status can only transition to connecting from disconnected` — leaving the
  client offline with no retry scheduled.
- **A wedged client cannot be reliably rehabilitated.** Neither exported probe
  distinguishes `disconnecting` from `disconnected`, so waiting for the status
  to settle is not possible through the public API, and a second `Disconnect()`
  returns early once already disconnected. **Recovery builds a NEW client**
  (`SetClientFactory`) — a fresh one starts at `disconnected`, so its
  `Connect()` is always legal. Adopt it only once connected.
- **An abandoned client keeps connecting.** With `ConnectRetry`, `Connect()`
  goes on trying after the caller has stopped waiting on its token. A client
  that is built and then dropped on a timeout connects by itself when the
  broker next answers, under the same client ID as its successor, and the two
  evict each other on every reconnect. `connectMQTT` therefore calls
  `Disconnect()` on any client it does not return. Verified against a real
  broker on 2026-10-03.
- **`Connect()` does not replay subscriptions** under CleanSession. Every
  reconnect path must resubscribe explicitly. That is what `SubscribeAll` is
  for, and why `OnConnect` calls it.
- **Recovery must trigger on `!connected || stalled`.** Gating on `stalled`
  alone is unrecoverable by construction: `stalled` requires
  `connected == true`, so once a failed reconnect flips connected to false,
  nothing can ever fire again.

The double subscribe seen in the logs at startup is intentional, not a bug:
`connectMQTT` blocks until connected, so the first `OnConnect` can fire before
the resubscribe hook is wired. The explicit `SubscribeAll()` in `main` covers
that first connection; subscribing twice is idempotent at the broker.

### The loopback probe and the watchdog

**Device silence is not a liveness signal.** The seven devices subscribed today
routinely go four to twelve minutes without publishing. From v0.4.1 until
2026-10-03 the watchdog read that as a half-open socket, and autoheal restarted
the container 25–39 times a day — each restart discarding hold timers,
overrides and parking. Nothing was ever deaf; Zigbee2MQTT's own log showed no
publishes in any "stall" window.

So the engine proves its own connection instead of inferring it. Every
`watchdogInterval` (30s) it publishes a probe to `marshal/loopback/<host>-<pid>`
and consumes it in `handleMessage`, after the inbox and on the dispatcher, so a
probe that comes back proves the whole inbound chain. `lastInboundAt` moves on
any arrival; the thresholds are measured against it.

Three rules that are easy to break:

- **Only a real arrival may move `lastInboundAt`.** `reconnect()` used to reset
  the last-message time to space out retries. With the verdict evaluated every
  tick, that reset would make a wedged engine read `ok` after every attempt and
  autoheal would never fire. Retries are spaced by `lastRecoveryAt` instead.
- **The probe is not a device message.** It stays out of `messages_total` and
  `last_message_age_sec`, which exist to show what the house is doing.
- **The topic is per process.** Two overlapping instances must not keep each
  other looking alive.

### The heartbeat

The heartbeat exists to make *absence* alertable, and it only reports — the
verdict and recovery belong to the watchdog. Read its two ages together:
`last_message_age_sec` is the house, `loopback_age_sec` is the engine. A large
message age with a small loopback age is a quiet house. A loopback age growing
past a couple of watchdog intervals is the engine going deaf. `-1` on either
means it has *never* arrived; on the loopback that usually means subscriptions
did not survive a reconnect.

### Health file and the container healthcheck

`restart: unless-stopped` cannot help when a service fails without exiting —
the process stays up and the container reports healthy while the client is
deaf. So every watchdog tick writes its verdict to `/tmp/marshal-health`
(`ok` / `unhealthy`, write-then-rename so a reader never sees a partial write),
and the compose healthcheck reads it.

**The check requires both recent contents and a recent mtime.** Contents alone
would report the last verdict forever if the watchdog goroutine died; mtime
alone would miss a client that is up but receiving nothing. `Start()` seeds the
file so a normal boot is not read as a fault.

**The verdict is written every tick, not every heartbeat.** When only the
five-minute heartbeat wrote it, one `unhealthy` stood for five minutes — longer
than the healthcheck's three retries — so a single bad reading restarted the
container and self-recovery could never clear it in time.

**The health verdict uses `unhealthyTimeout`, not `stalled`.** They answer
different questions: `stallTimeout` (5m) gates *recovery*, where thrashing is
worse than waiting, while the health verdict (3m) must sour sooner or a deaf
engine reports healthy right up to the moment it repairs itself. Keying the
verdict on `stalled` (then 20m) is exactly why a twenty-minute deafness
reported `(healthy)` throughout on 2026-08-25.

Docker only *marks* a container unhealthy — it never restarts one. The
`autoheal` service in the services stack does that, scoped to containers
labelled `autoheal=true`, so a healthcheck added elsewhere in that stack stays
informational until it opts in.

Timing is deliberately forgiving: the engine recovers on its own now, so the
check tolerates roughly six minutes of sustained unhealth before a restart.
That is long enough for self-recovery to win and short enough that a genuine
wedge does not last the night. Tighten it and you will restart containers that
were about to fix themselves.

## Ownership arbitration (actuation)

When the engine drives a device that can also be driven by a human or by its own
on-board logic, exactly one owner must hold it at a time. Precedence, highest
first: **parked** > **manual override (TTL)** > **automation** > **device-local**.

Two non-obvious rules:

- **The manual override is TTL-based on purpose.** A permanent force-on flag is
  how a nightlight ends up on at noon. The TTL guarantees automation resumes by
  itself; the enable topic exists for handing control back sooner.
- **`override_topic` is usually the device's own `/set` topic**, so a command
  from *any* source counts as manual — not just the one integration you thought
  of. The engine therefore hears its own publishes, and must pre-register and
  consume those echoes or it overrides itself on its first command and goes
  permanently dormant. See `internal/actuator/selfov_test.go`.

Worked example with full reasoning: `docs/nightlight-automation.md`.

## Build and release

```bash
make test                          # fmt + vet + tests — run before committing
make build                         # linux binary, CGO_ENABLED=0
make docker-build                  # container image, local tag only
make docker-push VERSION=v0.2.1   # build + push to GHCR
make help                          # all targets
```

Releases run in CI (`.github/workflows/publish-containers.yml`): push a `v*`
tag and it builds multi-arch and pushes to GHCR, gated on the tests passing.

```bash
git tag -a v0.2.1 -m "..." && git push origin v0.2.1
# -> ghcr.io/trv-enterprises/marshal:0.2.1  (+ :latest when not a prerelease)
```

A hyphenated version is a prerelease and does NOT move `:latest` — the deploy
role defaults to latest, so an rc must not become what an unpinned host picks
up.

`make docker-push VERSION=...` still works for a local build; it pins
`linux/amd64` because the build host is arm64 and the services LXC is not.

Then deploy from `homelab-deploy`:

```bash
make deploy-marshal MARSHAL_VERSION=0.2.1
```

Pin an explicit tag for anything that must be reproducible — the role defaults
to `latest`, which cannot be rolled back to a known-good build. The registry
path is duplicated between this Makefile and the role's `vars/main.yml`; change
both together.

The live ruleset is **not** the `rules.yaml` in this directory. It lives in
`homelab-deploy/files/marshal/rules.yaml`; deploy with
`make deploy-marshal` from there. The local file is an example only.
