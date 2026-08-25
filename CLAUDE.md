# CLAUDE.md — sensor-alert-engine

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
| `cmd/alert-engine/` | Wiring: config load, MQTT connect, OnConnect hook, signal handling |
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
  inbound silence is the real signal.
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

### The heartbeat

The heartbeat exists to make *absence* alertable. `messages_total` counts what
actually arrived from the broker — repeated heartbeats reporting zero while
devices are known to be publishing is a stall, and `last_message_age_sec` says
how long it has been going on. `-1` means no message has *ever* arrived, which
usually means subscriptions did not survive a reconnect.

### Health file and the container healthcheck

`restart: unless-stopped` cannot help when a service fails without exiting —
the process stays up and the container reports healthy while the client is
deaf. So every heartbeat also writes its verdict to `/tmp/alert-engine-health`
(`ok` / `unhealthy`, write-then-rename so a reader never sees a partial write),
and the compose healthcheck reads it.

**The check requires both recent contents and a recent mtime.** Contents alone
would report the last verdict forever if the heartbeat goroutine died; mtime
alone would miss a client that is up but receiving nothing. `Start()` seeds the
file so a normal boot is not read as a fault.

**The health verdict uses `unhealthyTimeout`, not `stalled`.** They answer
different questions: `stallTimeout` (20m) gates *recovery*, where thrashing is
worse than waiting, while the health verdict must sour sooner or a deaf engine
reports healthy right up to the moment it repairs itself. Keying the verdict on
`stalled` is exactly why a twenty-minute deafness reported `(healthy)`
throughout on 2026-08-25.

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
make docker-push VERSION=v0.2.0-rc.8   # build + push to GHCR
make help                          # all targets
```

There is no CI for this repo, so `docker-push` runs on your machine. It pins
`linux/amd64` deliberately: the build host is arm64 and the services LXC is
not, and an inherited platform produces an image the target cannot run. Git
tags carry a leading `v` and image tags do not — pass either, it is stripped.

Then deploy from `homelab-deploy`:

```bash
make deploy-alert-engine ALERT_ENGINE_VERSION=0.2.0-rc.8
```

Pin an explicit tag for anything that must be reproducible — the role defaults
to `latest`, which cannot be rolled back to a known-good build. The registry
path is duplicated between this Makefile and the role's `vars/main.yml`; change
both together.

The live ruleset is **not** the `rules.yaml` in this directory. It lives in
`homelab-deploy/files/alert-engine/rules.yaml`; deploy with
`make deploy-alert-engine` from there. The local file is an example only.
