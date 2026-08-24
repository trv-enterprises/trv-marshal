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

`internal/engine/` is the **only package without tests**, and it holds the
connection lifecycle — the exact code that has caused every outage so far. Add
tests here rather than assuming a change is safe.

## MQTT reconnect semantics (paho) — read before touching the connection code

Two multi-hour silent outages (2026-08-22, 2026-08-23) came from this area.
Both had the same signature: process up, container healthy, zero commands
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

A container `restart: unless-stopped` does not help here: the process never
exits, it just goes deaf. A healthcheck keyed on the heartbeat's `connected`
field is what turns this into an automatic restart.

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
make test          # fmt + vet + tests with coverage — run before committing
make build         # linux binary, CGO_ENABLED=0
make docker-build  # container image
make help          # all targets
```

The deployed image is published to GHCR under the `trv-enterprises` org and
rolled out by the `alert-engine` Ansible role. Pin the tag via
`ALERT_ENGINE_VERSION` — the compose file previously hardcoded `:latest`, which
made rollback impossible.

The live ruleset is **not** the `rules.yaml` in this directory. It lives in
`homelab-deploy/files/alert-engine/rules.yaml`; deploy with
`make deploy-alert-engine` from there. The local file is an example only.

> `go.mod` declares `github.com/trv-homelab/sensor-alert-engine`, which predates
> the repo split and does not match this path. Nothing imports it so it is
> inert — correct it to the path-based form if you touch `go.mod`.
