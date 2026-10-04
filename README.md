# Marshal — Writing Rules

A rule watches one MQTT topic, tests one field, and then **alerts**, **acts**,
or both. This document is about writing those rules, and about the one thing
in the file that is not a rule: the `reassert` list.

Rules live in YAML and are the **only** way to configure the engine — there is
no UI and no API. Edit the file, then deploy (or `SIGHUP` to reload in place).

- Deployment rules: `homelab-deploy/files/marshal/rules.yaml`
- Example rules: [`rules.yaml`](rules.yaml) in this directory

```bash
# from homelab-deploy
make deploy-marshal
```

---

## Anatomy of a rule

Every rule needs a `name`, a `topic`, and a `condition`. It then needs an
`alert:` block, an `action:` block, or both.

```yaml
rules:
  - name: office_too_warm          # unique; used in alert payloads
    topic: "zigbee2mqtt/Office"    # MQTT topic to watch
    condition:
      field: "temperature"
      operator: "gt"
      value: 76
    alert:                          # notify
      duration_minutes: 30
      severity: "warning"
      message: "Office above 76°F for {duration}"
```

---

## Conditions

One field, one comparison.

| Operator | Meaning | Applies to |
|---|---|---|
| `eq` | equals | numbers, strings, booleans |
| `ne` | not equals | numbers, strings, booleans |
| `gt` / `ge` | greater than / or equal | numbers |
| `lt` / `le` | less than / or equal | numbers |

**Nested fields** use dot-notation — `field: "before.severity"` reads
`data["before"]["severity"]`.

**Types coerce.** `value: 76` matches whether the device publishes `76`,
`76.0`, or `76.4` (as a `gt`). You do not need to match the device's exact
numeric type.

---

## Alerts — level-triggered

An alert fires once the condition has been **continuously true** for
`duration_minutes`, and resolves automatically when it stops being true.

```yaml
    alert:
      duration_minutes: 30      # how long it must hold (0 = immediately)
      repeat_minutes: 60        # re-notify interval (0 = never repeat)
      severity: "warning"       # info | warning | critical
      message: "Garage door open for {duration}"
```

Alerts publish to the global `alert_topic` (`sensors/alerts`) as JSON with a
`type` of `new`, `repeat`, or `resolved`.

Add `active: false` to switch an alert off without deleting it. This is a
static setting: whether an alert that *does* fire reaches a phone is decided
downstream (mutes, quiet hours), not here.

**Message variables:** `{duration}` `{device}` `{name}` `{field}` `{value}`

---

## Actions — edge-triggered

An action fires the **moment** the condition becomes true — no duration
threshold — and can publish to any MQTT topic.

```yaml
    action:
      topic: "zigbee2mqtt/Hallway Light/set"
      payload: '{"state":"ON","brightness":120}'
      off_payload: '{"state":"OFF"}'
      off_delay_seconds: 120     # wait this long after the condition clears
```

`off_topic` is only needed if the off-command goes somewhere other than
`topic`.

### Gate — an extra check before turning on

An optional `gate` is a second condition, evaluated against the **same
payload** as the trigger, consulted **only on the rising edge**:

```yaml
    action:
      gate: {field: "illuminance", operator: "lt", value: 15}
```

A failed gate suppresses the on-command; nothing else changes. The gate never
generates edges, never blocks the off path, and never cancels a pending off —
so gating a light on its own illuminance sensor cannot oscillate: once the
light is on under a passing gate it runs a normal motion cycle, and a bright
reading can only stop the *next* turn-on. If the gate field is missing from a
payload the gate fails open (with a warning), on the theory that a nightlight
losing its light sensor should degrade to a plain motion light, not go dark.

### Manual override

Without this, an automation rule and a human fight over the device. These
settings decide who owns it:

```yaml
      override_topic: "zigbee2mqtt/Hallway Light/set"
      override_ttl_minutes: 30
      enable_topic: "automation/hallway/enable"
      state_topic: "automation/hallway/owner"
```

- **`override_topic`** — a command seen here means a human took over. Pointing
  it at the device's own `/set` topic makes *any* source count (HomeKit, a
  dashboard, `mosquitto_pub`), not just one app. The engine ignores the echo
  of its own commands.
- **`override_ttl_minutes`** — how long the human keeps control before
  automation resumes on its own. A TTL rather than a permanent flag, because a
  "force on" nobody remembers to clear is how a light ends up on at noon.
- **`enable_topic`** — publish `false` to park automation entirely; `true`
  resumes it *and* clears any active override, so it doubles as "give control
  back now". Two payload forms are accepted: a bare string (`on`, `off`,
  `true`, `1`, …) or a JSON object `{"enable": true}` / `{"enable": "off"}`
  for publishers that can only emit JSON objects. Anything else — `{}`
  included — is logged and ignored, never guessed.
- **`state_topic`** — the engine publishes who is currently in charge:
  `automation`, `override`, or `parked`. Retained, so a display can read it on
  startup.

Precedence, highest first:

```
parked  >  override (TTL)  >  automation
```

### Active / inactive

"Tell me about motion here, but leave the light alone." An inactive action
issues no new on-commands. The rule's alert is not affected.

```yaml
      active: true                                  # the default; omit it
      active_topic: "automation/hallway/active"     # optional runtime switch
```

- **`active`** — the configured default. `false` ships the action switched
  off. A rule whose alert and action are both inactive does nothing at all.
- **`active_topic`** — publish `true` / `false` here to switch at runtime
  (also `on`, `off`, `active`, `inactive`, or `{"active": false}`). Publish it
  **retained**: the broker replays it on every subscribe, which is how the
  setting survives an engine restart. Clear the retained value (an empty
  retained payload) to hand the decision back to `active`.

Inactive is deliberately not the same as parked:

| | inactive | parked |
|---|---|---|
| New on-commands | no | no |
| A cycle already running | finishes: the scheduled off still fires | dropped: the light stays on |
| A manual override in progress | kept when switched back to active | cleared by enable `true` |
| Scope | one rule, via its own `active_topic` | every rule sharing the `enable_topic` |

The two are independent. Enable `true` never reactivates an inactive action,
and neither affects who owns the device, so `state_topic` does not report
inactive.

---

## Both at once

The same condition can notify *and* act, which is the point of one engine
rather than two:

```yaml
  - name: freezer_warm
    topic: "zigbee2mqtt/Freezer"
    condition: {field: "temperature", operator: "gt", value: 10}
    alert:
      duration_minutes: 15
      severity: "critical"
      message: "Freezer above 10°F for {duration}"
    action:
      topic: "notify/siren/set"
      payload: '{"state":"ON"}'
```

---

## Reassert — say it again on a schedule

Some settings a device forgets without telling anyone. A rule cannot help:
nothing changes on any topic when it happens, so there is no condition to
test. `reassert` is a separate, top-level list of messages that Marshal
publishes at startup, after every reload, and then again on an interval.

```yaml
reassert:
  - name: park_local_rule_hall
    topic: "zigbee2mqtt/bridge/request/device/write"
    payload: '{"id":"night-light-hall","endpoint":1,"cluster":64512,"options":{"manufacturerCode":4877},"payload":{"coldDownTime":5,"localRoutinTime":0,"luxThreshold":100}}'
    every_minutes: 60
```

- **`name`** — unique among reassert entries; it appears in the log.
- **`topic`** — where to publish. No wildcards: nothing is subscribed.
- **`payload`** — sent verbatim, QoS 1, not retained.
- **`every_minutes`** — the interval, at least 1. This is how long a setting
  can stay wrong before it is put back.

The example is what the list was built for. A Third Reality night light can
run motion→light by itself; writing `localRoutinTime: 0` parks that so it
cannot race the rule that owns the light. The device does not answer reads on
that cluster, so the setting cannot be checked, and it has reverted with no
power cycle or rejoin to explain it. The symptom is easy to miss: with the
rule's action active the light comes on either way. It shows only when the
action is switched inactive and the light comes on regardless.

A publish that fails is tried again within 30 seconds rather than after the
whole interval, and nothing is attempted while the broker connection is down.
Each success is logged as `reasserted`. Marshal does not read any reply, so a
write the device rejects is not noticed here.

Choosing the interval: shorter means less time wrong, but each publish may be
a write to a device's flash. Hourly is a reasonable default.

---

## Reloading

`SIGHUP` reloads the rules file without restarting the process or losing rule
state (timers, override ownership):

```bash
docker kill -s HUP services-marshal-1
```

Topics are diffed on reload — new ones are subscribed, removed ones
unsubscribed, and state for deleted rules is dropped. Every `reassert` entry
is published again within 30 seconds, changed or not.

---

## Known limits

Worth knowing before you design around it:

- **One field per condition.** No `AND` / `OR`. Two conditions means two
  rules, and they cannot currently combine into a single decision.
- **No time-of-day or day-of-week.** "Only after sunset" is not expressible;
  the closest is testing a lux/illuminance field the device already reports.
- **No cooldown on actions** beyond `off_delay_seconds`.
- **`active_topic` is per rule.** There is no group form; use `enable_topic`
  to switch several rules together.
- **`reassert` is blind.** It publishes on a timer and reads no reply; it
  cannot tell whether a device took the write or had drifted at all.
- **One alert topic** for every rule.
- **YAML only** — no UI, no API, no runtime rule entry.

The `AND`/`OR` gap is the most likely to bite. It is also the natural place a
real expression language (cel-go) would slot in later, replacing the
three-part `condition` with a single `expression:` field — which is why the
YAML shape was kept rather than adopting a rule-engine DSL.

---

## Backward compatibility

Rules written before the `action:` path existed used flat alert fields:

```yaml
  - name: old_style
    topic: "zigbee2mqtt/Door"
    condition: {field: "contact", operator: "eq", value: false}
    duration_minutes: 30        # flat — no `alert:` wrapper
    repeat_minutes: 60
    severity: "warning"
    message: "Door open for {duration}"
```

These still load unchanged; they are normalized into an `alert:` block at load
time. New rules should use the nested form.

---

## See also

- [`docs/nightlight-automation.md`](../../docs/nightlight-automation.md) — a
  worked example: motion → light, with device-local rule arbitration and the
  HomeKit surface.
