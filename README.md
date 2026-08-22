# Alert / Automation Engine — Writing Rules

A rule watches one MQTT topic, tests one field, and then **alerts**, **acts**,
or both. This document is about writing those rules.

Rules live in YAML and are the **only** way to configure the engine — there is
no UI and no API. Edit the file, then deploy (or `SIGHUP` to reload in place).

- Deployment rules: `homelab-deploy/files/alert-engine/rules.yaml`
- Example rules: [`rules.yaml`](rules.yaml) in this directory

```bash
# from homelab-deploy
make deploy-alert-engine
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
  back now".
- **`state_topic`** — the engine publishes who is currently in charge:
  `automation`, `override`, or `parked`. Retained, so a display can read it on
  startup.

Precedence, highest first:

```
parked  >  override (TTL)  >  automation
```

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

## Reloading

`SIGHUP` reloads the rules file without restarting the process or losing rule
state (timers, override ownership):

```bash
docker kill -s HUP services-alert-engine-1
```

Topics are diffed on reload — new ones are subscribed, removed ones
unsubscribed, and state for deleted rules is dropped.

---

## Known limits

Worth knowing before you design around it:

- **One field per condition.** No `AND` / `OR`. Two conditions means two
  rules, and they cannot currently combine into a single decision.
- **No time-of-day or day-of-week.** "Only after sunset" is not expressible;
  the closest is testing a lux/illuminance field the device already reports.
- **No cooldown on actions** beyond `off_delay_seconds`.
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
