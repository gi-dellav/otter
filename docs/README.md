# otter — documentation

**otter** is an experimental BEAM-like JavaScript runtime built on QuickJS
(via [rquickjs](https://crates.io/crates/rquickjs)). Each process owns an
isolated QuickJS heap and a mailbox; processes communicate only by message
passing, and a fixed pool of OS threads multiplexes arbitrarily many
processes.

## Guides

- **[API Reference](api.md)** — complete reference of the JavaScript API
  exposed to every process.

## Quick links

- [Project README](../README.md) — build, run, examples, limitations.
- [Examples](../examples) — `ping_pong.js`, `ring.js`, `coop.js`, `timer.js`.

## Process model summary

| Concept | Meaning |
|---------|---------|
| Process | One QuickJS `Runtime` + `Context` + mailbox. Never shares state. |
| PID | `u64` assigned at birth; `self()` returns it. |
| Mailbox | Unbounded, buffered. Messages are JSON strings. |
| Scheduler | M worker threads pull processes from a shared run queue. Each slice runs one pending QuickJS job, then re-queues, parks, or finishes. |
| Parking | A process with an empty mailbox is parked in a `HashMap` keyed by pid. A `send` to a parked pid re-queues it. |
| Sleeping | A process that called `sleep(ms)` is parked in a separate `sleeping` map, unreachable by `send`. A timer entry re-queues it when the deadline passes. |
| Timers | Serviced by idle workers every ~5 ms (best-effort, not real-time). |