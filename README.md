# otter

Experimental BEAM-like JavaScript runtime on QuickJS (via [rquickjs](https://crates.io/crates/rquickjs)).

Many isolated JS **processes** are multiplexed onto a small, fixed pool of OS
worker threads — one system thread can manage thousands of engines. Each
process owns its own QuickJS runtime (isolated heap, like a BEAM process) and
a mailbox; processes communicate only by message passing.

## Model

- **Process** = one `Runtime` + `Context` + mailbox. Never shares JS state.
- **Scheduler** = M worker threads pulling processes off a shared run queue.
  Each slice executes one pending QuickJS job, then the process is re-queued,
  parked, or finished. Processes migrate freely between worker threads.
- **Suspension** = `await recv()` on an empty mailbox parks the process until
  a message arrives. Nothing blocks an OS thread.
- **Isolation** = an uncaught error kills only its process; the exit code is
  non-zero if any process failed.

## Build & run

```sh
cargo build --release
otter [--workers N] script.js [more_scripts.js ...]
```

Every file on the command line starts as its own process (pids `0..n`); the
runtime exits when all processes (including spawned ones) have finished.

## JS API

| API | Description |
| --- | --- |
| `spawn(code)` | Start a new process from a source string, returns its pid. |
| `send(pid, value)` | Serialize `value` to JSON and deliver it to `pid`'s mailbox. Messages to dead pids are dropped silently. |
| `await recv(timeoutMs?)` | Suspend until a message arrives; resolves with the parsed value. With a `timeoutMs` it rejects with a `TimeoutError` if no message arrives in time (a message that races the deadline stays in the mailbox). |
| `await sleep(ms)` | Suspend the process for at least `ms` milliseconds; does not touch the mailbox. |
| `await yieldNow()` | Voluntarily give up the current slice and rejoin the back of the run queue. (Not named `yield` because that word is reserved inside async-function bodies, which is how top-level-await scripts are parsed.) |
| `self()` | The current process's pid. |
| `console.log/error` | Line-oriented output prefixed with the pid. |

Scripts support top-level `await`. Full API docs live in [`docs/`](docs/README.md).

## Examples

```sh
cargo run --release -- examples/ping_pong.js          # two processes volleying messages
cargo run --release -- --workers 1 examples/ring.js   # 1000 processes on ONE thread
cargo run --release -- examples/coop.js               # yieldNow()-driven interleaving
cargo run --release -- examples/timer.js              # sleep() and recv(timeoutMs) watchdog
```

## Limitations (v1)

- Cooperative scheduling: a long synchronous loop without `await`/`yieldNow()`
  monopolizes its worker thread until it suspends (no preemption yet).
- Timers are serviced by idle workers on a ~5 ms tick, so `sleep()`/`recv()`
  deadlines are accurate to within a tick or two, not to the millisecond.
- At most one outstanding suspension per process: calling `sleep()` while a
  `recv()` is pending (or vice versa) raises a `TypeError`. `recv()`/`yieldNow()`
  after a `sleep()` is fine once the sleep has completed.
- Messages must be JSON-serializable; functions, symbols and `undefined` are
  rejected.

## Tests

```sh
cargo test
```
