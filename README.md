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

## JSON-RPC control (RPC)

otter can expose a JSON-RPC 2.0 control socket over TCP, letting an external
tool drive the scheduler exactly like the in-JS API: `spawn`, `list`, `info`,
`kill`, `send`, `rename`, `count`, and `shutdown`. Enable it with
`--rpc-port` (the runtime then stays alive until you call `shutdown`); frames
are line-delimited, one request per line, so any CLI that speaks TCP works:

```sh
# start a runtime with a control socket on port 9000
otter --rpc-port 9000 script.js &

# drive it with netcat (or any line-based TCP client)
echo '{"jsonrpc":"2.0","id":1,"method":"list"}' | nc 127.0.0.1 9000
echo '{"jsonrpc":"2.0","id":2,"method":"spawn","params":{"code":"await recv();"}}' | nc 127.0.0.1 9000
echo '{"jsonrpc":"2.0","id":3,"method":"send","params":{"pid":1,"value":"hi"}}' | nc 127.0.0.1 9000
echo '{"jsonrpc":"2.0","id":4,"method":"shutdown"}' | nc 127.0.0.1 9000
```

Each request carries a numeric/string `id` and receives a matching response;
notifications (no `id`) get none, per the JSON-RPC 2.0 spec.

## JS API

| API | Description |
| --- | --- |
| `spawn(code, opts?)` | Start a new process from a source string, returns its pid. Optional `{ sandbox: { canSpawnAndKill: false } }` narrows the child's sandbox at birth. |
| `send(pid, value)` | Serialize `value` to JSON and deliver it to `pid`'s mailbox. Messages to dead pids are dropped silently. |
| `await recv(timeoutMs?)` | Suspend until a message arrives; resolves with the parsed value. With a `timeoutMs` it rejects with a `TimeoutError` if no message arrives in time (a message that races the deadline stays in the mailbox). |
| `await sleep(ms)` | Suspend the process for at least `ms` milliseconds; does not touch the mailbox. |
| `await yieldNow()` | Voluntarily give up the current slice and rejoin the back of the run queue. (Not named `yield` because that word is reserved inside async-function bodies, which is how top-level-await scripts are parsed.) |
| `self()` | The current process's pid. |
| `killProcess(pid)` | Request termination of a live process; returns `true` if `pid` is live. Best-effort: the process is reaped at its next scheduling boundary (parked/sleeping processes immediately). Killing an unknown pid returns `false`. |
| `listProcesses()` | Array of `{pid, name, status}` for every live process, sorted by pid. |
| `isProcessAlive(pid)` | `true` while `pid` is live. |
| `processInfo(pid)` | `{pid, name, status}` for a live pid, or `null`. |
| `processCount()` | Number of live processes. |
| `setName(name)` | Rename the current process; visible in `listProcesses()`/`processInfo()`. |
| `selfSandbox()` | Snapshot `{canSpawnAndKill}` of the current process's sandbox policy. |
| `restrictSandbox(policy?, opts?)` | Narrow a sandbox at runtime (self by default, or `{pid}`). Monotonic/irrevocable; returns the post-state. |
| `console.log/error` | Line-oriented output prefixed with the pid. |

Scripts support top-level `await`. Full API docs live in [`docs/`](docs/README.md).

## Examples

```sh
cargo run --release -- examples/ping_pong.js          # two processes volleying messages
cargo run --release -- --workers 1 examples/ring.js   # 1000 processes on ONE thread
cargo run --release -- examples/coop.js               # yieldNow()-driven interleaving
cargo run --release -- examples/timer.js              # sleep() and recv(timeoutMs) watchdog
cargo run --release -- examples/process_mgmt.js       # list, inspect, rename, and kill processes
cargo run --release -- examples/sandbox.js           # spawn confined children, self-restrict, no-escalation
```

## Limitations (v1)

- Cooperative scheduling: a long synchronous loop without `await`/`yieldNow()`
  monopolizes its worker thread until it suspends (no preemption yet).
- Killing is cooperative too: `killProcess()` takes effect at the target's next
  scheduling boundary, so a process stuck in a long synchronous loop won't die
  until it yields.
- Timers are serviced by idle workers on a ~5 ms tick, so `sleep()`/`recv()`
  deadlines are accurate to within a tick or two, not to the millisecond.
- At most one outstanding suspension per process: calling `sleep()` while a
  `recv()` is pending (or vice versa) raises a `TypeError`. `recv()`/`yieldNow()`
  after a `sleep()` is fine once the sleep has completed.
- Messages must be JSON-serializable; functions, symbols and `undefined` are
  rejected.
- Sandboxing is a single toggle today (`canSpawnAndKill`); it gates only `spawn`
  and `killProcess(other)`. `send`/`recv`/`sleep`/`yieldNow` are unrestricted,
  and there is no CPU/memory isolation. See [`docs/api.md`](docs/api.md#sandboxing).

## Tests

```sh
cargo test
```
