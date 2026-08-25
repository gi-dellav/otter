# API Reference

Every process is created with these globals. Scripts support top-level
`await`, so the suspension primitives can be used directly at the top level
of a file as well as inside async functions.

---

## `spawn(code, opts?) → pid`

Starts a new process from a JavaScript source string and returns its pid
(`u64`). The child gets a fresh, fully isolated runtime and mailbox; it can
never see the parent's variables.

```js
const pid = spawn(`send(0, "hello from a child");`);
```

- The child starts executing immediately (it is queued before `spawn`
  returns).
- A syntax error in `code` raises a `TypeError` in the caller.
- The child's pid is unique across the whole runtime.
- **Sandboxing:** the child inherits the parent's sandbox by default. Pass
  an options object to narrow it at birth — see [Sandboxing](#sandboxing):

  ```js
  spawn(code, { sandbox: { canSpawnAndKill: false } }); // confined child
  ```

  A process whose own sandbox denies `canSpawnAndKill` cannot `spawn` at all
  (it throws `PermissionError`); the child's sandbox can only ever be
  *narrower* than the parent's, never wider.

## `send(pid, value)`

Serializes `value` to JSON and delivers it to `pid`'s mailbox. Returns
immediately; delivery is asynchronous.

```js
send(other, { from: self(), n: 1 });
send(other, null);      // null is a valid message
```

- Sending to an unknown or finished pid is silently dropped, BEAM-style.
- `value` must be JSON-serializable. Functions, symbols, and `undefined`
  raise a `TypeError`.
- A message sent to a process that is parked on `recv()` wakes it; a message
  sent to a running process is buffered in its mailbox.

## `await recv(timeoutMs?) → value`

Suspends the process until a message arrives, then resolves with the parsed
value. This is the only way to consume messages; the mailbox is FIFO.

```js
const msg = await recv();            // wait forever
const reply = await recv(500);       // give up after 500 ms
```

With `timeoutMs`:

- If a message arrives first, resolves with it — identical to `recv()`.
- If no message arrives within `timeoutMs`, **rejects** with a
  `TimeoutError` (a global class; `e instanceof TimeoutError` is true).
  Catch it with `try/catch`:

```js
try {
  const reply = await recv(200);
  console.log("replied:", reply);
} catch (e) {
  console.log("no reply in time:", e.name);   // "TimeoutError"
}
```

- A message that arrives *after* the deadline but while the process is still
  alive is preserved in the mailbox and delivered to the next `recv()`.
- `recv()` without arguments behaves exactly as before timers existed.
- `timeoutMs <= 0` (or `NaN`/`Infinity`) behaves like an immediate deadline.

**Timeouts are best-effort:** timers are serviced on a ~5 ms tick, so the
actual rejection may be a few milliseconds late.

## `await sleep(ms)`

Suspends the process for at least `ms` milliseconds, then resolves. Does not
touch the mailbox — a `sleep()` is uninterruptible (a `send` to a sleeping
process just sits in its mailbox), BEAM-`timer:sleep`-style.

```js
await sleep(1000);
console.log("one second later");
```

- The resolved value is `undefined`.
- `sleep(0)` parks the process until the next timer tick; it is *not* a
  guaranteed immediate yield (use `yieldNow()` for that).
- Precision is bounded by the ~5 ms timer tick: `sleep(2)` may take up to
  ~7 ms, and `sleep` is never early.

## `await yieldNow()`

Voluntarily gives up the current scheduling slice and rejoins the back of the
run queue, letting other runnable processes run. Resolves when the process is
next scheduled.

```js
for (let i = 0; i < 1000; i++) {
  await yieldNow();   // keep the worker fair between heavy iterations
}
```

Named `yieldNow` because `yield` is a reserved word inside async-function
bodies (which is how top-level-await scripts are parsed).

## `self() → pid`

Returns the current process's pid.

```js
send(0, { from: self() });
```

---

## `killProcess(pid) → boolean`

Requests termination of a live process and returns `true` if `pid` was
live; returns `false` for an unknown or already-finished pid. Killing is
**best-effort and cooperative**: the target is marked killed and reaped at
its next scheduling boundary. A process parked on `recv()` or sleeping is
reaped immediately; a process on the run queue or mid-slice dies when its
slice ends.

```js
if (killProcess(other)) {
  console.log("termination of", other, "requested");
}
```

- Killing `self()` is allowed: the current slice finishes, then the process
  is reaped — any later `await` in that slice never resolves.
- A killed process dies **silently**: it is not counted as a failure, so
  the runtime's exit code stays `0`.
- After the kill, the pid is unregistered: `send` to it is dropped, and
  `isProcessAlive`/`listProcesses` no longer see it. Pids are never reused.
- A process stuck in a long synchronous loop (no `await`/`yieldNow()`) will
  not die until it yields — same cooperative limit as scheduling.
- **Sandboxing:** killing *another* process requires the caller's sandbox to
  hold `canSpawnAndKill` (see [Sandboxing](#sandboxing)). Killing *self* is
  always allowed, even when confined. The permission check runs *before* the
  liveness check, so a confined process that calls `killProcess(unknownPid)`
  gets a `PermissionError` rather than learning the pid is unknown.

## `listProcesses() → array`

Returns an array of `{pid, name, status}` for every live process, sorted by
pid. `status` is one of `"running" | "waiting" | "yielding" | "sleeping" |
"done" | "failed" | "killed"`; the snapshot is taken atomically but the
world may move on while you inspect it.

```js
for (const p of listProcesses()) {
  console.log(`pid ${p.pid} (${p.name}) is ${p.status}`);
}
```

## `isProcessAlive(pid) → boolean`

`true` while `pid` is registered as a live process.

```js
if (isProcessAlive(other)) send(other, "ping");
```

## `processInfo(pid) → object | null`

Returns `{pid, name, status}` for a live pid, or `null` for an unknown or
finished pid.

```js
const info = processInfo(self());
console.log(info.name);   // the process's name
```

## `processCount() → number`

Number of live processes, including the caller.

```js
console.log("live processes:", processCount());
```

## `setName(name)`

Renames the current process. The new name is visible to every process via
`listProcesses()` and `processInfo()`; it does not affect the pid.

```js
setName("http-server");
```

## `console.log(...args)` / `console.error(...args)`

Prints a line to stdout/stderr, prefixed with the calling process's pid.
Values are rendered like `JSON.stringify` (strings verbatim, `undefined` as
`"undefined"`).

```js
console.log("hello", { x: 1 });   // [pid 0] hello {"x":1}
```

## Globals you should not use

The implementation stashes internal bookkeeping on the global object under
`__otter_*` names (`__otter_recv_resolve`, `__otter_recv_reject`,
`__otter_sleep_resolve`, `__otter_yield_resolve`, `__otter_entry`,
`__otter_done`, `__otter_error`, `__otter_exc`). They are not part of the
public API and may change at any time.

## Error types

- **`TypeError`** — bad argument to a builtin (non-serializable message,
  overlapping suspensions, `spawn` of invalid source, a malformed
  `restrictSandbox` request).
- **`TimeoutError`** — rejection value of `recv(timeoutMs)` when it times
  out. Defined in every process's global scope.
- **`PermissionError`** — a privileged operation was attempted by a
  process whose sandbox denies it (`spawn` or `killProcess(other)` while
  `canSpawnAndKill` is off, or `restrictSandbox` on another process while
  the caller lacks the toggle being dropped). Defined in every process's
  global scope.
- Any other uncaught exception kills only the process that raised it; the
  runtime exits non-zero if at least one process failed.

## Sandboxing

Each process carries a **sandbox policy**: a set of on/off toggles that gate
privileged operations. Toggles only ever **narrow** — a process can drop a
permission it holds, but can never gain one it lacks. Every change is an
intersection, so confinement is irrevocable in effect: once a toggle is off,
no later call can turn it back on.

Today there is one toggle:

- **`canSpawnAndKill`** — if `false`, the process may not `spawn` and may not
  `killProcess` *other* processes. Killing *self* is always allowed.

### Defaults and inheritance

- Root processes (the scripts on the `otter` command line) start
  **privileged** (`canSpawnAndKill: true`).
- A spawned child **inherits** the parent's sandbox by default.
- A child can only ever be *narrower* than the parent: passing
  `{ sandbox: { canSpawnAndKill: false } }` to `spawn` confines the child,
  but a confined process cannot spawn at all, and could not grant privileges
  it lacks even if it could (no escalation).

### `selfSandbox() → { canSpawnAndKill: boolean }`

Returns a snapshot of the calling process's own sandbox. The only way to read
a sandbox across the JS API; capability info is deliberately **not** exposed
via `processInfo`/`listProcesses`.

```js
if (!selfSandbox().canSpawnAndKill) throw new Error("i am confined");
```

### `restrictSandbox(policy?, opts?) → { canSpawnAndKill: boolean }`

Narrows a sandbox at runtime. `policy` is a *partial* sandbox object
`{ canSpawnAndKill?: boolean }`; absent keys are left untouched (per-toggle
granularity, forward-compatible with more toggles). Returns the affected
sandbox's **post-state**.

```js
restrictSandbox({ canSpawnAndKill: false });          // self: drop, irrevocable
restrictSandbox({ canSpawnAndKill: false }, { pid: child }); // narrow a child
restrictSandbox();                                      // self: pure read
```

Semantics, per present key: `new = current & requested`. So `false` narrows
and `true` is a no-op (it never widens). The result is therefore monotonic:
asking to re-grant a dropped toggle is silently intersected away, and the
returned snapshot tells you so.

- **Self-target** (the default, or `opts.pid === self()`): always allowed —
  narrowing yourself is a pure loss of privilege, needing none to begin with.
  An empty/omitted policy is a pure read.
- **Other-target** (`opts.pid` set to another pid): only an actual narrowing
  is permitted — the policy must set `canSpawnAndKill` to `false`. A pure
  read or a widen attempt (`{}` or `{canSpawnAndKill: true}`) raises a
  `TypeError`, since cross-target reads of another sandbox are not allowed.
  The caller must currently hold the toggle it is dropping, else
  `PermissionError`. An unknown target raises `TypeError` *after* the
  privilege check, so a confined caller targeting a dead pid still gets
  `PermissionError` first (no liveness leak).

The standard secure pattern is setup-then-drop, like `pledge`/seccomp: a
privileged process sets up its resources, then calls
`restrictSandbox({ canSpawnAndKill: false })` on itself and can no longer
spawn or kill others for the rest of its life.

## Suspension rules

- At most **one** outstanding suspension per process. `recv()`, `sleep()`,
  and `yieldNow()` each suspend the process; calling `sleep()` while a
  `recv()` is pending (or vice versa, in the same synchronous stretch)
  raises a `TypeError`.
- Once a suspension has completed (resolved or rejected), the next
  suspension is fine — `await sleep(100); await recv();` is valid.
- A process can be killed from outside (or by itself) via `killProcess()`;
  see the [management API](#killprocesspid--boolean) above. Kills are
  cooperative and take effect at the target's next scheduling boundary. An
  uncaught error terminates only its own process.
