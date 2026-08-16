# API Reference

Every process is created with these globals. Scripts support top-level
`await`, so the suspension primitives can be used directly at the top level
of a file as well as inside async functions.

---

## `spawn(code) → pid`

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
  overlapping suspensions, `spawn` of invalid source).
- **`TimeoutError`** — rejection value of `recv(timeoutMs)` when it times
  out. Defined in every process's global scope.
- Any other uncaught exception kills only the process that raised it; the
  runtime exits non-zero if at least one process failed.

## Suspension rules

- At most **one** outstanding suspension per process. `recv()`, `sleep()`,
  and `yieldNow()` each suspend the process; calling `sleep()` while a
  `recv()` is pending (or vice versa, in the same synchronous stretch)
  raises a `TypeError`.
- Once a suspension has completed (resolved or rejected), the next
  suspension is fine — `await sleep(100); await recv();` is valid.
- A process is never killed from outside: no `kill`, no linked death. An
  uncaught error terminates only its own process.
