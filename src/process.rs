//! A single BEAM-like process: its own QuickJS runtime (isolated heap),
//! its mailbox, and the JS API injected into its global scope.

use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rquickjs::context::EvalOptions;
use rquickjs::prelude::{Opt, Rest};
use rquickjs::{Array, Context, Ctx, Function, Object, Promise, Runtime, Value};

use crate::scheduler::{self, Pid, World};

/// Per-process sandbox policy: a set of on/off toggles. Toggles only ever
/// narrow — a process can drop a permission it holds, but can never gain one
/// it lacks (every change is an intersection). Stored on `ProcShared` and read
/// by the JS callbacks that gate privileged operations; the scheduler itself
/// never consults it.
#[derive(Clone, Copy, Debug)]
pub struct Sandbox {
    /// If `false`, this process may not `spawn` processes or `killProcess`
    /// *other* processes. Killing *self* is always allowed.
    pub can_spawn_and_kill: bool,
}

impl Sandbox {
    /// Every toggle on. The sandbox for root (CLI) processes and the default
    /// a process starts from before any restriction.
    pub const PRIVILEGED: Sandbox = Sandbox {
        can_spawn_and_kill: true,
    };

    /// Every toggle off. A fully confined process.
    pub const CONFINED: Sandbox = Sandbox {
        can_spawn_and_kill: false,
    };

    /// Monotonic narrowing: the result holds a toggle only if both operands
    /// do. Used to inherit a parent's sandbox at `spawn` time and to apply a
    /// `restrictSandbox` policy at runtime. This is what makes confinement
    /// irrevocable in effect: intersecting with `CONFINED` can never be undone
    /// by a later intersect.
    pub fn intersect(self, other: Sandbox) -> Sandbox {
        Sandbox {
            can_spawn_and_kill: self.can_spawn_and_kill && other.can_spawn_and_kill,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// The process is running or runnable (jobs may be pending).
    Running,
    /// The process is suspended on `recv()` waiting for a message.
    Waiting,
    /// The process called `yieldNow()` and is waiting to be re-scheduled.
    Yielding,
    /// The process called `sleep()` and is waiting for its deadline.
    Sleeping,
    /// The entry script finished successfully.
    Done,
    /// The entry script (or a job) raised an uncaught error.
    Failed,
    /// A `killProcess()` request was accepted; the process will be reaped at
    /// its next scheduling boundary. Terminal, like `Done`/`Failed`.
    Killed,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Running => "running",
            Status::Waiting => "waiting",
            Status::Yielding => "yielding",
            Status::Sleeping => "sleeping",
            Status::Done => "done",
            Status::Failed => "failed",
            Status::Killed => "killed",
        }
    }
}

/// State shared between the scheduler and the JS callbacks of one process.
pub struct ProcShared {
    pub pid: Pid,
    pub name: Mutex<String>,
    pub inbox_rx: Mutex<Receiver<String>>,
    pub status: Mutex<Status>,
    /// Deadline of the outstanding `sleep()` or `recv(timeoutMs)` suspension,
    /// if any. At most one waiter per process, so a single field suffices;
    /// the scheduler reads it when parking the process to register the timer.
    pub deadline: Mutex<Option<Instant>>,
    /// The process's sandbox policy. Fixed at birth (inherited from the
    /// parent, narrowed by any `spawn` override) and only ever narrowed
    /// afterwards via `restrictSandbox`. A mutex even with a single bool so
    /// future toggles can be mutated in place without an API churn.
    pub sandbox: Mutex<Sandbox>,
}

impl ProcShared {
    /// Set the status unless the process is being killed. A killed process
    /// keeps `Status::Killed` so the scheduler reaps it at its next slice;
    /// a suspension callback (`recv`/`sleep`/`yieldNow`) or a wake must
    /// never overwrite that. The check and write are one critical section,
    /// so they cannot race a concurrent `killProcess`.
    pub fn set_status_unless_killed(&self, status: Status) {
        let mut st = self.status.lock().unwrap();
        if *st != Status::Killed {
            *st = status;
        }
    }
}

/// One process: an isolated QuickJS runtime + context plus shared state.
pub struct Process {
    pub rt: Runtime,
    pub ctx: Context,
    pub shared: Arc<ProcShared>,
}

/// JS function expression that renders any thrown value as
/// `String(e)` plus the stack trace when available.
const ERROR_DESCRIBER: &str = "(function(e){ if (e == null) return String(e); var m = String(e); return e.stack ? m + '\\n' + e.stack : m; })";

/// Create a process: a fresh runtime, the JS API globals, and evaluate the
/// entry script (with top-level await support).
pub fn create_process(
    world: &Arc<World>,
    pid: Pid,
    name: &str,
    source: &str,
    inbox_rx: Receiver<String>,
    sandbox: Sandbox,
) -> Result<Process, String> {
    let rt = Runtime::new().map_err(|e| e.to_string())?;
    let ctx = Context::full(&rt).map_err(|e| e.to_string())?;
    let shared = Arc::new(ProcShared {
        pid,
        name: Mutex::new(name.to_string()),
        inbox_rx: Mutex::new(inbox_rx),
        status: Mutex::new(Status::Running),
        deadline: Mutex::new(None),
        sandbox: Mutex::new(sandbox),
    });
    // Register before evaluating the entry script: synchronous code in the
    // script (e.g. a self-kill or `listProcesses()` call) must already see
    // this pid, mirroring the inbox-registration invariant in `spawn_process`.
    world.processes.lock().unwrap().insert(pid, shared.clone());

    let setup: Result<(), String> = ctx.with(|cx| {
        setup_globals(&cx, world, &shared).map_err(|e| e.to_string())?;
        let mut options = EvalOptions::default();
        options.promise = true;
        options.filename = Some(name.to_string());
        match cx.eval_with_options::<Promise, _>(source, options) {
            Ok(entry) => {
                cx.globals()
                    .set("__otter_entry", entry)
                    .map_err(|e| e.to_string())?;
                // Hook the entry promise into the lifecycle callbacks so
                // both sync and async failures are captured.
                let attach = format!(
                    "__otter_entry.then(() => __otter_done(), (e) => __otter_error({ERROR_DESCRIBER}(e)));"
                );
                cx.eval::<(), _>(attach).map_err(|e| e.to_string())?;
                Ok(())
            }
            Err(_) => Err(describe_catch(&cx)),
        }
    });
    setup.map(|()| Process { rt, ctx, shared })
}

fn setup_globals<'js>(
    cx: &Ctx<'js>,
    world: &Arc<World>,
    shared: &Arc<ProcShared>,
) -> rquickjs::Result<()> {
    let globals = cx.globals();

    // Distinguishable error type for `recv(timeoutMs)` timeouts.
    cx.eval::<(), _>(
        "var TimeoutError = class extends Error { constructor(msg) { super(msg); this.name = 'TimeoutError'; } };",
    )?;

    // Distinguishable error type for sandbox permission violations (a
    // privileged operation attempted by a process whose sandbox denies it).
    cx.eval::<(), _>(
        "var PermissionError = class extends Error { constructor(msg) { super(msg); this.name = 'PermissionError'; } };",
    )?;

    let w = world.clone();
    let s = shared.clone();
    globals.set(
        "spawn",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>, code: String, opts: Opt<Value<'js>>| -> rquickjs::Result<u64> {
                let parent = *s.sandbox.lock().unwrap();
                if !parent.can_spawn_and_kill {
                    return Err(permission_error(
                        &cx,
                        "spawn is not permitted in this sandbox",
                    ));
                }
                // Child sandbox: inherit the parent, narrowed by any explicit
                // override. Absent keys inherit; `true` is a no-op intersect;
                // only `false` narrows. No escalation is possible: the child can
                // never hold a toggle the parent lacks.
                let mut child = parent;
                if let Some(opts_v) = opts.0
                    && let Some(opts_obj) = opts_v.as_object()
                {
                    let sb_v: Option<Value> = opts_obj.get("sandbox")?;
                    if let Some(sb_v) = sb_v
                        && let Some(sb_obj) = sb_v.as_object()
                    {
                        let csk: Option<bool> = sb_obj.get("canSpawnAndKill")?;
                        if let Some(false) = csk {
                            child.can_spawn_and_kill = false;
                        }
                    }
                }
                scheduler::spawn_process(&w, "<spawned>", &code, child)
                    .map_err(|msg| js_type_error(&cx, &msg))
            },
        )?,
    )?;

    let w = world.clone();
    globals.set(
        "send",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>, pid: u64, value: Value<'js>| -> rquickjs::Result<()> {
                scheduler::send_message(&w, &cx, pid, value)
            },
        )?,
    )?;

    let s = shared.clone();
    globals.set(
        "recv",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>, timeout_ms: Opt<f64>| -> rquickjs::Result<Promise<'js>> {
                // At most one outstanding suspension per process.
                let pending_sleep: Option<Function> = cx.globals().get("__otter_sleep_resolve")?;
                if pending_sleep.is_some() {
                    return Err(js_type_error(
                        &cx,
                        "cannot call recv() while a sleep() is pending",
                    ));
                }
                let (promise, resolve, reject) = Promise::new(&cx)?;
                match s.inbox_rx.lock().unwrap().try_recv() {
                    Ok(json) => {
                        let v: Value = cx.json_parse(json)?;
                        resolve.call::<_, ()>((v,))?;
                    }
                    Err(TryRecvError::Empty) => {
                        // Park: stash the resolvers in JS space and mark the
                        // process as waiting. The scheduler parks it after
                        // this job completes and registers the deadline.
                        cx.globals().set("__otter_recv_resolve", resolve)?;
                        cx.globals().set("__otter_recv_reject", reject)?;
                        if let Some(ms) = timeout_ms.0 {
                            let ms = if ms.is_finite() { ms.max(0.0) } else { 0.0 };
                            *s.deadline.lock().unwrap() =
                                Some(Instant::now() + Duration::from_millis(ms as u64));
                        }
                        s.set_status_unless_killed(Status::Waiting);
                    }
                    Err(TryRecvError::Disconnected) => {
                        resolve.call::<_, ()>(())?;
                    }
                }
                Ok(promise)
            },
        )?,
    )?;

    // Suspend the process until `ms` milliseconds have elapsed. The deadline
    // is registered when the scheduler parks the (now Sleeping) process.
    let s = shared.clone();
    globals.set(
        "sleep",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>, ms: f64| -> rquickjs::Result<Promise<'js>> {
                // At most one outstanding suspension per process.
                let pending_recv: Option<Function> = cx.globals().get("__otter_recv_resolve")?;
                if pending_recv.is_some() {
                    return Err(js_type_error(
                        &cx,
                        "cannot call sleep() while a recv() is pending",
                    ));
                }
                let (promise, resolve, _reject) = Promise::new(&cx)?;
                let ms = if ms.is_finite() { ms.max(0.0) } else { 0.0 };
                cx.globals().set("__otter_sleep_resolve", resolve)?;
                *s.deadline.lock().unwrap() =
                    Some(Instant::now() + Duration::from_millis(ms as u64));
                s.set_status_unless_killed(Status::Sleeping);
                Ok(promise)
            },
        )?,
    )?;

    // Named `yieldNow` because `yield` is reserved inside async-function
    // bodies, which is how top-level-await scripts are parsed by QuickJS.
    let s = shared.clone();
    globals.set(
        "yieldNow",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>| -> rquickjs::Result<Promise<'js>> {
                let (promise, resolve, _reject) = Promise::new(&cx)?;
                cx.globals().set("__otter_yield_resolve", resolve)?;
                s.set_status_unless_killed(Status::Yielding);
                Ok(promise)
            },
        )?,
    )?;

    let s = shared.clone();
    globals.set("self", Function::new(cx.clone(), move || -> u64 { s.pid })?)?;

    let s = shared.clone();
    let w = world.clone();
    globals.set(
        "killProcess",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>, pid: u64| -> rquickjs::Result<bool> {
                // Killing *self* is always allowed; killing another process
                // requires the sandbox toggle. The privilege check runs before
                // the liveness check so a confined process learns nothing about
                // whether an unknown pid exists — it gets `PermissionError`
                // regardless.
                if !s.sandbox.lock().unwrap().can_spawn_and_kill && pid != s.pid {
                    return Err(permission_error(
                        &cx,
                        "killProcess on another process is not permitted in this sandbox",
                    ));
                }
                Ok(scheduler::kill_process(&w, pid))
            },
        )?,
    )?;

    // Snapshot the current process's own sandbox as `{canSpawnAndKill: bool}`.
    // The only way to read a sandbox across the JS API; capability info is not
    // exposed via `processInfo`/`listProcesses`.
    let s = shared.clone();
    globals.set(
        "selfSandbox",
        Function::new(cx.clone(), move |cx: Ctx<'js>| -> rquickjs::Result<Value<'js>> {
            let sb = *s.sandbox.lock().unwrap();
            sandbox_snapshot(&cx, &sb)
        })?,
    )?;

    // Narrow a sandbox at runtime. Monotonic: only intersections, so a
    // dropped toggle can never come back. Returns the target's post-state.
    //
    // - `restrictSandbox()` / `restrictSandbox({})` on self: pure read.
    // - `restrictSandbox({canSpawnAndKill:false})` on self: always allowed
    //   (a loss of privilege needs no privilege); irrevocable in effect.
    // - `restrictSandbox({canSpawnAndKill:false}, {pid: other})`: the caller
    //   must currently hold the toggle being dropped, and the request must
    //   actually narrow (no pure reads or widen attempts on others). An
    //   unknown target raises `TypeError` *after* the privilege check, so a
    //   confined caller still gets `PermissionError` first.
    let s = shared.clone();
    let w = world.clone();
    globals.set(
        "restrictSandbox",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>,
                  policy: Opt<Value<'js>>,
                  opts: Opt<Value<'js>>|
                -> rquickjs::Result<Value<'js>> {
                // Parse the partial policy: only `canSpawnAndKill` for now.
                let req_csk: Option<bool> = match &policy.0 {
                    Some(v) if v.is_object() => {
                        v.as_object().unwrap().get::<_, Option<bool>>("canSpawnAndKill")?
                    }
                    _ => None,
                };
                // Target pid: defaults to self.
                let target: Pid = match &opts.0 {
                    Some(v) if v.is_object() => {
                        let pid: Option<u64> = v.as_object().unwrap().get("pid")?;
                        pid.unwrap_or(s.pid)
                    }
                    _ => s.pid,
                };

                if target == s.pid {
                    // Narrow under the lock, then snapshot the value and
                    // release before building the JS object (no Rust mutex
                    // held across JS allocation).
                    let post: Sandbox = {
                        let mut sb = s.sandbox.lock().unwrap();
                        if let Some(false) = req_csk {
                            sb.can_spawn_and_kill = false;
                        }
                        // `Some(true)` and `None` are no-ops: never widen.
                        *sb
                    };
                    return sandbox_snapshot(&cx, &post);
                }

                // Cross-target: only actual narrowing is permitted (no pure
                // reads of another's sandbox, no widen attempts).
                if req_csk != Some(false) {
                    return Err(js_type_error(
                        &cx,
                        "restrictSandbox on another process must set canSpawnAndKill to false",
                    ));
                }
                // The caller must currently hold the toggle it is dropping.
                if !s.sandbox.lock().unwrap().can_spawn_and_kill {
                    return Err(permission_error(
                        &cx,
                        "not permitted to restrict other processes",
                    ));
                }
                let Some(target_shared) = w.processes.lock().unwrap().get(&target).cloned() else {
                    return Err(js_type_error(&cx, "unknown pid"));
                };
                let post: Sandbox = {
                    let mut sb = target_shared.sandbox.lock().unwrap();
                    sb.can_spawn_and_kill = false;
                    *sb
                };
                sandbox_snapshot(&cx, &post)
            },
        )?,
    )?;

    let w = world.clone();
    globals.set(
        "listProcesses",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>| -> rquickjs::Result<Array<'js>> {
                let procs = scheduler::list_processes(&w);
                let arr = Array::new(cx.clone())?;
                for (i, (pid, name, status)) in procs.into_iter().enumerate() {
                    let obj = Object::new(cx.clone())?;
                    obj.set("pid", pid)?;
                    obj.set("name", name)?;
                    obj.set("status", status)?;
                    arr.set(i, obj)?;
                }
                Ok(arr)
            },
        )?,
    )?;

    let w = world.clone();
    globals.set(
        "isProcessAlive",
        Function::new(cx.clone(), move |pid: u64| -> bool {
            scheduler::is_process_alive(&w, pid)
        })?,
    )?;

    let w = world.clone();
    globals.set(
        "processInfo",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>, pid: u64| -> rquickjs::Result<Value<'js>> {
                match scheduler::process_info(&w, pid) {
                    Some((pid, name, status)) => {
                        let obj = Object::new(cx.clone())?;
                        obj.set("pid", pid)?;
                        obj.set("name", name)?;
                        obj.set("status", status)?;
                        Ok(obj.into_value())
                    }
                    None => Ok(Value::new_null(cx.clone())),
                }
            },
        )?,
    )?;

    let w = world.clone();
    globals.set(
        "processCount",
        Function::new(cx.clone(), move || -> usize {
            scheduler::process_count(&w)
        })?,
    )?;

    let w = world.clone();
    let s = shared.clone();
    globals.set(
        "setName",
        Function::new(cx.clone(), move |name: String| {
            scheduler::set_process_name(&w, s.pid, name);
        })?,
    )?;

    let s = shared.clone();
    globals.set(
        "__otter_done",
        Function::new(cx.clone(), move || {
            s.set_status_unless_killed(Status::Done);
        })?,
    )?;

    let s = shared.clone();
    globals.set(
        "__otter_error",
        Function::new(cx.clone(), move |msg: String| {
            // A killed process dies silently: no error report and no `Failed`
            // status, so a kill can never turn into a non-zero exit code.
            let mut st = s.status.lock().unwrap();
            if *st != Status::Killed {
                eprintln!("[pid {}] error: {msg}", s.pid);
                *st = Status::Failed;
            }
        })?,
    )?;

    let s = shared.clone();
    let log = Function::new(
        cx.clone(),
        move |cx: Ctx<'js>, args: Rest<Value<'js>>| -> rquickjs::Result<()> {
            let mut parts = Vec::with_capacity(args.0.len());
            for v in args.0.iter() {
                parts.push(fmt_value(&cx, v)?);
            }
            println!("[pid {}] {}", s.pid, parts.join(" "));
            Ok(())
        },
    )?;
    let console = Object::new(cx.clone())?;
    console.set("log", log.clone())?;
    console.set("error", log)?;
    globals.set("console", console)?;

    Ok(())
}

fn fmt_value<'js>(cx: &Ctx<'js>, v: &Value<'js>) -> rquickjs::Result<String> {
    if v.is_undefined() {
        return Ok("undefined".to_string());
    }
    if let Some(s) = v.as_string() {
        return s.to_string();
    }
    match cx.json_stringify(v.clone())? {
        Some(s) => s.to_string(),
        None => Ok("undefined".to_string()),
    }
}

/// Build a `{canSpawnAndKill: bool}` snapshot of a sandbox for the JS API.
fn sandbox_snapshot<'js>(cx: &Ctx<'js>, sb: &Sandbox) -> rquickjs::Result<Value<'js>> {
    let obj = Object::new(cx.clone())?;
    obj.set("canSpawnAndKill", sb.can_spawn_and_kill)?;
    Ok(obj.into_value())
}

/// Build a JS `Error` of the named class (e.g. `"TypeError"`,
/// `"PermissionError"`) carrying `msg` and return it as an `Error::Exception`.
/// The class must already be defined in the context's global scope.
pub fn js_named_error(cx: &Ctx<'_>, class: &str, msg: &str) -> rquickjs::Error {
    let literal = cx
        .json_stringify(msg.to_string())
        .ok()
        .flatten()
        .and_then(|s| s.to_string().ok())
        .unwrap_or_else(|| "\"error\"".to_string());
    match cx.eval::<Value, _>(format!("new {class}({literal})")) {
        Ok(e) => cx.throw(e),
        Err(_) => rquickjs::Error::new_from_js_message("value", "message", msg),
    }
}

/// Build a `TypeError` and return it as an `Error::Exception`.
pub fn js_type_error(cx: &Ctx<'_>, msg: &str) -> rquickjs::Error {
    js_named_error(cx, "TypeError", msg)
}

/// Build a `PermissionError` (a global class defined in every process) and
/// return it as an `Error::Exception`. Used by the sandbox gates.
pub fn permission_error(cx: &Ctx<'_>, msg: &str) -> rquickjs::Error {
    js_named_error(cx, "PermissionError", msg)
}

/// Render the pending exception of a context as a readable string.
pub fn describe_catch(cx: &Ctx<'_>) -> String {
    let v = cx.catch();
    if v.is_null() || v.is_undefined() {
        return "unknown error".to_string();
    }
    if cx.globals().set("__otter_exc", v).is_err() {
        return "unknown error".to_string();
    }
    cx.eval::<String, _>(format!("{ERROR_DESCRIBER}(__otter_exc)"))
        .unwrap_or_else(|_| "unknown error".to_string())
}
