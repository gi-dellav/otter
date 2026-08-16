//! A single BEAM-like process: its own QuickJS runtime (isolated heap),
//! its mailbox, and the JS API injected into its global scope.

use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rquickjs::context::EvalOptions;
use rquickjs::prelude::{Opt, Rest};
use rquickjs::{Context, Ctx, Function, Object, Promise, Runtime, Value};

use crate::scheduler::{self, Pid, World};

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
}

/// State shared between the scheduler and the JS callbacks of one process.
pub struct ProcShared {
    pub pid: Pid,
    pub name: String,
    pub inbox_rx: Mutex<Receiver<String>>,
    pub status: Mutex<Status>,
    /// Deadline of the outstanding `sleep()` or `recv(timeoutMs)` suspension,
    /// if any. At most one waiter per process, so a single field suffices;
    /// the scheduler reads it when parking the process to register the timer.
    pub deadline: Mutex<Option<Instant>>,
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
) -> Result<Process, String> {
    let rt = Runtime::new().map_err(|e| e.to_string())?;
    let ctx = Context::full(&rt).map_err(|e| e.to_string())?;
    let shared = Arc::new(ProcShared {
        pid,
        name: name.to_string(),
        inbox_rx: Mutex::new(inbox_rx),
        status: Mutex::new(Status::Running),
        deadline: Mutex::new(None),
    });

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

    let w = world.clone();
    globals.set(
        "spawn",
        Function::new(
            cx.clone(),
            move |cx: Ctx<'js>, code: String| -> rquickjs::Result<u64> {
                scheduler::spawn_process(&w, "<spawned>", &code)
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
                        *s.status.lock().unwrap() = Status::Waiting;
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
                *s.status.lock().unwrap() = Status::Sleeping;
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
                *s.status.lock().unwrap() = Status::Yielding;
                Ok(promise)
            },
        )?,
    )?;

    let s = shared.clone();
    globals.set("self", Function::new(cx.clone(), move || -> u64 { s.pid })?)?;

    let s = shared.clone();
    globals.set(
        "__otter_done",
        Function::new(cx.clone(), move || {
            *s.status.lock().unwrap() = Status::Done;
        })?,
    )?;

    let s = shared.clone();
    globals.set(
        "__otter_error",
        Function::new(cx.clone(), move |msg: String| {
            eprintln!("[pid {}] error: {msg}", s.pid);
            *s.status.lock().unwrap() = Status::Failed;
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

/// Build a `TypeError` and return it as an `Error::Exception`.
pub fn js_type_error(cx: &Ctx<'_>, msg: &str) -> rquickjs::Error {
    let literal = cx
        .json_stringify(msg.to_string())
        .ok()
        .flatten()
        .and_then(|s| s.to_string().ok())
        .unwrap_or_else(|| "\"error\"".to_string());
    match cx.eval::<Value, _>(format!("new TypeError({literal})")) {
        Ok(e) => cx.throw(e),
        Err(_) => rquickjs::Error::new_from_js_message("value", "message", msg),
    }
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
