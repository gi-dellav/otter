//! The scheduler: a fixed pool of worker threads multiplexes all processes.
//!
//! Processes move between workers through an unbounded run queue. Each
//! scheduling slice wakes the process (if it was parked on an empty mailbox),
//! executes one pending QuickJS job, then re-queues, parks, or finishes the
//! process based on its status.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};

use crossbeam_channel::{Receiver, Sender};
use rquickjs::{Context, Ctx, Function, Value};

use crate::process::{self, Process, Status};

pub type Pid = u64;

pub enum RunItem {
    Process(Box<Process>),
    Stop,
}

/// Everything workers and JS callbacks share.
pub struct World {
    pub queue_tx: Sender<RunItem>,
    pub queue_rx: Receiver<RunItem>,
    /// Processes suspended on `recv()`, keyed by pid. Holding the parked map
    /// lock is what makes "is the inbox empty?" and "park the process"
    /// atomic with respect to `send_message`.
    pub parked: Mutex<HashMap<Pid, Box<Process>>>,
    /// Inbox senders for every live process, keyed by pid.
    pub inboxes: Mutex<HashMap<Pid, mpsc::Sender<String>>>,
    pub next_pid: AtomicU64,
    /// Number of live processes; main waits on `active_cv` for this to hit 0.
    pub active: Mutex<usize>,
    pub active_cv: Condvar,
    pub failed: AtomicU64,
}

impl World {
    pub fn new() -> Arc<Self> {
        let (queue_tx, queue_rx) = crossbeam_channel::unbounded();
        Arc::new(World {
            queue_tx,
            queue_rx,
            parked: Mutex::new(HashMap::new()),
            inboxes: Mutex::new(HashMap::new()),
            next_pid: AtomicU64::new(0),
            active: Mutex::new(0),
            active_cv: Condvar::new(),
            failed: AtomicU64::new(0),
        })
    }
}

/// Create a process from JS source, register it, and schedule it.
pub fn spawn_process(world: &Arc<World>, name: &str, source: &str) -> Result<Pid, String> {
    let pid = world.next_pid.fetch_add(1, Ordering::Relaxed);
    let (inbox_tx, inbox_rx) = mpsc::channel();
    // Register the inbox BEFORE the entry script is evaluated: code that
    // runs during the eval (e.g. a child spawned synchronously) must be
    // able to deliver messages to this process already.
    world.inboxes.lock().unwrap().insert(pid, inbox_tx);
    let proc = match process::create_process(world, pid, name, source, inbox_rx) {
        Ok(proc) => proc,
        Err(e) => {
            world.inboxes.lock().unwrap().remove(&pid);
            return Err(e);
        }
    };
    *world.active.lock().unwrap() += 1;

    let boxed = Box::new(proc);
    if *boxed.shared.status.lock().unwrap() == Status::Waiting {
        // The entry script suspended before returning (e.g. top-level
        // `await recv()` with an empty inbox).
        park(world, boxed);
    } else {
        let _ = world.queue_tx.send(RunItem::Process(boxed));
    }
    Ok(pid)
}

/// Deliver a message to another process (JSON-serialized). Messages to
/// unknown or finished pids are silently dropped, BEAM-style.
pub fn send_message<'js>(
    world: &Arc<World>,
    cx: &Ctx<'js>,
    pid: Pid,
    value: Value<'js>,
) -> rquickjs::Result<()> {
    if value.is_undefined() || value.is_function() || value.is_symbol() {
        return Err(process::js_type_error(
            cx,
            "value is not message-serializable",
        ));
    }
    let json = match cx.json_stringify(value)? {
        Some(s) => s.to_string()?,
        None => {
            return Err(process::js_type_error(
                cx,
                "value is not message-serializable",
            ))
        }
    };

    let tx = world.inboxes.lock().unwrap().get(&pid).cloned();
    if let Some(tx) = tx
        && tx.send(json).is_ok()
    {
        // Push-then-unpark ordering: the message is already buffered
        // before the process is re-scheduled, so the woken process is
        // guaranteed to see it.
        if let Some(p) = world.parked.lock().unwrap().remove(&pid) {
            let _ = world.queue_tx.send(RunItem::Process(p));
        }
    }
    Ok(())
}

/// Worker entry point: pull processes off the run queue until stopped.
pub fn worker_loop(world: Arc<World>) {
    while let Ok(item) = world.queue_rx.recv() {
        match item {
            RunItem::Stop => break,
            RunItem::Process(p) => run_slice(&world, p),
        }
    }
}

/// One scheduling slice: wake if parked, run one job, then decide whether the
/// process is done, parks, or goes back on the run queue.
fn run_slice(world: &Arc<World>, p: Box<Process>) {
    let wake_status = *p.shared.status.lock().unwrap();
    match wake_status {
        Status::Waiting => {
            let msg = p.shared.inbox_rx.lock().unwrap().try_recv();
            match msg {
                Ok(json) => {
                    if wake_with(&p, json).is_err() {
                        fail(world, p, "failed to deliver message");
                        return;
                    }
                }
                Err(TryRecvError::Empty) => {
                    // Spurious wake; park again.
                    park(world, p);
                    return;
                }
                Err(TryRecvError::Disconnected) => {
                    fail(world, p, "mailbox disconnected");
                    return;
                }
            }
        }
        Status::Yielding => {
            if wake_yield(&p).is_err() {
                fail(world, p, "failed to resume after yield");
                return;
            }
        }
        _ => {}
    }

    let status = *p.shared.status.lock().unwrap();
    if status != Status::Done
        && status != Status::Failed
        && p.rt.is_job_pending()
        && let Err(exc) = p.rt.execute_pending_job()
    {
        let msg = exception_message(&exc.0);
        eprintln!("[pid {}] unhandled exception: {msg}", p.shared.pid);
        *p.shared.status.lock().unwrap() = Status::Failed;
    }

    let status = *p.shared.status.lock().unwrap();
    match status {
        Status::Done | Status::Failed => finish(world, p),
        Status::Waiting => park(world, p),
        // A process that yielded goes to the back of the run queue; its
        // resolver fires the next time a worker picks it up.
        Status::Yielding => {
            let _ = world.queue_tx.send(RunItem::Process(p));
        }
        Status::Running => {
            if p.rt.is_job_pending() {
                let _ = world.queue_tx.send(RunItem::Process(p));
            } else {
                // Defensive: no waiter, no jobs, and the entry hook never
                // fired. Treat as finished so nothing leaks.
                finish(world, p);
            }
        }
    }
}

/// Resolve the stashed `recv()` resolver with a buffered message.
fn wake_with(p: &Process, json: String) -> rquickjs::Result<()> {
    p.ctx.with(|cx| {
        let globals = cx.globals();
        let resolve: Option<Function> = globals.get("__otter_recv_resolve")?;
        if let Some(resolve) = resolve {
            let v: Value = cx.json_parse(json)?;
            resolve.call::<_, ()>((v,))?;
            globals.set("__otter_recv_resolve", ())?;
        }
        *p.shared.status.lock().unwrap() = Status::Running;
        Ok(())
    })
}

/// Resume a process that yielded: fire the stashed `yieldNow()` resolver so
/// the suspended continuation becomes a pending job.
fn wake_yield(p: &Process) -> rquickjs::Result<()> {
    p.ctx.with(|cx| {
        let globals = cx.globals();
        let resolve: Option<Function> = globals.get("__otter_yield_resolve")?;
        if let Some(resolve) = resolve {
            resolve.call::<_, ()>(())?;
            globals.set("__otter_yield_resolve", ())?;
        }
        *p.shared.status.lock().unwrap() = Status::Running;
        Ok(())
    })
}

/// Park a waiting process. The inbox is re-checked while holding the parked
/// lock so a message that races the park either wakes the process or finds it
/// already in the map.
fn park(world: &Arc<World>, p: Box<Process>) {
    let mut parked = world.parked.lock().unwrap();
    let msg = p.shared.inbox_rx.lock().unwrap().try_recv();
    match msg {
        Ok(json) => {
            drop(parked);
            if wake_with(&p, json).is_ok() {
                let _ = world.queue_tx.send(RunItem::Process(p));
            } else {
                fail(world, p, "failed to deliver message");
            }
        }
        Err(TryRecvError::Empty) => {
            parked.insert(p.shared.pid, p);
        }
        Err(TryRecvError::Disconnected) => {
            drop(parked);
            finish(world, p);
        }
    }
}

fn fail(world: &Arc<World>, p: Box<Process>, msg: &str) {
    eprintln!("[pid {}] error: {msg}", p.shared.pid);
    *p.shared.status.lock().unwrap() = Status::Failed;
    finish(world, p);
}

/// Unregister a finished process and release its runtime.
fn finish(world: &Arc<World>, p: Box<Process>) {
    world.inboxes.lock().unwrap().remove(&p.shared.pid);
    if *p.shared.status.lock().unwrap() == Status::Failed {
        world.failed.fetch_add(1, Ordering::Relaxed);
    }
    drop(p);
    let mut active = world.active.lock().unwrap();
    *active = active.saturating_sub(1);
    if *active == 0 {
        world.active_cv.notify_all();
    }
}

fn exception_message(ctx: &Context) -> String {
    ctx.with(|cx| process::describe_catch(&cx))
}
