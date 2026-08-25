use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

use otter_rt::process::Sandbox;
use otter_rt::scheduler::{RunItem, World, worker_loop};

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[derive(Parser)]
#[command(
    name = "otter",
    about = "A BEAM-like JavaScript runtime: many isolated QuickJS processes multiplexed onto a few OS threads"
)]
struct Args {
    /// Number of worker threads that share all processes
    #[arg(long, default_value_t = default_workers())]
    workers: usize,

    /// Host for the JSON-RPC control socket (default: 127.0.0.1).
    /// The control server keeps the runtime alive after the entry scripts.
    #[arg(long, value_name = "HOST", default_value = "127.0.0.1")]
    rpc_host: String,

    /// Port for the JSON-RPC control socket. Enables external thread control
    /// over TCP (spawn/list/kill/inject/rename/shutdown). The runtime stays
    /// alive until `shutdown` is called or the process is signalled.
    #[arg(long, value_name = "PORT")]
    rpc_port: Option<u16>,

    /// Stay alive after the entry scripts finish, even without RPC.
    #[arg(long)]
    persistent: bool,

    /// Entry scripts; each file starts as its own process (pid 0, 1, ...)
    #[arg(required = true)]
    scripts: Vec<PathBuf>,
}

fn main() {
    let args = Args::parse();
    let workers = args.workers.max(1);

    let world = World::new();

    let mut handles = Vec::with_capacity(workers);
    for i in 0..workers {
        let w = world.clone();
        let handle = std::thread::Builder::new()
            .name(format!("otter-worker-{i}"))
            .spawn(move || worker_loop(w))
            .expect("failed to spawn worker thread");
        handles.push(handle);
    }

    for path in &args.scripts {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string());
        match std::fs::read_to_string(path) {
            Ok(source) => {
                if let Err(msg) = otter_rt::scheduler::spawn_process(&world, &name, &source, Sandbox::PRIVILEGED) {
                    eprintln!("otter: failed to start {}: {msg}", path.display());
                }
            }
            Err(e) => eprintln!("otter: cannot read {}: {e}", path.display()),
        }
    }

    // Optional JSON-RPC control server. If enabled it keeps the runtime alive
    // after the entry scripts finish, so an external tool can spawn/list/kill
    // processes until the port is released via the `shutdown` method.
    let rpc_shutdown = Arc::new(AtomicBool::new(false));
    let rpc_handle = match args.rpc_port {
        Some(port) => {
            match otter_rt::rpc::bind(&args.rpc_host, port) {
                Ok(listener) => {
                    let w = world.clone();
                    let shutdown = rpc_shutdown.clone();
                    let handle = std::thread::Builder::new()
                        .name("otter-rpc".to_string())
                        .spawn(move || otter_rt::rpc::serve_loop(listener, w, shutdown))
                        .expect("failed to spawn rpc thread");
                    eprintln!(
                        "otter: JSON-RPC control server on {}:{port} (call `shutdown` to stop, `list`/`spawn`/`kill`/`send`/`rename` to drive)",
                        args.rpc_host
                    );
                    Some(handle)
                }
                Err(e) => {
                    eprintln!("otter: cannot bind RPC on {}:{port}: {e}", args.rpc_host);
                    None
                }
            }
        }
        None => {
            if args.rpc_host != "127.0.0.1" {
                eprintln!("otter: note: --rpc-host has no effect without --rpc-port");
            }
            None
        }
    };

    // Wait until:
    //   - every process has finished, AND
    //   - the runtime is not being kept alive by --persistent / RPC
    // or until the RPC `shutdown` method (or a SIGINT, which terminates the
    // process outright) tells us to stop.
    //
    // Plain-mode waits on the active-count condvar so it wakes the instant the
    // last process finishes; keep-alive mode polls on a short tick because the
    // active count may be 0 for a long time while the control socket stays open.
    let keep_alive = args.persistent || rpc_handle.is_some();
    loop {
        if rpc_shutdown.load(Ordering::Relaxed) {
            break;
        }
        // Block until the active count changes or ~100ms elapses.
        let mut active = world.active.lock().unwrap();
        let (guard, _) = world
            .active_cv
            .wait_timeout(active, Duration::from_millis(100))
            .unwrap();
        active = guard;
        if *active == 0 && !keep_alive {
            break;
        }
        // In keep-alive mode we deliberately loop even when active == 0.
        drop(active);
    }

    // Tell the RPC server to stop, then stop the workers and join everything.
    rpc_shutdown.store(true, Ordering::Relaxed);
    for _ in 0..workers {
        let _ = world.queue_tx.send(RunItem::Stop);
    }
    for handle in handles {
        let _ = handle.join();
    }
    if let Some(handle) = rpc_handle {
        let _ = handle.join();
    }

    if world.failed.load(Ordering::Relaxed) > 0 {
        std::process::exit(1);
    }
}
