use std::path::PathBuf;
use std::sync::atomic::Ordering;

use clap::Parser;

use otter::scheduler::{RunItem, World, worker_loop};

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
                if let Err(msg) = otter::scheduler::spawn_process(&world, &name, &source) {
                    eprintln!("otter: failed to start {}: {msg}", path.display());
                }
            }
            Err(e) => eprintln!("otter: cannot read {}: {e}", path.display()),
        }
    }

    // Wait until every process (including all spawned ones) has finished.
    {
        let mut active = world.active.lock().unwrap();
        while *active > 0 {
            active = world.active_cv.wait(active).unwrap();
        }
    }

    for _ in 0..workers {
        let _ = world.queue_tx.send(RunItem::Stop);
    }
    for handle in handles {
        let _ = handle.join();
    }

    if world.failed.load(Ordering::Relaxed) > 0 {
        std::process::exit(1);
    }
}
