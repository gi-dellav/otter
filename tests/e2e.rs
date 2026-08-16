//! End-to-end scheduler tests: real QuickJS processes running on worker
//! threads, exchanging messages through their mailboxes.

use std::sync::Arc;
use std::time::Duration;

use otter::scheduler::{spawn_process, worker_loop, RunItem, World};

fn wait_for_completion(world: &Arc<World>, timeout: Duration) {
    let active = world.active.lock().unwrap();
    let (guard, timed_out) = world
        .active_cv
        .wait_timeout_while(active, timeout, |a| *a > 0)
        .unwrap();
    drop(guard);
    assert!(!timed_out.timed_out(), "processes did not finish within {timeout:?}");
    assert_eq!(world.failed.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn two_processes_exchange_messages() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "a.js",
        r#"
            const b = spawn(`
                send(0, "hello");
                await recv();
            `);
            const msg = await recv();
            if (msg !== "hello") throw new Error("unexpected message: " + msg);
            send(b, "bye");
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn one_worker_multiplexes_many_processes() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    // 30 workers reply to pid 0, which sums their answers. The sum is only
    // correct if every process ran and every message was delivered.
    spawn_process(
        &world,
        "collector.js",
        r#"
            const COUNT = 30;
            for (let i = 1; i <= COUNT; i++) {
                const p = spawn(`
                    const n = await recv();
                    send(0, n * 2);
                `);
                send(p, i);
            }
            let total = 0;
            for (let i = 0; i < COUNT; i++) {
                total += await recv();
            }
            if (total !== 930) throw new Error("bad total: " + total);
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(30));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn yield_suspends_and_resumes() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "a.js",
        r#"
            const order = [];
            const b = spawn(`
                for (let i = 0; i < 3; i++) {
                    await yieldNow();
                }
                send(0, "b-done");
            `);
            for (let i = 0; i < 3; i++) {
                order.push(i);
                await yieldNow();
            }
            const msg = await recv();
            if (msg !== "b-done") throw new Error("unexpected: " + msg);
            if (order.join(",") !== "0,1,2") {
                throw new Error("bad order: " + order.join(","));
            }
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn failing_process_does_not_stop_others() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "survivor.js",
        r#"
            const bad = spawn(`
                await recv();
                throw new Error("boom");
            `);
            send(bad, "go");
            spawn(`send(0, "alive");`);
            const msg = await recv();
            if (msg !== "alive") throw new Error("unexpected: " + msg);
        "#,
    )
    .unwrap();

    // The survivor and the good child finish; the throwing child fails.
    let active = world.active.lock().unwrap();
    let (guard, timed_out) = world
        .active_cv
        .wait_timeout_while(active, Duration::from_secs(10), |a| *a > 0)
        .unwrap();
    drop(guard);
    assert!(!timed_out.timed_out(), "processes did not finish in time");
    assert_eq!(world.failed.load(std::sync::atomic::Ordering::Relaxed), 1);

    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}
