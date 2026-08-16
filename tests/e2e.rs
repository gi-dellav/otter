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

#[test]
fn sleep_resumes_after_delay() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "sleeper.js",
        r#"
            const t0 = Date.now();
            await sleep(60);
            const elapsed = Date.now() - t0;
            if (elapsed < 50) throw new Error("woke too early: " + elapsed + "ms");
            if (elapsed > 1000) throw new Error("woke too late: " + elapsed + "ms");
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn top_level_await_sleep_parks_entry_script() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    // The entry script suspends before returning; spawn_process must park it
    // in the sleeping map (not the run queue) and the timer must resume it.
    spawn_process(
        &world,
        "top.js",
        r#"
            await sleep(40);
            send(0, "done");
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn recv_timeout_rejects_with_timeout_error() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "timeout.js",
        r#"
            try {
                await recv(40);
                throw new Error("recv did not time out");
            } catch (e) {
                if (!(e instanceof TimeoutError)) {
                    throw new Error("wrong error type: " + e);
                }
            }
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn recv_message_before_timeout() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "a.js",
        r#"
            const b = spawn(`send(0, "quick");`);
            const msg = await recv(1000);
            if (msg !== "quick") throw new Error("unexpected: " + msg);
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn message_that_races_a_timeout_is_still_delivered() {
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
                await sleep(30);
                send(0, "late");
            `);
            let timedOut = false;
            try {
                await recv(10);
            } catch (e) {
                timedOut = e instanceof TimeoutError;
            }
            if (!timedOut) throw new Error("expected a timeout");
            // The late message must still be waiting in the mailbox.
            const m = await recv();
            if (m !== "late") throw new Error("unexpected: " + m);
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn sleeping_process_does_not_block_others() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    // While the sleeper is parked, pid 0 must still be able to ping-pong
    // with a third process, then receive the sleeper's wake-up message.
    spawn_process(
        &world,
        "a.js",
        r#"
            spawn(`
                await sleep(80);
                send(0, "awake");
            `);
            const pinger = spawn(`
                for (let i = 0; i < 3; i++) {
                    send(0, "ping");
                    await recv();
                }
            `);
            for (let i = 0; i < 3; i++) {
                const m = await recv();
                if (m !== "ping") throw new Error("unexpected: " + m);
                send(pinger, "pong");
            }
            const awake = await recv();
            if (awake !== "awake") throw new Error("unexpected: " + awake);
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn sleep_then_plain_recv_has_no_stale_timeout() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    // Regression: after a sleep wakes, a subsequent plain recv() must not
    // inherit the (stale) sleep deadline as a recv timeout.
    spawn_process(
        &world,
        "a.js",
        r#"
            await sleep(20);
            spawn(`send(0, "ping");`);
            const m = await recv();
            if (m !== "ping") throw new Error("unexpected: " + m);
        "#,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}
