//! End-to-end scheduler tests: real QuickJS processes running on worker
//! threads, exchanging messages through their mailboxes.

use std::sync::Arc;
use std::time::Duration;

use otter_rt::process::Sandbox;
use otter_rt::scheduler::{RunItem, World, spawn_process, worker_loop};

fn wait_for_completion(world: &Arc<World>, timeout: Duration) {
    let active = world.active.lock().unwrap();
    let (guard, timed_out) = world
        .active_cv
        .wait_timeout_while(active, timeout, |a| *a > 0)
        .unwrap();
    drop(guard);
    assert!(
        !timed_out.timed_out(),
        "processes did not finish within {timeout:?}"
    );
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
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
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn kill_process_reaps_parked_and_sleeping_processes() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "killer.js",
        r#"
            const parked = spawn(`await recv();`);
            const sleeper = spawn(`await sleep(60000);`);
            await sleep(100); // let both park
            if (processCount() !== 3) throw new Error("expected 3 processes, got " + processCount());
            const listed = listProcesses().map(p => p.pid);
            if (!listed.includes(parked) || !listed.includes(sleeper)) {
                throw new Error("children not listed: " + JSON.stringify(listed));
            }
            if (!killProcess(parked)) throw new Error("kill parked failed");
            if (!killProcess(sleeper)) throw new Error("kill sleeper failed");
            await sleep(50); // let the reaps happen
            if (isProcessAlive(parked) || isProcessAlive(sleeper)) {
                throw new Error("killed process still alive");
            }
            if (listProcesses().some(p => p.pid === parked || p.pid === sleeper)) {
                throw new Error("killed process still listed");
            }
            if (processCount() !== 1) throw new Error("expected only self, got " + processCount());
            send(parked, "hello"); // to a dead pid: dropped silently
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn kill_self_terminates_current_process() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "selfkill.js",
        r#"
            const me = self();
            if (!killProcess(me)) throw new Error("self kill failed");
            if (!isProcessAlive(me)) throw new Error("self should still be listed until reaped");
            // Reaped at the next scheduling boundary: this suspension never
            // resolves and the process dies instead of parking.
            await recv();
            console.log("should never print");
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn kill_unknown_pid_is_false_and_info_is_null() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "info.js",
        r#"
            if (processInfo(99999) !== null) throw new Error("expected null info");
            if (isProcessAlive(99999)) throw new Error("unknown pid reported alive");
            if (killProcess(99999)) throw new Error("kill of unknown pid should be false");
            const info = processInfo(self());
            if (info.pid !== self()) throw new Error("wrong pid in info");
            if (typeof info.name !== "string" || typeof info.status !== "string") {
                throw new Error("bad info shape");
            }
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn set_name_updates_process_info_and_list() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "rename.js",
        r#"
            setName("renamed-self");
            if (processInfo(self()).name !== "renamed-self") throw new Error("info name not updated");
            const listed = listProcesses().find(p => p.pid === self());
            if (!listed || listed.name !== "renamed-self") throw new Error("list name not updated");
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn confined_child_cannot_spawn() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "parent.js",
        r#"
            // Privileged root spawns a confined child.
            const child = spawn(
                `
                    try {
                        spawn("send(0, 'should not happen');");
                        throw new Error("spawn should have thrown");
                    } catch (e) {
                        if (!(e instanceof PermissionError)) {
                            throw new Error("expected PermissionError, got: " + e);
                        }
                        send(0, "blocked");
                    }
                `,
                { sandbox: { canSpawnAndKill: false } },
            );
            const msg = await recv();
            if (msg !== "blocked") throw new Error("unexpected: " + msg);
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn confined_child_cannot_kill_others() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "parent.js",
        r#"
            const victim = spawn(`await recv();`); // privileged, parks
            await sleep(50); // let it park
            const confined = spawn(
                `
                    try {
                        killProcess(VICTIM);
                        throw new Error("kill should have thrown");
                    } catch (e) {
                        if (!(e instanceof PermissionError)) {
                            throw new Error("expected PermissionError, got: " + e);
                        }
                        send(0, "blocked");
                    }
                `.replace("VICTIM", victim),
                { sandbox: { canSpawnAndKill: false } },
            );
            const msg = await recv();
            if (msg !== "blocked") throw new Error("unexpected: " + msg);
            // Victim is still alive: the confined kill was blocked outright.
            if (!isProcessAlive(victim)) throw new Error("victim should still be alive");
            killProcess(victim); // privileged parent cleans up
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn confined_child_can_kill_self() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "parent.js",
        r#"
            const child = spawn(
                `
                    if (!killProcess(self())) throw new Error("self-kill denied");
                    await recv(); // never resolves; reaped at next boundary
                    send(0, "should never happen");
                `,
                { sandbox: { canSpawnAndKill: false } },
            );
            await sleep(80);
            if (isProcessAlive(child)) throw new Error("confined self-kill did not reap");
            send(0, "ok"); // wake parent's own recv below
            await recv();
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn selfsandbox_reports_current_policy() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            if (selfSandbox().canSpawnAndKill !== true) {
                throw new Error("root should be privileged");
            }
            const child = spawn(
                `
                    const sb = selfSandbox();
                    if (sb.canSpawnAndKill !== false) throw new Error("child should be confined");
                    send(0, "reported");
                `,
                { sandbox: { canSpawnAndKill: false } },
            );
            const msg = await recv();
            if (msg !== "reported") throw new Error("unexpected: " + msg);
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn restrictsandbox_self_narrows_irrevocably() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            // Privileged root self-confines, then proves it cannot escape.
            const after = restrictSandbox({ canSpawnAndKill: false });
            if (after.canSpawnAndKill !== false) throw new Error("restrict did not narrow");
            if (selfSandbox().canSpawnAndKill !== false) throw new Error("selfSandbox disagrees");

            // Re-granting is a silent no-op (intersection): confinement is
            // irrevocable in effect.
            const still = restrictSandbox({ canSpawnAndKill: true });
            if (still.canSpawnAndKill !== false) throw new Error("re-grant widened!");
            if (selfSandbox().canSpawnAndKill !== false) throw new Error("selfSandbox widened!");

            // Spawn is now blocked.
            try {
                spawn("send(0,'x');");
                throw new Error("spawn after restrict should throw");
            } catch (e) {
                if (!(e instanceof PermissionError)) {
                    throw new Error("expected PermissionError, got: " + e);
                }
            }
            send(0, "done");
            await recv();
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn restrictsandbox_cross_target_requires_privilege_and_narrows() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            // A privileged child that, on request, tries to spawn and reports
            // whether it was blocked.
            const victim = spawn(`
                const cmd = await recv();
                if (cmd === "try-spawn") {
                    try {
                        spawn("send(0, 'should not happen');");
                        send(0, "leaked");
                    } catch (e) {
                        send(0, e instanceof PermissionError ? "blocked" : ("other:" + e));
                    }
                }
            `);
            await sleep(50); // let it park

            // Privileged parent narrows the live victim.
            const after = restrictSandbox({ canSpawnAndKill: false }, { pid: victim });
            if (after.canSpawnAndKill !== false) throw new Error("victim not narrowed");

            // Victim now cannot spawn: ask it to try and report back.
            send(victim, "try-spawn");
            const reply = await recv();
            if (reply !== "blocked") throw new Error("unexpected: " + reply);

            // A confined process cannot narrow another process.
            const confined = spawn(
                `
                    try {
                        restrictSandbox({ canSpawnAndKill: false }, { pid: VICTIM });
                        send(0, "leaked");
                    } catch (e) {
                        if (!(e instanceof PermissionError)) {
                            send(0, "wrong-error:" + e);
                        } else {
                            send(0, "denied");
                        }
                    }
                `.replace("VICTIM", victim),
                { sandbox: { canSpawnAndKill: false } },
            );
            const r2 = await recv();
            if (r2 !== "denied") throw new Error("confined cross-restrict not denied: " + r2);

            // Cross-target without an actual narrowing (pure read / widen
            // attempt) is a TypeError, not a permission issue.
            const confined2 = spawn(
                `
                    try {
                        restrictSandbox({}, { pid: VICTIM });
                        send(0, "read-leaked");
                    } catch (e) {
                        send(0, e instanceof TypeError ? "typeerror" : ("other:" + e));
                    }
                `.replace("VICTIM", victim),
                { sandbox: { canSpawnAndKill: false } },
            );
            const r3 = await recv();
            if (r3 !== "typeerror") throw new Error("empty cross-policy should be TypeError: " + r3);

            killProcess(victim);
            killProcess(confined);
            killProcess(confined2);
            send(0, "done");
            await recv();
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn restrictsandbox_unknown_target_is_typeerror_after_privilege() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            // Privileged caller, dead target -> TypeError (bad arg).
            try {
                restrictSandbox({ canSpawnAndKill: false }, { pid: 99999 });
                throw new Error("expected TypeError");
            } catch (e) {
                if (!(e instanceof TypeError)) throw new Error("expected TypeError, got: " + e);
            }

            // Confined caller, dead target -> PermissionError (privilege first).
            const confined = spawn(
                `
                    try {
                        restrictSandbox({ canSpawnAndKill: false }, { pid: 99999 });
                        send(0, "leaked");
                    } catch (e) {
                        send(0, e instanceof PermissionError ? "denied" : ("other:" + e));
                    }
                `,
                { sandbox: { canSpawnAndKill: false } },
            );
            const r = await recv();
            if (r !== "denied") throw new Error("confined should get PermissionError first: " + r);
            killProcess(confined);
            send(0, "done");
            await recv();
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn spawn_inherits_parent_sandbox_by_default() {
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    // A confined root: its children inherit confinement even without an
    // explicit override (no-escalation by inheritance).
    spawn_process(
        &world,
        "confined-root.js",
        r#"
            if (selfSandbox().canSpawnAndKill !== false) throw new Error("root should be confined");
            // Cannot spawn at all from a confined process.
            try {
                spawn("send(0,'x');");
                throw new Error("spawn should throw");
            } catch (e) {
                if (!(e instanceof PermissionError)) throw new Error("expected PermissionError: " + e);
            }
            send(0, "done");
            await recv();
        "#,
        Sandbox::CONFINED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn confined_process_can_still_send_recv_and_sleep() {
    // The sandbox gates only spawn/killProcess(other); message passing,
    // sleep, and yieldNow must keep working for a confined process.
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            const child = spawn(
                `
                    setName("confined");
                    // sleep and recv both work.
                    await sleep(30);
                    send(0, "ping");
                    const reply = await recv();
                    if (reply !== "pong") throw new Error("unexpected: " + reply);
                    send(0, "child-done");
                `,
                { sandbox: { canSpawnAndKill: false } },
            );
            const ping = await recv();
            if (ping !== "ping") throw new Error("unexpected: " + ping);
            send(child, "pong");
            const done = await recv();
            if (done !== "child-done") throw new Error("unexpected: " + done);
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn spawn_explicit_true_is_noop_child_still_privileged() {
    // `{ sandbox: { canSpawnAndKill: true } }` is a no-op intersect: the
    // child stays privileged and can itself spawn. The grandchild's report
    // and the child's report both arrive at the root; order is not fixed, so
    // collect both.
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            const child = spawn(
                `
                    if (selfSandbox().canSpawnAndKill !== true) throw new Error("should be privileged");
                    const gc = spawn("send(0, 'grandchild-up');");
                    send(0, "child-spawned:" + gc);
                    await recv();
                `,
                { sandbox: { canSpawnAndKill: true } },
            );
            const m1 = await recv();
            const m2 = await recv();
            const msgs = [m1, m2].sort();
            // Order-agnostic: both messages must be present.
            const hasGc = msgs.includes("grandchild-up");
            const hasChild = msgs.some((m) => m.startsWith("child-spawned:"));
            if (!hasGc || !hasChild) throw new Error("missing reports: " + msgs);
            send(child, "bye");
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn spawn_with_empty_or_partial_sandbox_inherits() {
    // Empty opts, empty sandbox object, and an unknown key are all treated
    // as "inherit": the child is privileged and can spawn.
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            const a = spawn("send(0,'a');");                  // no opts
            const b = spawn("send(0,'b');", {});              // empty opts
            const c = spawn("send(0,'c');", { sandbox: {} }); // empty sandbox
            const d = spawn("send(0,'d');", { sandbox: { unknownKey: 1 } }); // unknown key
            const got = [];
            for (let i = 0; i < 4; i++) got.push(await recv());
            got.sort();
            if (got.join(",") !== "a,b,c,d") throw new Error("missing children: " + got);
            // Each child inherited privilege.
            for (const pid of [a, b, c, d]) {
                send(pid, "go");
            }
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn restrictsandbox_empty_or_omitted_is_pure_read() {
    // `restrictSandbox()` and `restrictSandbox({})` on a privileged process
    // return the current state unchanged and leave spawn working.
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            const a = restrictSandbox();
            if (a.canSpawnAndKill !== true) throw new Error("pure read should be true");
            const b = restrictSandbox({});
            if (b.canSpawnAndKill !== true) throw new Error("empty policy read should be true");
            // Explicit true on self is also a no-op (never widens, but it
            // wasn't narrow to begin with).
            const c = restrictSandbox({ canSpawnAndKill: true });
            if (c.canSpawnAndKill !== true) throw new Error("true should be no-op");
            // Spawn still works.
            const child = spawn("send(0, 'ok');");
            if ((await recv()) !== "ok") throw new Error("spawn broke after pure read");
            send(child, "bye");
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn confinement_gates_caller_not_target() {
    // A confined process cannot be killed by another confined process, but a
    // privileged caller can kill a confined target. The sandbox describes
    // what the *caller* may do, not what may be done to the target.
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            const target = spawn(
                `await recv();`,
                { sandbox: { canSpawnAndKill: false } },
            );
            await sleep(50); // let it park
            if (!isProcessAlive(target)) throw new Error("target died early");

            // A confined attacker cannot kill the confined target.
            const attacker = spawn(
                `
                    try {
                        killProcess(TARGET);
                        send(0, "attacker-leaked");
                    } catch (e) {
                        send(0, e instanceof PermissionError ? "attacker-blocked" : ("other:" + e));
                    }
                `.replace("TARGET", target),
                { sandbox: { canSpawnAndKill: false } },
            );
            const r = await recv();
            if (r !== "attacker-blocked") throw new Error("attacker should be blocked: " + r);
            if (!isProcessAlive(target)) throw new Error("target died from a blocked kill");

            // A privileged caller CAN kill the confined target.
            if (!killProcess(target)) throw new Error("privileged kill of confined target failed");
            await sleep(50);
            if (isProcessAlive(target)) throw new Error("confined target survived privileged kill");
            killProcess(attacker);
            send(0, "done");
            await recv();
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn self_restrict_then_self_kill_still_allowed() {
    // Self-kill is never gated by the sandbox, even after self-restriction
    // drops canSpawnAndKill. The setup-then-die lifecycle must work.
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            const child = spawn(`
                restrictSandbox({ canSpawnAndKill: false });
                if (selfSandbox().canSpawnAndKill !== false) throw new Error("not confined");
                // Self-kill still allowed despite confinement.
                if (!killProcess(self())) throw new Error("self-kill denied after restrict");
                await recv(); // never resolves; reaped at next boundary
                send(0, "should never happen");
            `);
            await sleep(80);
            if (isProcessAlive(child)) throw new Error("confined self-kill didn't reap");
            send(0, "done");
            await recv();
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}

#[test]
fn sandbox_inherits_through_privileged_chain() {
    // Multi-level inheritance: privileged root → privileged child (explicit
    // true) → confined grandchild (explicit false). The grandchild is
    // confined and cannot spawn; the middle child stays privileged.
    let world = World::new();
    let worker = {
        let w = world.clone();
        std::thread::spawn(move || worker_loop(w))
    };

    spawn_process(
        &world,
        "root.js",
        r#"
            const child = spawn(`
                if (selfSandbox().canSpawnAndKill !== true) throw new Error("child should be privileged");
                // Spawn a confined grandchild. Use a double-quoted one-liner
                // (no backticks) so it doesn't close this template literal; use
                // a numeric probe message so no nested string quotes are
                // needed (backtick template literals eat backslash-escapes).
                const gc = spawn("if(selfSandbox().canSpawnAndKill!==false)throw new Error('gc should be confined');try{spawn('send(0,42)');send(0,'gc-leaked')}catch(e){send(0,e instanceof PermissionError?'gc-blocked':('other:'+e))};await recv();", { sandbox: { canSpawnAndKill: false } });
                // The grandchild reports back directly to root (pid 0) and is
                // now parked on its own await recv(). This child waits to be
                // told the confinement held, then wakes the grandchild with
                // "bye" so it can finish.
                const ok = await recv();
                if (ok !== "bye") throw new Error("child expected a bye from root: " + ok);
                send(gc, "bye");
            `, { sandbox: { canSpawnAndKill: true } });
            const r = await recv();
            if (r !== "gc-blocked") throw new Error("grandchild should be confined: " + r);
            send(child, "bye");
        "#,
        Sandbox::PRIVILEGED,
    )
    .unwrap();

    wait_for_completion(&world, Duration::from_secs(10));
    let _ = world.queue_tx.send(RunItem::Stop);
    worker.join().unwrap();
}
