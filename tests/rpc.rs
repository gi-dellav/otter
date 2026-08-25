//! End-to-end JSON-RPC control-server tests: a real scheduler and worker,
//! driven over a loopback TCP socket exactly as a `nc`/`curl` client would.
//!
//! Each test boots its own `World` + worker + RPC server on an ephemeral
//! port, drives it with JSON-RPC requests, and tears everything down.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use otter::scheduler::{RunItem, World, worker_loop};
use serde_json::{json, Value};

/// Live server + worker under test.
struct Harness {
    world: Arc<World>,
    worker: std::thread::JoinHandle<()>,
    port: u16,
    shutdown: Arc<AtomicBool>,
    server: std::thread::JoinHandle<()>,
}

/// Bind the RPC server on an ephemeral port and start the accept + worker
/// threads.
fn start_harness() -> Harness {
    let world = World::new();

    let w = world.clone();
    let worker = std::thread::spawn(move || worker_loop(w));

    let listener = otter::rpc::bind("127.0.0.1", 0).expect("bind rpc ephemeral");
    let port = ephemeral_port(&listener);
    let shutdown = Arc::new(AtomicBool::new(false));

    let w2 = world.clone();
    let s2 = shutdown.clone();
    let server = std::thread::spawn(move || otter::rpc::serve_loop(listener, w2, s2));

    Harness { world, worker, port, shutdown, server }
}

/// Stop the worker (so it exits) and ask the RPC server to shut down.
fn teardown(h: Harness) {
    h.shutdown.store(true, Ordering::Relaxed);
    let _ = h.server.join();
    let _ = h.world.queue_tx.send(RunItem::Stop);
    let _ = h.worker.join();
}

/// Read the ephemeral port out of a bound `TcpListener`.
fn ephemeral_port(listener: &TcpListener) -> u16 {
    let s = format!("{}", listener.local_addr().unwrap());
    s.split(":").last().unwrap().parse::<u16>().unwrap()
}

/// Open a client connection to the harnessed server, send one JSON-RPC
/// request (single `id`), and read one response line.
fn request_rpc(port: u16, method: &str, params: Value) -> Value {
    let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), port);
    let mut sock = TcpStream::connect(addr).expect("connect to rpc server");
    let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));

    let req = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let mut bytes = serde_json::to_vec(&req).unwrap();
    bytes.push(b'\n');
    let _ = sock.write_all(&bytes).unwrap();

    let mut reader = BufReader::new(sock.try_clone().unwrap());
    let mut line = String::new();
    let _ = reader.read_line(&mut line).unwrap();
    serde_json::from_str::<Value>(&line).unwrap()
}

/// Like `request_rpc`, but send a raw line verbatim (to exercise malformed
/// input and notifications).
fn request_raw(port: u16, raw: &str, timeout: Duration) -> std::io::Result<String> {
    let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), port);
    let mut sock = TcpStream::connect(addr)?;
    let _ = sock.set_read_timeout(Some(timeout));
    let payload = format!("{raw}\n");
    let _ = sock.write_all(payload.as_bytes())?;
    let mut reader = BufReader::new(sock.try_clone().unwrap());
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(_) => Ok(line.to_owned()),
        Err(e) => Err(e),
    }
}

/// Poll `count` until it hits `0` (the callbacks are async: a `send` wakes a
/// parked process and a worker then runs its next slice).
fn wait_until(h: &Harness, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let resp = request_rpc(h.port, "count", json!({}));
        let count = resp.get("result").unwrap().as_u64().unwrap();
        if count == 0 {
            return;
        }
        if Instant::now() > deadline {
            assert!(false, "processes did not finish before timeout");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn rpc_list_reports_spawned_process() {
    let h = start_harness();

    let resp = request_rpc(h.port, "spawn", json!({
        "name": "waiter.js",
        "code": "await recv();",
    }));
    let pid = resp.get("result").unwrap().get("pid").unwrap().as_u64().unwrap();
    assert_eq!(pid, 0, "first spawned process is pid 0");

    let list = request_rpc(h.port, "list", json!({}));
    let arr = list.get("result").unwrap().as_array().unwrap();
    assert_eq!(arr.len(), 1, "one live process");
    let first = arr.first().unwrap();
    assert_eq!(first.get("pid").unwrap().as_u64().unwrap(), 0);
    assert_eq!(first.get("name").unwrap().as_str().unwrap().to_owned(), "waiter.js");
    assert_eq!(first.get("status").unwrap().as_str().unwrap().to_owned(), "waiting");

    teardown(h);
}

#[test]
fn rpc_send_wakes_parked_process_and_it_finishes() {
    let h = start_harness();
    // A process parked on `await recv()`, then returns (done) once it wakes.
    let _ = request_rpc(h.port, "spawn", json!({ "code": "await recv();" }));

    // Nothing else can reach it yet: it stays parked and alive.
    let resp = request_rpc(h.port, "count", json!({}));
    assert_eq!(resp.get("result").unwrap().as_u64().unwrap(), 1);

    // An external `send` delivers the message; the parked process wakes, runs
    // its single `recv` slice, and the entry script returns -> finished.
    let resp = request_rpc(h.port, "send", json!({ "pid": 0, "value": "wake-up" }));
    assert_eq!(resp.get("result").unwrap().as_bool().unwrap(), true);

    wait_until(&h, Duration::from_secs(5));
    teardown(h);
}

#[test]
fn rpc_kill_reaps_a_sleeping_process() {
    let h = start_harness();
    let _ = request_rpc(h.port, "spawn", json!({ "code": "await sleep(60000);" }));

    let count = request_rpc(h.port, "count", json!({}));
    assert_eq!(count.get("result").unwrap().as_u64().unwrap(), 1);

    let resp = request_rpc(h.port, "kill", json!({ "pid": 0 }));
    assert_eq!(resp.get("result").unwrap().as_bool().unwrap(), true);

    wait_until(&h, Duration::from_secs(5));
    teardown(h);
}

#[test]
fn rpc_rename_is_visible_in_list() {
    let h = start_harness();
    let _ = request_rpc(h.port, "spawn", json!({ "name": "orig.js", "code": "await sleep(60000);" }));

    let resp = request_rpc(h.port, "rename", json!({ "pid": 0, "name": "renamed.js" }));
    assert_eq!(resp.get("result").unwrap().as_bool().unwrap(), true);

    let list = request_rpc(h.port, "list", json!({}));
    let arr = list.get("result").unwrap().as_array().unwrap();
    let first = arr.first().unwrap();
    assert_eq!(first.get("name").unwrap().as_str().unwrap().to_owned(), "renamed.js");

    teardown(h);
}

#[test]
fn rpc_unknown_pid_yields_null_and_false() {
    let h = start_harness();

    // info for a pid that never existed -> null result.
    let resp = request_rpc(h.port, "info", json!({ "pid": 42 }));
    assert_eq!(resp.get("result").unwrap().as_null().is_some(), true);

    // kill/rename for unknown -> false.
    let resp = request_rpc(h.port, "kill", json!({ "pid": 42 }));
    assert_eq!(resp.get("result").unwrap().as_bool().unwrap(), false);
    let resp = request_rpc(h.port, "rename", json!({ "pid": 42, "name": "x" }));
    assert_eq!(resp.get("result").unwrap().as_bool().unwrap(), false);

    teardown(h);
}

#[test]
fn rpc_count_tracks_player() {
    let h = start_harness();
    assert_eq!(request_rpc(h.port, "count", json!({})).get("result").unwrap().as_u64().unwrap(), 0);

    let _ = request_rpc(h.port, "spawn", json!({ "code": "await recv();" }));
    assert_eq!(request_rpc(h.port, "count", json!({})).get("result").unwrap().as_u64().unwrap(), 1);

    let _ = request_rpc(h.port, "spawn", json!({ "code": "await recv();" }));
    assert_eq!(request_rpc(h.port, "count", json!({})).get("result").unwrap().as_u64().unwrap(), 2);

    teardown(h);
}

#[test]
fn rpc_method_not_found_returns_32601() {
    let h = start_harness();
    let resp = request_rpc(h.port, "bogusMethod", json!({}));
    let err = resp.get("error").unwrap();
    assert_eq!(err.get("code").unwrap().as_i64().unwrap(), -32601);
    teardown(h);
}

#[test]
fn rpc_malformed_json_returns_parse_error() {
    let h = start_harness();
    let resp = request_raw(h.port, "this is not json", Duration::from_secs(5)).unwrap();
    let v = serde_json::from_str::<Value>(&resp).unwrap();
    let err = v.get("error").unwrap();
    assert_eq!(err.get("code").unwrap().as_i64().unwrap(), -32700);
    teardown(h);
}

#[test]
fn rpc_missing_required_param_is_invalid_params() {
    let h = start_harness();
    // spawn with no `code` string.
    let resp = request_rpc(h.port, "spawn", json!({ "name": "x" }));
    let err = resp.get("error").unwrap();
    assert_eq!(err.get("code").unwrap().as_i64().unwrap(), -32602);
    teardown(h);
}

#[test]
fn rpc_notification_gets_no_response() {
    let h = start_harness();
    // A request with no `id` is a JSON-RPC notification: the server must not
    // reply. A short read timeout proves silence rather than blocking.
    let resp: std::io::Result<String> = request_raw(h.port, r#"{"jsonrpc":"2.0","method":"count"}"#, Duration::from_millis(300));
    assert!(resp.is_err(), "notification must not produce a response");
    teardown(h);
}

#[test]
fn rpc_shutdown_stops_the_server() {
    let h = start_harness();
    let resp = request_rpc(h.port, "shutdown", json!({}));
    assert_eq!(resp.get("result").unwrap().as_bool().unwrap(), true);

    // The server thread returns once it observes the shutdown flag.
    let _ = h.server.join();
    // A fresh connection is now refused (the listener is closed).
    let resp: std::io::Result<String> = request_raw(h.port, r#"{"jsonrpc":"2.0","method":"count","id":1}"#, Duration::from_secs(2));
    assert!(resp.is_err(), "server socket should be closed after shutdown");

    let _ = h.world.queue_tx.send(RunItem::Stop);
    let _ = h.worker.join();
}

#[test]
fn rpc_idle_client_does_not_block_other_clients() {
    // Regression: connections are handled on separate threads, so a client
    // that connects and sends nothing (blocking on its own read) must not
    // stall the accept loop or another well-behaved client. Before the
    // thread-per-connection change this would hang until the idle client hit
    // its long read timeout.
    let h = start_harness();

    // A stuck client: connect and never send a line. Its connection thread
    // blocks in read() waiting for input that never arrives.
    let _stuck = std::net::TcpStream::connect(SocketAddr::new("127.0.0.1".parse().unwrap(), h.port))
        .expect("stuck client connect");

    // A second client should still be served promptly rather than queued
    // behind the stuck one.
    let started = std::time::Instant::now();
    let resp = request_rpc(h.port, "count", json!({}));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "count was served, but slower than a blocked accept-loop would allow"
    );
    assert_eq!(resp.get("result").unwrap().as_u64().unwrap(), 0);

    teardown(h);
}