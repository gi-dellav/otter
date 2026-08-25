//! External control of the process tree over a JSON-RPC 2.0 TCP socket.
//!
//! This lets a tool outside the runtime drive the scheduler exactly like the
//! in-JS API does:
//!
//! - `list`          -> `[{pid, name, status}, ...]`
//! - `info` {pid}    -> process detail or `null`
//! - `count`         -> number of live processes
//! - `spawn` {name?, code} -> new pid
//! - `kill` {pid}    -> `true`/`false` (best-effort, like `killProcess`)
//! - `send` {pid, value}   -> deliver a JSON value to a mailbox
//! - `rename` {pid, name}  -> `true`/`false`
//! - `shutdown`      -> stops the control server and the runtime
//!
//! Line-delimited JSON-RPC 2.0 frames: one request per line. Requests carry a
//! numeric/string `id` and receive a matching response; notifications (no
//! `id`) get none, per the JSON-RPC 2.0 spec.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};

use crate::process::Sandbox;
use crate::scheduler::World;

/// Bind the control listener. `port == 0` asks the OS for an ephemeral port.
pub fn bind(host: &str, port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind((host, port))
}

/// Accept connections until `shutdown` flips. Each accepted connection is
/// handled on its own detached thread, so a single idle or misbehaving client
/// (one that connects and never sends a line) can never block the accept loop
/// or the control surface for other clients. The JSON-RPC dispatch itself is
/// serialized on the shared `World` locks, so per-connection threads remain
/// race-free with the scheduler.
pub fn serve_loop(listener: TcpListener, world: Arc<World>, shutdown: Arc<AtomicBool>) {
    listener
        .set_nonblocking(true)
        .expect("failed to set nonblocking");
    // Cap the number of live connection threads so a client cannot exhaust
    // file descriptors / threads by opening unbounded connections.
    let mut live: Vec<std::thread::JoinHandle<()>> = Vec::new();
    let max_conns = 256;
    loop {
        // Reap finished connection threads so `live` stays a true count.
        live.retain(|h| !h.is_finished());
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        if live.len() >= max_conns {
            std::thread::sleep(std::time::Duration::from_millis(50));
            continue;
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                let w = world.clone();
                let s = shutdown.clone();
                let handle = std::thread::Builder::new()
                    .name("otter-rpc-conn".to_string())
                    .spawn(move || handle_conn(stream, &w, &s))
                    .expect("failed to spawn rpc connection thread");
                live.push(handle);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(e) => {
                eprintln!("otter: rpc accept error: {e}");
                return;
            }
        }
    }
}

fn handle_conn(mut stream: TcpStream, world: &Arc<World>, shutdown: &Arc<AtomicBool>) {
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(3600)));
    let mut reader = BufReader::new(stream.try_clone().expect("dup stream"));
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return, // client closed
            Ok(_) => {}
            Err(_) => return,
        }
        let req: Value = match serde_json::from_str(line.trim()) {
            Ok(v) => v,
            Err(e) => {
                let resp = json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": format!("parse error: {e}") } });
                let _ = write_all_json(&mut stream, &resp);
                continue;
            }
        };
        // A request without an `id` is a JSON-RPC notification and gets no
        // response, so the framing loop can keep reading without a reply to
        // consume. (A malformed payload always draws an error, id or not.)
        let is_notification = req.get("id").is_none();
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req.get("method").and_then(Value::as_str).unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(json!({}));
        let resp = dispatch(method, &params, world, shutdown);
        if !is_notification {
            let _ = write_all_json(&mut stream, &resolve(&req, id, resp));
        }
        if method == "shutdown" {
            return;
        }
    }
}

/// `dispatch` returns either a `result` or an `error` object; `resolve` wraps
/// it in a JSON-RPC response envelope.
fn dispatch(
    method: &str,
    params: &Value,
    world: &Arc<World>,
    shutdown: &Arc<AtomicBool>,
) -> Result<Value, Value> {
    match method {
        "list" => Ok(json!(
            crate::scheduler::list_processes(world)
                .into_iter()
                .map(|(pid, name, status)| json!({ "pid": pid, "name": name, "status": status }))
                .collect::<Vec<_>>()
        )),
        "count" => Ok(json!(crate::scheduler::process_count(world))),
        "info" => {
            let pid = params.get("pid").and_then(Value::as_u64).unwrap_or(u64::MAX);
            match crate::scheduler::process_info(world, pid) {
                Some((pid, name, status)) => Ok(json!({ "pid": pid, "name": name, "status": status })),
                None => Ok(Value::Null),
            }
        }
        "spawn" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("rpc");
            let code = match params.get("code").and_then(Value::as_str) {
                Some(c) => c.to_string(),
                None => return Err(json!({ "code": -32602, "message": "missing string `code`" })),
            };
            match crate::scheduler::spawn_process(world, name, &code, Sandbox::PRIVILEGED) {
                Ok(pid) => Ok(json!({ "pid": pid })),
                Err(e) => Err(json!({ "code": -32000, "message": e })),
            }
        }
        "kill" => {
            let pid = params.get("pid").and_then(Value::as_u64).unwrap_or(u64::MAX);
            Ok(json!(crate::scheduler::kill_process(world, pid)))
        }
        "send" => {
            let pid = params.get("pid").and_then(Value::as_u64).unwrap_or(u64::MAX);
            let value = params.get("value").cloned().unwrap_or(Value::Null);
            // The value was already JSON on the wire; pass it through
            // pre-serialized so no QuickJS context is needed here.
            let json = serde_json::to_string(&value).unwrap_or_else(|_| "null".into());
            crate::scheduler::deliver_json_string(world, pid, json);
            Ok(json!(true))
        }
        "rename" => {
            let pid = params.get("pid").and_then(Value::as_u64).unwrap_or(u64::MAX);
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            if crate::scheduler::is_process_alive(world, pid) {
                crate::scheduler::set_process_name(world, pid, name.to_string());
                Ok(json!(true))
            } else {
                Ok(json!(false))
            }
        }
        "shutdown" => {
            shutdown.store(true, Ordering::Relaxed);
            Ok(json!(true))
        }
        _ => Err(json!({ "code": -32601, "message": format!("method not found: {method}") })),
    }
}

fn resolve(_req: &Value, id: Value, resp: Result<Value, Value>) -> Value {
    match resp {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": error }),
    }
}

fn write_all_json(stream: &mut TcpStream, value: &Value) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(value)?;
    buf.push(b'\n');
    stream.write_all(&buf)
}
