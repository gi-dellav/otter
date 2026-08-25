// Per-process sandboxing: spawn confined children, self-restrict, and prove
// that confinement is irrevocable (no escalation).
//
//   otter examples/sandbox.js

setName("root");

// 1. A privileged root spawns a confined child. The child cannot spawn or
//    kill *other* processes, but it can still kill itself and send/recv.
const child = spawn(
  `
    setName("confined");
    if (selfSandbox().canSpawnAndKill !== false) {
      throw new Error("child should be confined");
    }
    // spawn is blocked.
    try {
      spawn("send(0, 'leaked');");
      send(0, "spawn-leaked");
    } catch (e) {
      send(0, "spawn-blocked:" + e.name); // PermissionError
    }
    // killing another process is blocked; killing self is allowed.
    try {
      killProcess(0); // the root
      send(0, "kill-leaked");
    } catch (e) {
      send(0, "kill-blocked:" + e.name); // PermissionError
    }
    send(0, "child-ready");
    await recv(); // park until the root lets us finish
  `,
  { sandbox: { canSpawnAndKill: false } },
);

console.log("from child:", await recv()); // spawn-blocked:PermissionError
console.log("from child:", await recv()); // kill-blocked:PermissionError
console.log("from child:", await recv()); // child-ready

// 2. Setup-then-drop: a privileged process self-restricts, then proves it
//    cannot escape. This is the pledge/seccomp-style lifecycle.
const privileged = spawn(`
  // Set up while privileged: spawn a harmless worker.
  spawn("send(0, 'worker-alive');");
  // Now drop the toggle, irrevocably.
  const after = restrictSandbox({ canSpawnAndKill: false });
  send(0, "after-restrict:" + JSON.stringify(after));
  // Re-granting is a silent no-op (intersection): confinement sticks.
  const still = restrictSandbox({ canSpawnAndKill: true });
  send(0, "after-regrant:" + JSON.stringify(still));
  // Spawn is now blocked.
  try {
    spawn("send(0, 'should not happen');");
    send(0, "post-restrict-leaked");
  } catch (e) {
    send(0, "post-restrict-blocked:" + e.name);
  }
  await recv();
`);
console.log("from privileged:", await recv()); // worker-alive (from the worker)
console.log("from privileged:", await recv()); // after-restrict:{"canSpawnAndKill":false}
console.log("from privileged:", await recv()); // after-regrant:{"canSpawnAndKill":false}
console.log("from privileged:", await recv()); // post-restrict-blocked:PermissionError
send(privileged, "bye");

// 3. Cross-target restriction: a privileged parent narrows a live child, then
//    asks it to try to spawn (which is now blocked).
const victim = spawn(`
  let done = false;
  while (!done) {
    const cmd = await recv();
    if (cmd === "try-spawn") {
      try {
        spawn("send(0, 'should not happen');");
        send(0, "victim-spawn-leaked");
      } catch (e) {
        send(0, "victim-spawn-blocked:" + e.name);
      }
    } else if (cmd === "bye") {
      done = true;
    }
  }
`);
await sleep(50); // let it park

const narrowed = restrictSandbox({ canSpawnAndKill: false }, { pid: victim });
console.log("narrowed victim to:", JSON.stringify(narrowed));

send(victim, "try-spawn");
console.log("from victim:", await recv()); // victim-spawn-blocked:PermissionError
send(victim, "bye");

// Let the confined child finish too.
send(child, "bye");
console.log("done");
