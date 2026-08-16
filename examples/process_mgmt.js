// Process management: spawn, list, inspect, rename, and kill.
//
//   otter examples/process_mgmt.js

setName("manager");

const workers = [];
for (let i = 0; i < 3; i++) {
  workers.push(spawn(`await recv();`)); // park until a message (or death) arrives
}
await sleep(50); // let them park

console.log("processCount:", processCount());
console.log("processes:");
for (const p of listProcesses()) {
  console.log(`  pid ${p.pid} (${p.name}) — ${p.status}`);
}

console.log("info about self:", JSON.stringify(processInfo(self())));
console.log("is worker 0 alive?", isProcessAlive(workers[0]));

// Kill two workers, keep one as the survivor.
console.log("kill", workers[0], "->", killProcess(workers[0]));
console.log("kill", workers[1], "->", killProcess(workers[1]));
console.log("kill unknown pid 999 ->", killProcess(999));

await sleep(50);
console.log("alive after kills:", listProcesses().map((p) => p.pid));

send(workers[2], "bye"); // wakes the survivor, which then finishes
console.log("done");
