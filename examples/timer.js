// Timers: a watchdog pattern with recv(timeoutMs), and a plain sleep()
// countdown. pid 0 pings two workers and waits for their replies; the slow
// worker misses the deadline and the recv rejects with a TimeoutError.

const fast = spawn(`
  await sleep(50);
  send(0, { from: self(), result: "fast" });
`);

const slow = spawn(`
  await sleep(500);
  send(0, { from: self(), result: "slow" });
`);

for (let i = 0; i < 2; i++) {
  try {
    const reply = await recv(200);
    console.log("replied in time:", reply.result);
  } catch (e) {
    console.log("timed out waiting for a reply:", e.name);
  }
}

// Plain sleep: a countdown.
for (let i = 3; i > 0; i--) {
  await sleep(100);
  console.log("t-minus", i);
}
console.log("launch");
