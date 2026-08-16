// Two processes interleaving via yieldNow(): each yield re-queues the process
// at the back of the run queue, giving other runnable processes a turn. The
// exact interleaving depends on when each process becomes runnable.

spawn(`
  for (let i = 1; i <= 3; i++) {
    console.log("B step", i);
    await yieldNow();
  }
  console.log("B done");
`);

for (let i = 1; i <= 3; i++) {
  console.log("A step", i);
  await yieldNow();
}
console.log("A done");
