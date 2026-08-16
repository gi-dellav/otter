// A ring of SIZE processes: each process forwards a token to the next one.
// pid 0 builds the ring tail-first, injects the token, and reports the trip.

const SIZE = 1000;

let next = self();
for (let i = SIZE - 1; i > 0; i--) {
  const target = next;
  next = spawn(`
    const n = await recv();
    send(${target}, n + 1);
  `);
}

const t0 = Date.now();
send(next, 0);
const hops = await recv();
console.log(`token passed ${hops} processes in ${Date.now() - t0} ms (ring size ${SIZE})`);
