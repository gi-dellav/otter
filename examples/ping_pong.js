// Two processes exchanging messages: pid 0 spawns a partner, then they
// volley a counter back and forth until it reaches zero.

const N = 10;

const partner = spawn(`
  for (;;) {
    const msg = await recv();
    if (msg === null) break;
    console.log("pong", msg.n);
    send(msg.from, { from: self(), n: msg.n - 1 });
  }
  console.log("partner done");
`);

send(partner, { from: self(), n: N });

for (;;) {
  const msg = await recv();
  if (msg.n <= 0) break;
  console.log("ping", msg.n);
  send(partner, { from: self(), n: msg.n });
}

send(partner, null);
console.log("ping-pong finished");
