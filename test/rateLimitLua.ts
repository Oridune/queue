import { Queue } from "../mod.ts";

await Queue.start({ namespace: "testing", logs: true });

for (let i = 0; i < 100; i++) {
  console.log(await Queue.redis.rateLimitIncr(Queue.resolveKey("foo"), 50, 10));
}

await Queue.stop(true);
