import { Queue } from "../mod.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Backlog drains without per-task tick delay",
  async fn() {
    await Queue.start({
      namespace: "testing",
      logs: false,
      redis: redisOptions,
    });

    const topic = "throughputTest";
    let processed = 0;

    await Queue.deleteAll(topic);

    for (let i = 0; i < 20; i++) {
      await Queue.enqueue(topic, { id: "fast" + i, data: {} });
    }

    await Queue.subscribe(topic, {
      handler: () => {
        processed++;
      },
    });

    // Previously 1 task per second per slot, so this would be ~4
    await new Promise((_) => setTimeout(_, 4000));

    await Queue.stop(true);

    if (processed !== 20) throw new Error(`Only ${processed}/20 processed!`);
  },
});

Deno.test({
  name: "Idle polling doesn't consume rate limit budget",
  async fn() {
    await Queue.start({
      namespace: "testing",
      logs: false,
      redis: redisOptions,
    });

    const topic = "idleRateLimitTest";
    let processed = 0;

    await Queue.deleteAll(topic);

    await Queue.subscribe(topic, {
      handler: () => {
        processed++;
      },
      rateLimit: { limit: 3, ttl: 30 },
    });

    // Idle ticks used to burn the whole budget here
    await new Promise((_) => setTimeout(_, 5000));

    for (let i = 0; i < 3; i++) {
      await Queue.enqueue(topic, { id: "idle" + i, data: {} });
    }

    await new Promise((_) => setTimeout(_, 4000));

    await Queue.stop(true);

    if (processed !== 3) throw new Error(`Only ${processed}/3 processed!`);
  },
});
