import { Queue } from "../mod.ts";
import { QueueTaskStatus } from "../v1.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Rate limit task execution",
  async fn() {
    await Queue.start({
      namespace: "testing",
      logs: true,
      redis: redisOptions,
    });

    const topic = "rateLimiting";
    const results: number[] = [];

    await Queue.deleteAll(topic);

    for (let i = 0; i < 10; i++) {
      await Queue.enqueue(topic, {
        id: "limited" + i,
        data: {},
      });
    }

    await Queue.subscribe(topic, {
      handler: () => {
        results.push(1);
      },
      concurrency: 4,
      rateLimit: {
        limit: 3,
        ttl: 10,
      },
    });

    await new Promise((_) => setTimeout(_, 12000));

    if (results.length !== 3) {
      throw new Error("Rate limiting is not working properly!");
    }

    const list1 = await Queue.listTaskIds(topic, {
      status: QueueTaskStatus.WAITING,
    });

    if (list1.length !== 7) {
      throw new Error("Something is not right!");
    }

    await Queue.stop(true);
  },
});
