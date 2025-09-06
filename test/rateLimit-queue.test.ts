import { Queue } from "../mod.ts";
import { QueueTaskStatus } from "../v1.ts";

Deno.test({
  name: "Rate limit task execution",
  async fn() {
    await Queue.start({ namespace: "testing", logs: true });

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
      rateLimit: {
        limit: 3,
        ttl: 5,
      },
    });

    await new Promise((_) => setTimeout(_, 8000));

    if (results.length !== 3) {
      throw new Error("Rate limiting is not working properly!");
    }

    const list1 = await Queue.listTaskIds(topic, {
      status: QueueTaskStatus.WAITING,
    });

    if (list1.length !== 7) {
      throw new Error("Something is not right!");
    }

    await new Promise((_) => setTimeout(_, 8000));

    // deno-lint-ignore ban-ts-comment
    // @ts-ignore
    if (results.length !== 6) {
      throw new Error("Rate limiting is not working properly!");
    }

    const list2 = await Queue.listTaskIds(topic, {
      status: QueueTaskStatus.WAITING,
    });

    if (list2.length !== 4) {
      throw new Error("Something is not right!");
    }

    await Queue.stop(true);
  },
});
