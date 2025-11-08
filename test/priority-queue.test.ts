import { Queue } from "../mod.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Priority task execution",
  async fn() {
    await Queue.start({
      namespace: "testing",
      logs: true,
      redis: redisOptions,
    });

    const topic = "flowTest";

    await Queue.deleteAll(topic);

    await Queue.enqueue(topic, {
      id: "p10",
      data: {},
    });

    await Queue.enqueue(topic, {
      id: "p3",
      data: {},
      priority: 1,
    });

    await Queue.enqueue(topic, {
      id: "p1",
      data: {},
      priority: 5,
    });

    await Queue.enqueue(topic, {
      id: "p0",
      data: {},
      priority: 10,
    });

    await Queue.enqueue(topic, {
      id: "p2",
      data: {},
      priority: 2,
    });

    let task: string | undefined;

    await Queue.subscribe(topic, {
      handler: (t) => {
        if (!task) task = t.details.id;
      },
      sort: -1,
      concurrency: 4,
    });

    await new Promise((_) => setTimeout(_, 3000));

    if (task !== "p0") throw new Error("Priority task was not executed!");

    await Queue.stop(true);
  },
});
