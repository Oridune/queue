import { Queue } from "../mod.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Capped complete/failed task execution",
  async fn() {
    await Queue.start({
      namespace: "testing",
      logs: true,
      redis: redisOptions,
    });

    const topic = "flowTest";

    await Queue.deleteAll(topic);

    const completingTaskIds = Array.from({ length: 10 }).map((_, i) =>
      `completeTask${i + 1}`
    );
    const failingTaskIds = Array.from({ length: 10 }).map((_, i) =>
      `failTask${i + 1}`
    );

    for (const id of completingTaskIds) {
      await Queue.enqueue(topic, {
        id: id,
        data: {
          success: true,
        },
        retryCount: 0,
      });
    }

    for (const id of failingTaskIds) {
      await Queue.enqueue(topic, {
        id: id,
        data: {},
        retryCount: 0,
      });
    }

    const resolveMap: Record<
      string,
      (value: void | PromiseLike<void>) => void
    > = {};
    const promises = [...completingTaskIds, ...failingTaskIds].map(
      (id) =>
        new Promise<void>((resolve) => {
          resolveMap[id] = resolve;
        }),
    );

    await Queue.subscribe(topic, {
      sort: 1,
      handler: ({ details: { id, data: { success } } }) => {
        resolveMap[id]?.();

        console.log(`Task ${id} executed`);

        if (!success) {
          throw new Error("Intentional Failure");
        }
      },
      concurrency: 1,
      completedCap: 5,
      failedCap: 5,
    });

    console.log("Waiting for task...");

    await Promise.all(promises);

    const totalTasks = await Queue.countTasks(topic);

    if (totalTasks === 20) {
      throw new Error("Cap not working properly!", {
        cause: {
          totalTasks,
        },
      });
    }

    await Queue.stop(true);
  },
});
