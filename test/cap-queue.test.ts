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
    const task1 = "shouldSucceed";
    const task2 = "shouldFail";

    await Queue.deleteAll(topic);

    await Queue.enqueue(topic, {
      id: task1,
      data: {},
    });

    await Queue.enqueue(topic, {
      id: task2,
      data: {},
      retryCount: 0
    });

    const resolveMap: Record<
      string,
      (value: void | PromiseLike<void>) => void
    > = {};
    const p1 = new Promise<void>((res) => {
      resolveMap[task1] = res;
    });
    const p2 = new Promise<void>((res) => {
      resolveMap[task2] = res;
    });
    const promises = [p1, p2];

    await Queue.subscribe(topic, {
      sort: 1,
      handler: ({ details: { id } }) => {
        resolveMap[id]?.();

        console.log(`Task ${id} executed`);

        if (id === task1) {
          return { success: true };
        } else {
          throw new Error("Intentional Failure");
        }
      },
    });

    console.log("Waiting for task...");

    await Promise.all(promises);

    const task1Exists = await Queue.listTasks(topic, { id: task1 });
    const task2Exists = await Queue.listTasks(topic, { id: task2 });

    if (!(task1Exists.length && task2Exists.length)) {
      throw new Error("Tasks deleted too early!", {
        cause: {
          task1Length: task1Exists.length,
          task2Length: task2Exists.length,
        },
      });
    }

    await new Promise((_) => setTimeout(_, 15000));

    const task1ExistsNot = await Queue.listTasks(topic, { id: task1 });
    const task2ExistsNot = await Queue.listTasks(topic, { id: task2 });

    if (task1ExistsNot.length || task2ExistsNot.length) {
      throw new Error("Tasks didn't delete after TTL!", {
        cause: {
          task1Length: task1ExistsNot.length,
          task2Length: task2ExistsNot.length,
        },
      });
    }

    await Queue.stop(true);
  },
});
