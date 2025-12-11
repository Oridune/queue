import { Queue } from "../mod.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Delayed task execution",
  async fn() {
    await Queue.start({ namespace: "testing", logs: true, redis: redisOptions });

    const topic = "flowTest";
    const taskId = "delayed";
    const delayMs = 3000;
    const results: number[] = [];

    await Queue.deleteAll(topic);

    await Queue.enqueue(topic, {
      id: taskId,
      data: {},
      delayMs,
    });

    const resolveMap: Record<
      string,
      (value: void | PromiseLike<void>) => void
    > = {};
    const promise = new Promise<void>((res) => {
      resolveMap[taskId] = res;
    });

    await Queue.subscribe(topic, {
      handler: ({ details: { id } }) => {
        resolveMap[id]?.();

        results.push(1);
      },
    });

    await new Promise((_) => setTimeout(_, delayMs - 500));

    if (results.length) throw new Error("Delayed task executed early!");

    console.log("Waiting for task...");

    await promise;

    if (!results.length) throw new Error("Delayed task didn't executed!");

    await Queue.stop(true);
  },
});
