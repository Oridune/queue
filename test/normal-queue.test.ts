import { Queue } from "../mod.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Normal task execution",
  async fn(t) {
    await Queue.start({
      namespace: "testing",
      logs: true,
      redis: redisOptions,
    });

    const topic = "flowTest";
    const taskId = "normal";
    const results: number[] = [];

    await Queue.deleteAll(topic);

    await t.step("Check empty", async () => {
      const items = await Queue.listTasks(topic);

      if (items.length) throw new Error("There should be no tasks!");
    });

    await Queue.enqueue(topic, {
      id: taskId,
      data: {},
    });

    await t.step("Check enqueue", async () => {
      const items = await Queue.listTasks(topic);

      if (!items.length) throw new Error("There should be some tasks!");
    });

    await Queue.subscribe(topic, {
      handler: () => {
        results.push(1);
      },
    });

    await new Promise((_) => setTimeout(_, 6000));

    if (!results.length) throw new Error("Normal task didn't executed!");

    await Queue.stop(true);
  },
});
