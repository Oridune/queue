import { Queue } from "../mod.ts";

Deno.test({
  name: "Normal task execution",
  async fn(t) {
    await Queue.start({ namespace: "testing", logs: true });

    const topic = "flowTest";
    const taskId = "normal";
    const results: number[] = [];

    await Queue.deleteAll(topic);

    await t.step("Check empty", async () => {
      const items = await Queue.listAllTasks(topic);

      if (items.length) throw new Error("There should be no tasks!");
    });

    await Queue.enqueue(topic, {
      id: taskId,
      data: {},
    });

    await t.step("Check enqueue", async () => {
      const items = await Queue.listAllTasks(topic);

      if (!items.length) throw new Error("There should be some tasks!");
    });

    await Queue.subscribe(topic, {
      handler: () => {
        results.push(1);
      },
    });

    await new Promise((_) => setTimeout(_, 3000));

    if (!results.length) throw new Error("Normal task didn't executed!");

    await Queue.stop(true);
  },
});
