import { Queue } from "../mod.ts";

Deno.test({
  name: "Delayed task execution",
  async fn() {
    await Queue.start({ namespace: "testing", logs: true });

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

    await Queue.subscribe(topic, {
      handler: () => {
        results.push(1);
      },
    });

    await new Promise((_) => setTimeout(_, delayMs - 500));

    if (results.length) throw new Error("Delayed task executed early!");

    await new Promise((_) => setTimeout(_, delayMs));

    if (!results.length) throw new Error("Delayed task didn't executed!");

    await Queue.stop(true);
  },
});
