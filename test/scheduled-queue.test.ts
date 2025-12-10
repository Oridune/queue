import { Queue } from "../mod.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Scheduled task execution",
  async fn() {
    await Queue.start({
      namespace: "testing",
      logs: true,
      redis: redisOptions,
    });

    const topic = "flowTest";
    const taskId = "scheduled";

    await Queue.deleteAll(topic);

    await Queue.enqueue(topic, {
      id: taskId,
      data: {},
    });

    let executed = false;

    const schedule = [
      [
        new Date(Date.now() + 30000),
        new Date(Date.now() + 60000 * 2),
      ] as [Date, Date],
    ];

    console.time("Delay");

    await Queue.subscribe(topic, {
      handler: () => {
        executed = true;

        console.timeEnd("Delay");
        console.log("Scheduled task executed!");
      },
      executionSchedules: schedule,
    });

    console.log("Waiting for scheduled task...");

    await new Promise((_) => setTimeout(_, 30000));

    if (executed) throw new Error("Scheduled task executed too early!");

    await new Promise((_) => setTimeout(_, 35000));

    if (!executed) throw new Error("Scheduled task didn't executed!");

    await Queue.stop(true);
  },
});
