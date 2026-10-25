import { Queue, QueueTaskStatus } from "../mod.ts";
import { redisOptions } from "./global.ts";

Deno.test({
  name: "Task without an id gets completed",
  async fn() {
    await Queue.start({
      namespace: "testing",
      logs: true,
      redis: redisOptions,
    });

    const topic = "noIdTest";

    await Queue.deleteAll(topic);

    const { id } = await Queue.enqueue(topic, { data: { foo: "bar" } });

    let received: unknown;

    await Queue.subscribe(topic, {
      handler: ({ details }) => {
        received = details;
      },
    });

    await new Promise((_) => setTimeout(_, 4000));

    const [task] = await Queue.listTasks(topic, { id });

    await Queue.stop(true);

    if (!received) throw new Error("Task was never processed!");
    if (task.id !== id) throw new Error(`Task id not stored: ${task.id}`);
    if (!task.completedOn) throw new Error("Task was not marked completed!");
    if ("validate" in task) throw new Error("validate leaked into task data!");

    const completed = await (async () => {
      await Queue.start({ namespace: "testing", redis: redisOptions });
      const ids = await Queue.listTaskIds(topic, {
        status: QueueTaskStatus.COMPLETED,
      });
      const processing = await Queue.countTasks(
        topic,
        QueueTaskStatus.PROCESSING,
      );
      await Queue.deleteAll(topic);
      await Queue.stop(true);
      return { ids, processing };
    })();

    if (!completed.ids.includes(id)) {
      throw new Error("Task not in completed list!");
    }
    if (completed.processing) throw new Error("Task stuck in processing!");
  },
});
