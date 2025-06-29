import { Queue } from "../mod.ts";
import { QueueTaskStatus } from "../v1.ts";

Deno.test({
  name: "Pause queue check",
  async fn(t) {
    await Queue.start({ namespace: "testing", logs: true });

    await t.step("Global pause", async () => {
      const topic = "globalPauseTest";

      await Queue.deleteAll();

      await Queue.subscribe(topic, {
        handler: async (t) => {
          console.log("Executed", t.details.id);

          if (t.details.id === "t1") {
            await Queue.pause();
          }
        },
      });

      await Queue.enqueue(topic, {
        id: "t1",
        data: {},
      });

      await Queue.enqueue(topic, {
        id: "t2",
        data: {},
      });

      await new Promise((_) => setTimeout(_, 2000));

      const tasks1 = await Queue.listTaskIds(topic, QueueTaskStatus.WAITING);

      if (tasks1.length !== 1) {
        throw new Error(
          tasks1.length > 1
            ? "Nothing was consumed! Is there a global pause?"
            : tasks1.length < 1
            ? "Consumed everything! Topic was never paused!"
            : "Something is not correct!",
        );
      }

      await Queue.resume();

      await new Promise((_) => setTimeout(_, 7000));

      const tasks2 = await Queue.listTaskIds(topic, QueueTaskStatus.WAITING);

      if (tasks2.length !== 0) {
        throw new Error(
          tasks2.length > 1
            ? "Did you add more tasks?"
            : "Nothing was consumed! Is the resume working?",
        );
      }
    });

    await t.step("Topic pause", async () => {
      const topic = "topicPauseTest";

      await Queue.deleteAll(topic);

      await Queue.subscribe(topic, {
        handler: async (t) => {
          console.log("Executed", t.details.id);

          if (t.details.id === "t1") {
            await Queue.pause(topic);
          }
        },
      });

      await Queue.enqueue(topic, {
        id: "t1",
        data: {},
      });

      await Queue.enqueue(topic, {
        id: "t2",
        data: {},
      });

      await new Promise((_) => setTimeout(_, 2000));

      const tasks1 = await Queue.listTasks(topic, QueueTaskStatus.WAITING);

      if (tasks1.length !== 1) {
        throw new Error(
          tasks1.length > 1
            ? "Nothing was consumed! Is there a global pause?"
            : tasks1.length < 1
            ? "Consumed everything! Topic was never paused!"
            : "Something is not correct!",
        );
      }

      await Queue.resume();

      await new Promise((_) => setTimeout(_, 7000));

      const tasks2 = await Queue.listTaskIds(topic, QueueTaskStatus.WAITING);

      if (tasks2.length !== 1) {
        throw new Error("Did global resume, resumed the topic?");
      }

      await Queue.resume(topic);

      await new Promise((_) => setTimeout(_, 7000));

      const tasks3 = await Queue.listTaskIds(topic, QueueTaskStatus.WAITING);

      if (tasks3.length !== 0) {
        throw new Error(
          tasks3.length > 1
            ? "Did you add more tasks?"
            : "Nothing was consumed! Is the resume working?",
        );
      }
    });

    await Queue.stop(true);
  },
});
