import { Queue } from "../mod.ts";
import { QueueTaskStatus } from "../v1.ts";

Deno.test({
  name: "Parallel task execution",
  async fn() {
    await Queue.start({ namespace: "testing", logs: true });

    const topic1 = "slowJobs";
    const topic2 = "fastJobs";

    await Queue.deleteAll(topic1);
    await Queue.deleteAll(topic2);

    let inserted = false;

    await Queue.subscribe(topic1, {
      handler: async (t) => {
        console.log("Slow execution", t.details.id);
        await new Promise((_) => setTimeout(_, 10000));

        if (!inserted) {
          await Queue.enqueue(topic2, {
            id: "fast6",
            data: {},
          }, { unique: true });

          await Queue.enqueue(topic2, {
            id: "fast7",
            data: {},
          });

          await Queue.enqueue(topic2, {
            id: "fast8",
            data: {},
          });

          await Queue.enqueue(topic2, {
            id: "fast9",
            data: {},
          });

          await Queue.enqueue(topic2, {
            id: "fast10",
            data: {},
          });

          inserted = true;

          await new Promise((_) => setTimeout(_, 10000));
        }

        console.log("Slow execution end", t.details.id);
      },
    });

    await Queue.subscribe(topic2, {
      handler: async () => {
        await new Promise((_) => setTimeout(_, 1000));
      },
      concurrency: 3,
    });

    await Queue.enqueue(topic1, {
      id: "slow1",
      data: {},
    });

    await Queue.enqueue(topic1, {
      id: "slow2",
      data: {},
    });

    await Queue.enqueue(topic1, {
      id: "slow3",
      data: {},
    });

    await Queue.enqueue(topic2, {
      id: "fast1",
      data: {},
    });

    await Queue.enqueue(topic2, {
      id: "fast2",
      data: {},
    });

    await Queue.enqueue(topic2, {
      id: "fast3",
      data: {},
    });

    await Queue.enqueue(topic2, {
      id: "fast4",
      data: {},
    });

    await Queue.enqueue(topic2, {
      id: "fast5",
      data: {},
    });

    await new Promise((_) => setTimeout(_, 45000));

    const [tasks1, tasks2] = await Promise.all([
      Queue.listTasks(topic1, { status: QueueTaskStatus.WAITING }),
      Queue.listTasks(topic2, { status: QueueTaskStatus.WAITING }),
    ]);

    if (tasks1.length || tasks2.length) {
      throw new Error("Some tasks were not consumed!");
    }

    const [tasks3, tasks4] = await Promise.all([
      Queue.listTasks(topic1, { status: QueueTaskStatus.COMPLETED }),
      Queue.listTasks(topic2, { status: QueueTaskStatus.COMPLETED }),
    ]);

    if (tasks3.length !== 3 || tasks4.length !== 10) {
      throw new Error("All tasks were not consumed!");
    }

    await Queue.stop(true);
  },
});
