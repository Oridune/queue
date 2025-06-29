import { Queue } from "../mod.ts";

Deno.test({
  name: "Timeout task execution",
  async fn() {
    await Queue.start({ namespace: "testing", logs: true });

    const topic = "timeoutQueue";

    await Queue.deleteAll(topic);

    await Queue.enqueue(topic, {
      id: "longTask1",
      data: {},
    });

    const longRunningFunction = async (signal?: AbortSignal) => {
      for (let i = 0; i < 100; i++) {
        if (signal?.aborted) {
          console.log("Function aborted");
          return;
        }

        console.log("Working", i);

        await new Promise((resolve) => setTimeout(resolve, 500)); // simulate async work
      }

      console.log("Done!");
    };

    await Queue.subscribe(topic, {
      handler: async ({ signal }) => {
        await longRunningFunction(signal);
      },
      timeoutMs: 1000,
    });

    await new Promise((_) => setTimeout(_, 3000));

    await Queue.enqueue(topic, {
      id: "longTask2",
      data: {},
      timeoutMs: 3000,
    });

    await Queue.subscribe(topic, {
      handler: async ({ signal }) => {
        await longRunningFunction(signal);
      },
    }, { replace: true });

    await new Promise((_) => setTimeout(_, 6000));

    await Queue.stop(true);
  },
});
