import { Queue } from "../mod.ts";

Deno.test({
  name: "Retry task execution",
  async fn() {
    await Queue.start({ namespace: "testing", logs: true });

    const topic = "retryQueue";

    await Queue.deleteAll(topic);

    await Queue.enqueue(topic, {
      id: "foo",
      data: {},
    });

    let attempt1 = 0;

    await Queue.subscribe(topic, {
      handler: () => {
        attempt1++;

        throw new Error("Try again!");
      },
    });

    await new Promise((_) => setTimeout(_, 3000));

    if (attempt1 !== 3) {
      throw new Error("Retry attempts are not working properly!");
    }

    await Queue.enqueue(topic, {
      id: "bar",
      data: {},
      retryCount: 5,
    });

    let attempt2 = 0;

    await Queue.subscribe(topic, {
      handler: () => {
        attempt2++;

        throw new Error("Try again!");
      },
    }, { replace: true });

    await new Promise((_) => setTimeout(_, 3000));

    if (attempt2 !== 5) {
      throw new Error("Custom retry attempts are not working properly!");
    }

    await Queue.stop(true);
  },
});
