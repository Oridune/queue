import { Queue } from "../mod.ts";

await Queue.start({ namespace: "testing", logs: true });

await Queue.deleteAll();
// Queue.resume("test");

await Queue.subscribe<{
  foo: string;
}>("test", {
  concurrency: 1,
  sort: -1, // 1 ASC, -1 DESC
  handler: async (event) => {
    await event.progress(50);

    console.log(event.details);

    // if (event.details.id === "bar") {
    //     throw new Error("Something went wrong!");
    // }

    await new Promise((_) => setTimeout(_, 1000));

    await event.progress(100);
  },
  // shared: true,
});

// console.log("Waiting for Jobs!");

// await new Promise((_) => setTimeout(_, 10000));

// Queue.incrSlot("test");
// Queue.incrSlot("test");

// await new Promise((_) => setTimeout(_, 10000));

// Queue.decrSlot("test");
