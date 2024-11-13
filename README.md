# Oridune Queue

A lightweight and performant queue library alternative to bull.js based on
redis.

### Basic Example

```ts
import { redis } from "./redis-connection.ts";
import { Queue } from "./mod.ts";

await Queue.start({ namespace: "development", redis });

/* producer.ts */
await Queue.enqueue("my-topic", {
    id: ["foo"],
    data: {
        foo: "bar",
    },
});

/* consumer.ts */
await Queue.subscribe<{
    foo: string;
}>("my-topic", {
    // Concurrency determines how many tasks should be executed at a time.
    concurrency: 2,
    handler: async (event) => {
        // Save time to time updates
        await event.progress(10, "Verification successful!");

        console.log(event.details.data);

        // Execute your task here...

        await event.progress(100);
    },
    // If you don't want to distribute the load (to process all queued tasks in parallel) on multiple workers
    // then set shared to false.
    shared: true,
});
```
