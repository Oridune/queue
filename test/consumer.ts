import { redis } from "../test-connection.ts";
import { Queue } from "../mod.ts";

Queue.start({ namespace: "testing", redis, logs: true });

// Queue.resume("test");

Queue.subscribe<{
    foo: string;
}>("test", {
    concurrency: 5,
    sort: -1, // 1 ASC, -1 DESC
    handler: async (event) => {
        await event.progress(50);

        console.log(event.details);

        // if (event.details.id === "bar") {
        //     throw new Error("Something went wrong!");
        // }

        await new Promise((_) => setTimeout(_, 3000));

        await event.progress(100);
    },
    shared: true,
});
