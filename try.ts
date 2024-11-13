import { redis } from "./test-connection.ts";
import { Queue } from "./mod.ts";

Queue.start({ namespace: "testing", redis, tickMs: 3000 });

setInterval(async () => {
    await Queue.enqueue("test", {
        id: "ola",
        data: {
            ola: "bola",
        },
        priority: 10,
    });
}, 3000);

// await Queue.enqueue("test", {
//     id: ["foo"],
//     data: {
//         foo: "bar",
//     },
// });

// await Queue.enqueue("test", {
//     id: ["bar"],
//     data: {
//         bar: "baz",
//     },
//     delayMs: 5000,
// });

// await Queue.enqueue("test", {
//     id: ["baz"],
//     data: {
//         baz: "foo",
//     },
//     delayMs: 15000,
// });

// await Queue.enqueue("test", {
//     id: ["hello"],
//     data: {
//         hello: "world",
//     },
//     delayMs: 15000,
// });

Queue.subscribe<{
    foo: string;
}>("test", {
    concurrency: 2,
    sort: 1, // 1 ASC, -1 DESC
    handler: async (event, _topic) => {
        await event.progress(50);

        console.log(event.details.data);

        await new Promise((_) => setTimeout(_, 3000));

        await event.progress(100);
    },
    // shared: true,
});

// Queue.stop();
// redis.end();
