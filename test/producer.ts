import { redis } from "../test-connection.ts";
import { Queue } from "../mod.ts";

Queue.start({ namespace: "testing", redis, tickMs: 3000 });

// console.log(await Queue.deleteAll("test"));

await Queue.enqueue("test", {
    id: "ola",
    data: {
        ola: "bola",
    },
    priority: 10,
});

await Queue.enqueue("test", {
    id: "foo",
    data: {
        foo: "bar",
    },
    // retryCount: 3,
    priority: 5,
});

await Queue.enqueue("test", {
    id: "bar",
    data: {
        bar: "baz",
    },
    delayMs: 5000,
    retryCount: 3,
    priority: 10,
});

await Queue.enqueue("test", {
    id: "baz",
    data: {
        baz: "foo",
    },
    delayMs: 15000,
    priority: 10,
});

await Queue.enqueue("test", {
    id: "hello",
    data: {
        hello: "world",
    },
    delayMs: 20000,
    priority: 10,
});

// console.log(
//     await Queue.listAllTasks("test"),
// );

// await Queue.retryAll("test");

Queue.stop(true);

redis.disconnect();
