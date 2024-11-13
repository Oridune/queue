import { redis } from "../test-connection.ts";
import { Queue } from "../mod.ts";

Deno.test({
    name: "Normal task execution",
    async fn() {
        Queue.start({ namespace: "testing", redis, logs: true }, true);

        await Queue.deleteAll();

        const topic = "flowTest";
        const taskId = "normal";
        const results: number[] = [];

        await Queue.enqueue(topic, {
            id: taskId,
            data: {},
        });

        await Queue.subscribe(topic, {
            handler: () => {
                results.push(1);
            },
        });

        await new Promise((_) => setTimeout(_, 1000));

        if (!results.length) throw new Error("Normal task didn't executed!");

        await Queue.stop(true);
    },
});
