import { Queue } from "../mod.ts";

await Queue.start({ namespace: "testing" });

for (let i = 0; i < 100; i++) {
  await Queue.enqueue("test", {
    id: "foo" + i,
    data: {},
  });
}

await Queue.stop(true);
