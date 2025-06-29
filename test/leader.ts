import { Queue } from "../mod.ts";

await Queue.start({ namespace: "testing", logs: true });

await Queue.acquireLock(
  "Foo",
  () => console.log("Locked 1"),
  () => console.log("Unlocked 1"),
);

console.log("First Done!");

await Queue.acquireLock(
  "Foo",
  () => console.log("Locked 2"),
  () => console.log("Unlocked 2"),
);

console.log("Second Done!");
