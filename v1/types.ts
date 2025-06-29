// deno-lint-ignore-file no-explicit-any
import type { Redis } from "ioredis";

export type TSort = 1 | -1;

export type IRedis = Redis & {
  recoverTasks(
    namespace: string,
    thresholdMs: number,
  ): Promise<string[]>;

  processTasks(
    namespace: string,
    count: number,
    sort: TSort,
  ): Promise<string[]>;

  updateTaskProgress(
    dataKey: string,
    uuid: string,
    percentage: number,
    log: string,
  ): Promise<1>;

  updateTaskError(
    namespace: string,
    topic: string,
    taskId: string,
    uuid: string,
    message: string,
    stack: string,
    timestamp: number,
    moveToFailed: 0 | 1,
    priority?: number,
  ): Promise<1>;

  listTasks<T extends Record<string, any>>(
    sorted_set_key: string,
    dataKeyPrefix: string,
    sort_dir: TSort,
    offset: number,
    limit: number,
    ...args: string[]
  ): Promise<T[]>;

  deleteKeysWithPattern(
    pattern: string,
    batch_size?: number,
  ): Promise<number>;

  retryTask(
    namespace: string,
    topic: string,
    taskId: string,
    ...taskIds: string[]
  ): Promise<1>;

  retryAllTasks(
    namespace: string,
    topic: string,
  ): Promise<1>;

  rateLimitIncr(
    key: string,
    limit: number,
    ttl: number,
  ): Promise<number>;
};
