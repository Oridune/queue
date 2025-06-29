// deno-lint-ignore-file no-explicit-any

import { Redis, type RedisOptions } from "ioredis";
import type { IRedis, TSort } from "./types.ts";
import { QueueWorker, QueueWorkerEvent } from "./worker.ts";
import { leader, type LeaderOpts } from "../common/leader.ts";

export enum LogType {
  INFO = "info",
  WARN = "warn",
  ERROR = "error",
}

export enum QueueTaskStatus {
  DELAYED = "delayed",
  WAITING = "waiting",
  PROCESSING = "processing",
  COMPLETED = "completed",
  FAILED = "failed",
}

export enum QueueEvent {
  PAUSE = "__onOriduneQueuePause",
  RESUME = "__onOriduneQueueResume",
}

export type TTaskData = Record<string, unknown>;

export type TTaskProgress = {
  percentage: string;
  log: string;
  timestamp: string;
};

export type TTaskError = {
  message: string;
  stack: string;
  timestamp: string;
};

export interface IQueueTaskPayload<T extends TTaskData> {
  id: string;
  data: T;
  delayMs?: number;
  retryCount?: number;
  priority?: number;
  timeoutMs?: number;
}

export type TQueueTaskDetails<T = string> = {
  id: string;
  data: T;
  delayMs?: string;
  retryCount?: string;
  priority?: string;
  timeoutMs?: number;
  progress?: string;
  attempt?: string;
  processedOn: string;
  updatedOn: string;
  retriedOn?: string;
  completedOn?: string;
};

export type TQueueProgressFunction = (
  percentage: number,
  log?: string,
) => Promise<void>;

export interface IQueueEvent<T extends TTaskData> {
  details: TQueueTaskDetails<T>;
  progress: TQueueProgressFunction;
  signal?: AbortSignal;
}

export interface IQueueEventHandlerOptions<T extends TTaskData> {
  concurrency?: number | (() => number | Promise<number>);
  sort?: TSort;
  handler: (
    event: IQueueEvent<T>,
  ) => unknown | Promise<unknown>;
  timeoutMs?: number;
  rateLimit?: {
    limit: number;
    ttl: number;
  };
  worker?: QueueWorker<T>;
  readonly shared?: boolean;
}

export interface IQueueInitOptions {
  logs?: boolean;
  namespace?: string;
  redis?: TRedisOpts;
  taskExpiryMs?: number;
  taskRecoveryIteration?: number;
}

export type TRedisOpts = Redis | RedisOptions | (() => Redis | RedisOptions);

export type TSubscriptionDetails = {
  unsubscribe: () => any;
  handlerOpts: IQueueEventHandlerOptions<any>;
};

export class Queue {
  protected static ready = false;
  protected static enableLogs = false;
  protected static namespace = ["OriduneQueue"];
  protected static subscriptions: Map<string, TSubscriptionDetails> = new Map();
  protected static taskExpiryMs = 10000;
  protected static taskRecoveryIteration = 10;

  protected static Redis?: Redis;
  protected static RedisOpts?: TRedisOpts;

  protected static async prepareRedis() {
    this.redis.defineCommand("processTasks", {
      numberOfKeys: 3,
      lua: await Deno.readTextFile("./v1/lua/processTasks.lua"),
    });

    this.redis.defineCommand("recoverTasks", {
      numberOfKeys: 2,
      lua: await Deno.readTextFile("./v1/lua/recoverTasks.lua"),
    });

    this.redis.defineCommand("updateTaskProgress", {
      numberOfKeys: 4,
      lua: await Deno.readTextFile("./v1/lua/updateTaskProgress.lua"),
    });

    this.redis.defineCommand("updateTaskError", {
      numberOfKeys: 4,
      lua: await Deno.readTextFile("./v1/lua/updateTaskError.lua"),
    });

    this.redis.defineCommand("retryTask", {
      numberOfKeys: 2,
      lua: await Deno.readTextFile("./v1/lua/retryTask.lua"),
    });

    this.redis.defineCommand("retryAllTasks", {
      numberOfKeys: 2,
      lua: await Deno.readTextFile("./v1/lua/retryAllTasks.lua"),
    });

    this.redis.defineCommand("listTasks", {
      numberOfKeys: 2,
      lua: await Deno.readTextFile("./v1/lua/listTasks.lua"),
    });

    this.redis.defineCommand("deleteKeysWithPattern", {
      numberOfKeys: 1,
      lua: await Deno.readTextFile("./v1/lua/deleteKeysWithPattern.lua"),
    });

    this.redis.defineCommand("rateLimitIncr", {
      numberOfKeys: 3,
      lua: await Deno.readTextFile("./v1/lua/rateLimitIncr.lua"),
    });
  }

  public static log(type: LogType, args: () => any[]) {
    if (this.enableLogs) {
      const Logger = type === LogType.ERROR
        ? console.error
        : type === LogType.WARN
        ? console.warn
        : console.log;

      const Prefix = [
        `${type.toUpperCase()}:`,
        new Date().toLocaleString(),
        "::",
      ];

      Logger(...Prefix, ...args());
    }
  }

  public static get redis(): IRedis {
    if (!this.ready) throw new Error("Queue is not initialized yet!");

    return (this.Redis ??= (() => {
      const Opts =
        (typeof this.RedisOpts === "function"
          ? this.RedisOpts()
          : this.RedisOpts) ?? {};

      return Opts instanceof Redis ? Opts.duplicate() : new Redis(Opts);
    })() as any);
  }

  public static resolveKey(
    key?: string | string[],
    namespace?: string | string[],
  ): string {
    const Namespace = namespace
      ? namespace instanceof Array ? namespace : [namespace]
      : this.namespace;

    if (key === undefined) return Namespace.join(":");

    const Key = key instanceof Array ? key : [key];

    return [...Namespace, ...Key].join(":");
  }

  public static async isPaused(topic?: string) {
    const paused = await this.redis.getbit(
      this.resolveKey("isPaused"),
      1,
    );

    if (paused) {
      this.log(
        LogType.WARN,
        () => ["All queue(s) paused!"],
      );

      return true;
    }

    if (topic) {
      const paused = await this.redis.getbit(
        this.resolveKey([topic, "isPaused"]),
        1,
      );

      if (paused) {
        this.log(
          LogType.WARN,
          () => [topic, "is paused!"],
        );
      }

      return !!paused;
    }

    return !!paused;
  }

  public static async pause(topic?: string) {
    await this.redis.setbit(
      this.resolveKey(topic ? [topic, "isPaused"] : "isPaused"),
      1,
      1,
    );

    dispatchEvent(
      new CustomEvent(QueueEvent.PAUSE, {
        detail: {
          topic,
        },
      }),
    );
  }

  public static async resume(topic?: string) {
    await this.redis.setbit(
      this.resolveKey(topic ? [topic, "isPaused"] : "isPaused"),
      1,
      0,
    );

    dispatchEvent(
      new CustomEvent(QueueEvent.RESUME, {
        detail: {
          topic,
        },
      }),
    );
  }

  public static onPause(
    listener: (event: CustomEvent, unRegister: () => void) => any,
    options?: boolean | AddEventListenerOptions,
  ) {
    const handler = (e: Event) => {
      listener(e as CustomEvent, () => {
        removeEventListener(QueueEvent.PAUSE, handler);
      });
    };

    addEventListener(QueueEvent.PAUSE, handler, options);
  }

  public static onResume(
    listener: (event: CustomEvent, unRegister: () => void) => any,
    options?: boolean | AddEventListenerOptions,
  ) {
    const handler = (e: Event) => {
      listener(e as CustomEvent, () => {
        removeEventListener(QueueEvent.RESUME, handler);
      });
    };

    addEventListener(QueueEvent.RESUME, handler, options);
  }

  public static async crashRecovery(topic: string) {
    const failedIds = await this.redis.recoverTasks(
      this.resolveKey(topic),
      this.taskExpiryMs,
    );

    failedIds.length &&
      this.log(LogType.INFO, () => ["Recovered Tasks:", failedIds]);

    return failedIds;
  }

  public static async getNextTasks(topic: string, count: number, sort: TSort) {
    // Move delayed or waiting tasks to processing list
    const movedTaskIds = await this.redis.processTasks(
      this.resolveKey(topic),
      count,
      sort,
    );

    movedTaskIds.length &&
      this.log(LogType.INFO, () => ["Captured Tasks:", movedTaskIds]);

    // Fetch tasks data
    const pl = this.redis.pipeline();

    movedTaskIds.forEach((id) => {
      pl.hgetall(this.resolveKey([topic, "data", id]));
    });

    return await pl.exec() as [
      Error | null,
      TQueueTaskDetails,
    ][];
  }

  public static async start(opts?: IQueueInitOptions) {
    if (this.ready) {
      throw new Error("Queue is already initialized!");
    }

    // Customize options
    if (typeof opts === "object") {
      this.RedisOpts = opts.redis;

      typeof opts.namespace === "string" && this.namespace.push(opts.namespace);
      typeof opts.logs === "boolean" && (this.enableLogs = opts.logs);
      typeof opts.taskExpiryMs === "number" && opts.taskExpiryMs > 100 &&
        (this.taskExpiryMs = opts.taskExpiryMs);
      typeof opts.taskRecoveryIteration === "number" &&
        opts.taskRecoveryIteration > 0 &&
        (this.taskRecoveryIteration = opts.taskRecoveryIteration);
    }

    this.ready = true;

    // Register custom redis commands
    await this.prepareRedis();

    this.log(LogType.INFO, () => ["Queue started!"]);
  }

  static async stop(endConnection = false) {
    this.log(LogType.WARN, () => ["Attempting to stop the queue!"]);

    await Promise.all(
      this.subscriptions.entries().map(
        ([, { unsubscribe }]) => unsubscribe(),
      ),
    );

    this.log(LogType.WARN, () => ["All subscriptions removed!"]);

    if (endConnection) {
      this.redis.disconnect();
      this.Redis = undefined;
      this.RedisOpts = undefined;
    }

    this.ready = false;

    this.log(LogType.WARN, () => ["Queue forcefully stopped!"]);
  }

  public static async enqueue<T extends TTaskData>(
    topic: string,
    payload: IQueueTaskPayload<T>,
    opts?: {
      unique?: boolean;
    },
  ) {
    const dataKey = this.resolveKey([topic, "data", payload.id]);

    if (opts?.unique && await this.redis.exists(dataKey)) {
      throw new Error(`Task ID: ${payload.id} already exists!`);
    }

    const tx = this.redis.multi();

    tx.hmset(dataKey, {
      ...payload,
      retryCount: payload.retryCount ?? 3,
      data: typeof payload.data === "object" && payload.data !== null
        ? JSON.stringify(payload.data)
        : "{}",
      createdOn: Date.now(),
    });

    if (typeof payload.delayMs === "number" && payload.delayMs >= 1000) {
      const delayedKey = this.resolveKey([
        topic,
        QueueTaskStatus.DELAYED,
      ]);

      tx.zadd(
        delayedKey,
        Date.now() + payload.delayMs,
        payload.id,
      );
    } else {
      const waitingKey = this.resolveKey([
        topic,
        QueueTaskStatus.WAITING,
      ]);

      tx.zadd(
        waitingKey,
        payload.priority ?? 0,
        payload.id,
      );
    }

    await tx.exec();

    this.log(LogType.INFO, () => ["New task added:", topic, payload, opts]);
  }

  static async acquireLock(
    key: string | string[],
    onLock: (unlock: () => Promise<void>) => void | Promise<void>,
    onUnlock: () => void | Promise<void>,
    opts?: LeaderOpts,
  ): Promise<{ release: () => Promise<void> }> {
    const { on, elect, shutdown } = await leader(
      this.redis,
      this.resolveKey(["locks", ...(key instanceof Array ? key : [key])]),
      opts,
    );

    on("elected", () => onLock(shutdown));
    on("demoted", () => onUnlock());

    await elect();

    return {
      release: shutdown,
    };
  }

  static async subscribe<T extends TTaskData>(
    topic: string,
    handlerOpts: IQueueEventHandlerOptions<T>,
    opts?: {
      replace?: boolean;
    },
  ): Promise<{ unsubscribe: () => Promise<void> }> {
    const existing = this.subscriptions.get(topic);

    if (existing) {
      if (opts?.replace) {
        await existing.unsubscribe();

        return this.subscribe(topic, handlerOpts, opts);
      }

      return existing;
    }

    const subscribe = () => {
      this.log(
        LogType.INFO,
        () => ["Subscription added:", topic, handlerOpts],
      );

      handlerOpts.worker ??= new QueueWorker(Queue, topic, handlerOpts);

      const subscription: TSubscriptionDetails = {
        handlerOpts,
        unsubscribe,
      };

      this.subscriptions.set(topic, subscription);

      handlerOpts.worker.run();

      return subscription;
    };

    const unsubscribe = async () => {
      await handlerOpts.worker?.stop();

      this.subscriptions.delete(topic);
    };

    if (handlerOpts.shared) return subscribe();
    else {
      const unsubscribeLocked = (unlock: () => Promise<void>) => async () => {
        await unsubscribe();
        await unlock();
      };

      const { release } = await this.acquireLock(topic, (shutdown) => {
        const subscription = subscribe();

        subscription.unsubscribe = unsubscribeLocked(shutdown);
      }, unsubscribe);

      return {
        unsubscribe: unsubscribeLocked(release),
      };
    }
  }

  static getSubscription(topic: string) {
    return this.subscriptions.get(topic);
  }

  static async unsubscribe(topic: string) {
    await this.subscriptions.get(topic)?.unsubscribe();
  }

  static incrSlot(topic?: string) {
    this.redis.publish(
      QueueWorkerEvent.INCR_SLOT + topic,
      "incr",
    );
  }

  static decrSlot(topic?: string) {
    this.redis.publish(
      QueueWorkerEvent.INCR_SLOT + topic,
      "decr",
    );
  }

  static async listTaskIds(topic: string, status: QueueTaskStatus, opts?: {
    offset?: number;
    limit?: number;
    sort?: TSort;
  }): Promise<string[]> {
    const sort = opts?.sort ?? 0;
    const offset = opts?.offset ?? 0;
    const limit = opts?.limit ?? 10000000;

    let ids: string[];

    if (sort > 0) {
      ids = await this.redis.zrangebyscore(
        this.resolveKey([topic, status]),
        "-inf",
        "+inf",
        "LIMIT",
        offset,
        limit,
      );
    } else {
      ids = await this.redis.zrevrangebyscore(
        this.resolveKey([topic, status]),
        "+inf",
        "-inf",
        "LIMIT",
        offset,
        limit,
      );
    }

    return ids;
  }

  static async listTaskProgress(
    topic: string,
    taskId: string,
  ): Promise<TTaskProgress[]> {
    const progressKey = this.resolveKey([
      topic,
      "data",
      taskId,
      "progress",
    ]);

    const progressKeys = await this.redis.lrange(progressKey, 0, -1);

    return await Promise.all<Array<TTaskProgress>>(
      progressKeys.map((uuid) =>
        this.redis.hgetall(this.resolveKey(uuid, progressKey)) as any
      ),
    );
  }

  static async listTaskError(
    topic: string,
    taskId: string,
  ): Promise<TTaskError[]> {
    const errorKey = this.resolveKey([
      topic,
      "data",
      taskId,
      "error",
    ]);

    const errorKeys = await this.redis.lrange(errorKey, 0, -1);

    return await Promise.all<Array<TTaskError>>(
      errorKeys.map((uuid) =>
        this.redis.hgetall(this.resolveKey(uuid, errorKey)) as any
      ),
    );
  }

  protected static async populateTasks(
    topic: string,
    tasks: Record<string, string>[],
  ): Promise<(TQueueTaskDetails<string> & {
    progressTimeline: Array<TTaskProgress>;
    errorTimeline: Array<TTaskError>;
  })[]> {
    const resolvedTasks = tasks.map((task) => ({
      ...task,
      data: task.data && JSON.parse(task.data),
    })) as Array<
      TQueueTaskDetails & {
        progressTimeline: Array<TTaskProgress>;
        errorTimeline: Array<TTaskError>;
      }
    >;

    return await Promise.all(resolvedTasks.map(async (task) => {
      const [progress, error] = await Promise.all(
        [
          this.listTaskProgress(topic, task.id),
          this.listTaskError(topic, task.id),
        ] as const,
      );

      task.progressTimeline = progress;
      task.errorTimeline = error;

      return task;
    }));
  }

  protected static redisHashsToTasks(
    hashs: string[][],
    ...fields: string[]
  ): Record<string, string>[] {
    return hashs.map((hash) => {
      const result: Record<string, string> = {};

      if (fields.length) {
        fields.forEach((k, i) => {
          result[k] = hash[i];
        });
      } else {
        for (let i = 0; i < hash.length; i += 2) {
          result[hash[i]] = hash[i + 1];
        }
      }

      return result;
    });
  }

  static async listTasks(topic: string, status: QueueTaskStatus, opts?: {
    offset?: number;
    limit?: number;
    sort?: TSort;
    fields?: string[];
  }): Promise<(TQueueTaskDetails<string> & {
    progressTimeline: Array<TTaskProgress>;
    errorTimeline: Array<TTaskError>;
  })[]> {
    const sort = opts?.sort ?? 1;
    const offset = opts?.offset ?? 0;
    const limit = opts?.limit ?? 10000000;
    const fields = opts?.fields
      ? Array.from(new Set(["id", ...opts.fields]))
      : [];

    const results = await this.redis.listTasks<string[]>(
      this.resolveKey([topic, status]),
      this.resolveKey([topic, "data"]),
      sort,
      offset,
      limit,
      ...fields,
    );

    return await this.populateTasks(
      topic,
      this.redisHashsToTasks(results),
    );
  }

  static async listAllTasks(
    topic: string,
  ): Promise<(TQueueTaskDetails<string> & {
    progressTimeline: Array<TTaskProgress>;
    errorTimeline: Array<TTaskError>;
  })[]> {
    return (await Promise.all(
      Object.values(QueueTaskStatus).map((status) =>
        this.listTasks(topic, status)
      ),
    )).flat();
  }

  static async retry(topic: string, taskId: string, ...taskIds: string[]) {
    await this.redis.retryTask(
      this.resolveKey(),
      topic,
      taskId,
      ...taskIds,
    );
  }

  static async retryAll(topic: string) {
    await this.redis.retryAllTasks(this.resolveKey(), topic);
  }

  static async delete(topic: string, ...taskIds: string[]): Promise<number> {
    const delayedKey = this.resolveKey([
      topic,
      QueueTaskStatus.DELAYED,
    ]);
    const waitingKey = this.resolveKey([
      topic,
      QueueTaskStatus.WAITING,
    ]);
    const processingKey = this.resolveKey([
      topic,
      QueueTaskStatus.PROCESSING,
    ]);
    const completedKey = this.resolveKey([
      topic,
      QueueTaskStatus.COMPLETED,
    ]);
    const failedKey = this.resolveKey([
      topic,
      QueueTaskStatus.FAILED,
    ]);

    return (await Promise.all(taskIds.map(async (taskId) => {
      const dataKey = this.resolveKey([topic, "data", taskId]);

      const tx = this.redis.multi();

      tx.del(dataKey);
      tx.zrem(delayedKey, taskId);
      tx.zrem(waitingKey, taskId);
      tx.zrem(processingKey, taskId);
      tx.zrem(completedKey, taskId);
      tx.zrem(failedKey, taskId);

      const results = await tx.exec();

      await this.redis.deleteKeysWithPattern(
        this.resolveKey("*", dataKey),
      );

      return (results?.[0][1] ?? 0) as number;
    }))).reduce((_, num) => _ + num, 0);
  }

  static async deleteAll(topic?: string): Promise<number> {
    return await this.redis.deleteKeysWithPattern(
      this.resolveKey(topic ? [topic, "*"] : "*"),
    );
  }
}
