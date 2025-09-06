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

export type TQueueTaskDetails<T = TTaskData> = {
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
    await Promise.all([
      {
        name: "processTasks",
        keys: 3,
        path: "./lua/processTasks.lua",
      },
      {
        name: "recoverTasks",
        keys: 2,
        path: "./lua/recoverTasks.lua",
      },
      {
        name: "updateTaskProgress",
        keys: 4,
        path: "./lua/updateTaskProgress.lua",
      },
      {
        name: "updateTaskError",
        keys: 4,
        path: "./lua/updateTaskError.lua",
      },
      {
        name: "retryTask",
        keys: 2,
        path: "./lua/retryTask.lua",
      },
      {
        name: "retryAllTasks",
        keys: 2,
        path: "./lua/retryAllTasks.lua",
      },
      {
        name: "listTasks",
        keys: 2,
        path: "./lua/listTasks.lua",
      },
      {
        name: "deleteKeysWithPattern",
        keys: 1,
        path: "./lua/deleteKeysWithPattern.lua",
      },
      {
        name: "rateLimitIncr",
        keys: 3,
        path: "./lua/rateLimitIncr.lua",
      },
    ].map(async (script) =>
      this.redis.defineCommand(script.name, {
        numberOfKeys: script.keys,
        lua: (await import(script.path, { with: { type: "text" } })).default,
      })
    ));
  }

  /**
   * Custom logging method
   * @param type Type of log
   * @param args
   */
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

  /**
   * Get redis instance
   */
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

  /**
   * Resolves the key parts
   * @param key
   * @param namespace
   * @returns
   */
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

  /**
   * Check if a queue or a topic is paused
   * @param topic
   * @returns
   */
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

  /**
   * Pause the queue or a specific topic
   * @param topic
   * @returns
   */
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

  /**
   * Resume the queue or a specific topic
   * @param topic
   * @returns
   */
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

  /**
   * Do something when a queue is paused
   * @param listener
   * @param options
   */
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

  /**
   * Do something when a queue is resumed
   * @param listener
   * @param options
   */
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

  /**
   * Recover any incomplete/crashed tasks (Changes the task status to Failed)
   * @param topic
   * @returns
   */
  public static async crashRecovery(topic: string) {
    const failedIds = await this.redis.recoverTasks(
      this.resolveKey(topic),
      this.taskExpiryMs,
    );

    failedIds.length &&
      this.log(LogType.INFO, () => ["Recovered Tasks:", failedIds]);

    return failedIds;
  }

  /**
   * Moves the tasks to processing state and returns them
   * @param topic
   * @param count
   * @param sort
   * @returns
   */
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

  /**
   * Initialize/Start a queue
   * @param opts
   */
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

  /**
   * Stop/Uninitialize a queue
   * @param endConnection
   */
  public static async stop(endConnection = false) {
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

  /**
   * Enqueue a task in the queue
   * @param topic
   * @param payload
   * @param opts
   */
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

    const now = Date.now();

    const idsKey = this.resolveKey([topic, "ids"]);

    const tx = this.redis.multi();

    tx.zadd(idsKey, now, payload.id);

    tx.hmset(dataKey, {
      ...payload,
      retryCount: payload.retryCount ?? 3,
      data: typeof payload.data === "object" && payload.data !== null
        ? JSON.stringify(payload.data)
        : "{}",
      createdOn: now,
    });

    if (typeof payload.delayMs === "number" && payload.delayMs >= 1000) {
      const delayedKey = this.resolveKey([
        topic,
        QueueTaskStatus.DELAYED,
      ]);

      tx.zadd(
        delayedKey,
        now + payload.delayMs,
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

  /**
   * A utility method to acquire a lock on any resource, so that any other executor don't consume it
   * @param key
   * @param onLock
   * @param onUnlock
   * @param opts
   * @returns
   */
  public static async acquireLock(
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

  /**
   * Subscribe to a topic to consume the enqueued tasks
   * @param topic
   * @param handlerOpts
   * @param opts
   * @returns
   */
  public static async subscribe<T extends TTaskData>(
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

  /**
   * Get subscription details
   * @param topic
   * @returns
   */
  public static getSubscription(topic: string) {
    return this.subscriptions.get(topic);
  }

  /**
   * Unsubscribe from a topic
   * @param topic
   */
  public static async unsubscribe(topic: string) {
    await this.subscriptions.get(topic)?.unsubscribe();
  }

  /**
   * Retry all or specific tasks under a specific topic
   * @param topic
   * @param taskId
   * @param taskIds
   */
  public static async retry(
    topic: string,
    taskId?: string,
    ...taskIds: string[]
  ) {
    if (taskId) {
      await this.redis.retryTask(
        this.resolveKey(),
        topic,
        taskId,
        ...taskIds,
      );
    } else {
      await this.redis.retryAllTasks(this.resolveKey(), topic);
    }
  }

  /**
   * Increment parallel task execution slot
   *
   * If you want to execute more tasks per second per subscription you can use this method to increment the execution slot.
   * @param topic
   */
  public static incrSlot(topic?: string) {
    this.redis.publish(
      QueueWorkerEvent.INCR_SLOT + topic,
      "incr",
    );
  }

  /**
   * This method is used to decrement an execution slot
   *
   * If a subscriber executes 2 tasks per second you can use this method to reduce it to execute only one task at a time.
   * @param topic
   */
  public static decrSlot(topic?: string) {
    this.redis.publish(
      QueueWorkerEvent.INCR_SLOT + topic,
      "decr",
    );
  }

  /**
   * List all task ids from a specific topic
   * @param topic
   * @param opts
   * @returns
   */
  public static async listTaskIds(
    topic: string,
    opts?: {
      status?: QueueTaskStatus;
      offset?: number;
      limit?: number;
      sort?: TSort;
    },
  ): Promise<string[]> {
    const sort = opts?.sort ?? 0;
    const offset = opts?.offset ?? 0;
    const limit = opts?.limit ?? 10000000;

    if (sort > 0) {
      return await this.redis.zrangebyscore(
        this.resolveKey([topic, opts?.status ?? "ids"]),
        "-inf",
        "+inf",
        "LIMIT",
        offset,
        limit,
      );
    } else {
      return await this.redis.zrevrangebyscore(
        this.resolveKey([topic, opts?.status ?? "ids"]),
        "+inf",
        "-inf",
        "LIMIT",
        offset,
        limit,
      );
    }
  }

  /**
   * List any in-progress task's current progress timeline
   * @param topic
   * @param taskId
   * @returns
   */
  public static async listTaskProgress(
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

  /**
   * List task's errors
   * @param topic
   * @param taskId
   * @returns
   */
  public static async listTaskError(
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

  protected static async resolveTasks<T extends TTaskData>(
    topic: string,
    tasks: Record<string, string>[],
    opts?: {
      progress?: boolean;
      error?: boolean;
    },
  ) {
    return await Promise.all(
      tasks.map(async (task: Record<string, unknown>) => {
        if (typeof task.data === "string") {
          try {
            task.data = JSON.parse(task.data);
          } catch {
            // Do nothing...
          }
        }

        const [progress, error] = await Promise.all(
          [
            opts?.progress !== false && typeof task.id === "string"
              ? this.listTaskProgress(topic, task.id)
              : [],
            opts?.error !== false && typeof task.id === "string"
              ? this.listTaskError(topic, task.id)
              : [],
          ] as const,
        );

        if (opts?.progress !== false) {
          task.progressTimeline = progress;
        }

        if (opts?.error !== false) {
          task.errorTimeline = error;
        }

        return task;
      }),
    ) as unknown as (TQueueTaskDetails<T> & {
      progressTimeline?: Array<TTaskProgress>;
      errorTimeline?: Array<TTaskError>;
    })[];
  }

  /**
   * List the queued tasks
   * @param topic
   * @param opts
   * @returns
   */
  public static async listTasks<T extends TTaskData>(topic: string, opts?: {
    id?: string;
    status?: QueueTaskStatus;
    offset?: number;
    limit?: number;
    sort?: TSort;
    fields?: Array<
      keyof TQueueTaskDetails | "progressTimeline" | "errorTimeline"
    >;
  }) {
    const ids = typeof opts?.id === "string"
      ? [opts.id]
      : await this.listTaskIds(topic, opts);

    const fields = opts?.fields
      ? Array.from(new Set(["id", ...opts.fields]))
      : undefined;

    const results: Record<string, string>[] = [];

    for (const id of ids) {
      const dataKey = this.resolveKey([topic, "data", id]);

      if (fields?.length) {
        const values = await this.redis.hmget(
          dataKey,
          ...fields,
        );

        results.push(fields.reduce((obj, field, i) => {
          if (values[i] !== null) obj[field] = values[i];

          return obj;
        }, {} as Record<string, string>));
      } else {
        results.push(await this.redis.hgetall(dataKey));
      }
    }

    return await this.resolveTasks<T>(
      topic,
      results,
      {
        progress: fields?.includes("progressTimeline") ?? true,
        error: fields?.includes("errorTimeline") ?? true,
      },
    );
  }

  /**
   * Deletes the given tasks by id under a specific topic
   * @param topic
   * @param taskIds
   * @returns
   */
  public static async delete(
    topic: string,
    ...taskIds: string[]
  ): Promise<number> {
    if (!taskIds.length) return 0;

    const idsKey = this.resolveKey([
      topic,
      "ids",
    ]);
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
      tx.zrem(idsKey, taskId);
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

  /**
   * Deletes all tasks in a topic or all the topics
   * @param topic
   * @returns
   */
  public static async deleteAll(topic?: string): Promise<number> {
    return await this.redis.deleteKeysWithPattern(
      this.resolveKey(topic ? [topic, "*"] : "*"),
    );
  }
}
