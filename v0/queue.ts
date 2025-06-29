// deno-lint-ignore-file no-explicit-any

import type { IRedis } from "./types.ts";
import { Redis, type RedisOptions } from "ioredis";
import { Throttler } from "./throttler.ts";
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
}

export type TQueueTaskDetails<T = string> = {
  id: string;
  data: T;
  delayMs?: string;
  retryCount?: string;
  priority?: string;
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
}

export interface IQueueEventHandlerOptions<T extends TTaskData> {
  concurrency?: number | (() => number | Promise<number>);
  sort?: -1 | 1;
  handler: (event: IQueueEvent<T>) => void | Promise<void>;
  readonly shared?: boolean;
}

export interface IQueueInitOptions {
  redis?: TRedisOpts;
  namespace?: string;
  tickMs?: number;
  pauseSleepMs?: number;
  recoveryIteration?: number;
  slowdownIteration?: number;
  slowdownTimes?: number;
  logs?: boolean;
}

export type TRedisOpts = Redis | RedisOptions | (() => Redis | RedisOptions);

export type TSubscriptionDetails = {
  unsubscribe: () => any;
  handlerOpts: IQueueEventHandlerOptions<any>;
};

const DefaultNamespace = ["OriduneQueue"];

export class Queue {
  protected static Ready = false;
  protected static Logs = false;
  protected static Namespace = DefaultNamespace;
  protected static Redis?: Redis;
  protected static RedisOpts?: TRedisOpts;
  protected static TickMs = 1000;
  protected static Ticker?: number;
  protected static Ticks = 0;
  protected static Consumptions = 0;
  protected static Missed = 0;
  protected static IdleCount = 0;
  protected static RecoveryIteration = 10;
  protected static SlowdownIteration = 10;
  protected static SlowdownTimes = 10;
  protected static PauseSleepMs = 5000;
  protected static ExpiredTaskMs = 10000;
  protected static Subscriptions: Map<string, TSubscriptionDetails> = new Map();

  protected static get redis(): IRedis {
    if (!this.Ready) throw new Error("Queue is not initialized yet!");

    return (this.Redis ??= (() => {
      const Opts =
        (typeof this.RedisOpts === "function"
          ? this.RedisOpts()
          : this.RedisOpts) ?? {};

      return new Redis(Opts instanceof Redis ? Opts.options : Opts);
    })() as any);
  }

  protected static prepareRedis() {
    this.redis.defineCommand("fetchAndMoveTasks", {
      numberOfKeys: 3,
      lua: `
            -- Lua script to fetch tasks and move them to the processing set atomically

            -- KEYS:
            -- KEYS[1] - Delayed key
            -- KEYS[2] - Waiting key
            -- KEYS[3] - Processing key

            -- ARGV:
            -- ARGV[1] - Concurrency
            -- ARGV[2] - Timestamp
            -- ARGV[3] - Sort

            local delayedKey = KEYS[1]
            local waitingKey = KEYS[2]
            local processingKey = KEYS[3]
            local concurrency = tonumber(ARGV[1])
            local timestamp = tonumber(ARGV[2])
            local sort = tonumber(ARGV[3])

            -- Fetch delayed tasks
            local delayedIds = {}
            if concurrency > 0 then
                delayedIds = redis.call('ZRANGEBYSCORE', delayedKey, '-inf', timestamp, 'LIMIT', 0, concurrency)
            else
                delayedIds = redis.call('ZRANGEBYSCORE', delayedKey, '-inf', timestamp)
            end

            local delayedCount = #delayedIds

            -- Fetch waiting tasks if needed
            local waitingIds = {}
            if concurrency > 0 and delayedCount < concurrency then
                local remaining = concurrency - delayedCount
                if remaining > 0 then
                    if sort > 0 then
                        waitingIds = redis.call('ZRANGEBYSCORE', waitingKey, '-inf', '+inf', 'LIMIT', 0, remaining)
                    else
                        waitingIds = redis.call('ZREVRANGEBYSCORE', waitingKey, '+inf', '-inf', 'LIMIT', 0, remaining)
                    end
                end
            end

            -- Move tasks to the processing set
            if #delayedIds > 0 or #waitingIds > 0 then
                -- Move delayed tasks
                if #delayedIds > 0 then
                    redis.call('ZREM', delayedKey, unpack(delayedIds))
                    local zaddArgs = {}
                    for _, id in ipairs(delayedIds) do
                        table.insert(zaddArgs, timestamp + 10000)
                        table.insert(zaddArgs, id)
                    end
                    redis.call('ZADD', processingKey, unpack(zaddArgs))
                end

                -- Move waiting tasks
                if #waitingIds > 0 then
                    redis.call('ZREM', waitingKey, unpack(waitingIds))
                    local zaddArgs = {}
                    for _, id in ipairs(waitingIds) do
                        table.insert(zaddArgs, timestamp + 10000)
                        table.insert(zaddArgs, id)
                    end
                    redis.call('ZADD', processingKey, unpack(zaddArgs))
                end
            end

            -- Return the list of moved task IDs
            local movedIds = {}
            for _, id in ipairs(delayedIds) do
                table.insert(movedIds, id)
            end
            
            for _, id in ipairs(waitingIds) do
                table.insert(movedIds, id)
            end

            return movedIds
            `,
    });

    this.redis.defineCommand("moveExpiredTasks", {
      numberOfKeys: 2,
      lua: `
            -- Lua script to move expired tasks from processing to failed

            -- KEYS:
            -- KEYS[1] - Processing key
            -- KEYS[2] - Failed key

            -- ARGV:
            -- ARGV[1] - Data key prefix (e.g., 'topic:data')
            -- ARGV[2] - Current timestamp
            -- ARGV[3] - Expired task threshold (ExpiredTaskMs)

            local processingKey = KEYS[1]
            local failedKey = KEYS[2]
            local dataKeyPrefix = ARGV[1]
            local timestamp = tonumber(ARGV[2])
            local thresholdMs = tonumber(ARGV[3])

            -- Calculate the cutoff timestamp
            local cutoffTimestamp = timestamp - thresholdMs

            -- Fetch expired task IDs from the processing set
            local failedIds = redis.call('ZRANGEBYSCORE', processingKey, '-inf', cutoffTimestamp)

            if #failedIds > 0 then
                -- Remove expired tasks from the processing set
                redis.call('ZREM', processingKey, unpack(failedIds))

                -- Prepare arguments for ZADD to add tasks to the failed set
                local zaddArgs = {}
                for _, id in ipairs(failedIds) do
                    -- Construct the data key for each task
                    local dataKey = dataKeyPrefix .. ':' .. id

                    -- Fetch the 'priority' field from the task's hash
                    local priority = redis.call('HGET', dataKey, 'priority')
                    if not priority then
                        priority = '0' -- Default priority if not set
                    end

                    -- Convert priority to a number
                    priority = tonumber(priority) or 0

                    -- Append priority and task ID to the arguments
                    table.insert(zaddArgs, priority)
                    table.insert(zaddArgs, id)
                end

                -- Add tasks to the failed sorted set with their priorities as scores
                redis.call('ZADD', failedKey, unpack(zaddArgs))

                -- Return the list of moved task IDs
                return failedIds
            else

                -- No expired tasks to process
                return {}
            end
            `,
    });

    this.redis.defineCommand("updateTaskProgress", {
      numberOfKeys: 3, // Number of KEYS the script expects
      lua: `
            -- Lua script to update task progress

            -- KEYS:
            -- KEYS[1] - dataKey (e.g., "topic:data:taskId")
            -- KEYS[2] - progressListKey (e.g., "topic:data:taskId:progress")
            -- KEYS[3] - uuid

            -- ARGV:
            -- ARGV[1] - percentage (integer or string representing integer)
            -- ARGV[2] - log (string)
            -- ARGV[3] - timestamp (integer, milliseconds since epoch)

            local dataKey = KEYS[1]
            local progressListKey = KEYS[2]
            local uuid = KEYS[3]

            local percentage = tonumber(ARGV[1])
            local log = ARGV[2]
            local timestamp = tonumber(ARGV[3])

            local progressListItemKey = progressListKey .. ':' .. uuid

            -- Set the 'progress' field in dataKey
            redis.call('HSET', dataKey, 'progress', percentage)
          
            -- Push the UUID to the progressListKey
            redis.call('RPUSH', progressListKey, uuid)
          
            -- Set multiple fields in progressDetailsKey
            redis.call('HSET', progressListItemKey, 'percentage', percentage, 'log', log, 'timestamp', timestamp)
          
            -- Return success
            return 1
            `,
    });

    this.redis.defineCommand("updateTaskError", {
      numberOfKeys: 4, // Number of KEYS the script expects
      lua: `
            -- Lua script to update task error

            -- KEYS:
            -- KEYS[1] - namespace
            -- KEYS[2] - topic
            -- KEYS[3] - taskId
            -- KEYS[4] - uuid

            -- ARGV:
            -- ARGV[1] - message (error message string)
            -- ARGV[2] - stack (error stack string)
            -- ARGV[3] - timestamp (integer, milliseconds since epoch)
            -- ARGV[4] - moveToFailed (0 - 1)
            -- ARGV[5] - priority

            local namespace = KEYS[1]
            local topic = KEYS[2]
            local taskId = KEYS[3]
            local uuid = KEYS[4]

            local message = ARGV[1]
            local stack = ARGV[2]
            local timestamp = tonumber(ARGV[3])
            local moveToFailed = tonumber(ARGV[4])
            local priority = tonumber(ARGV[5]) or 0
            
            local dataKey = namespace .. ':' .. topic .. ':' .. 'data' .. ':' .. taskId
            local processingKey = namespace .. ':' .. topic .. ':' .. 'processing'
            local failedKey = namespace .. ':' .. topic .. ':' .. 'failed'
            local errorListKey = dataKey .. ':' .. 'error'
            local errorListItemKey = errorListKey .. ':' .. uuid
          
            -- Push the UUID to the errorListKey
            redis.call('RPUSH', errorListKey, uuid)
          
            -- Set multiple fields in errorDetailsKey
            redis.call('HSET', errorListItemKey, 'message', message, 'stack', stack, 'timestamp', timestamp)

            if moveToFailed > 0 then
                redis.call('ZREM', processingKey, taskId)
                redis.call('ZADD', failedKey, priority, taskId)
            end
          
            -- Return success
            return 1
            `,
    });

    this.redis.defineCommand("retryTask", {
      numberOfKeys: 2, // Number of KEYS the script expects
      lua: `
            -- Lua script to update task error

            -- KEYS:
            -- KEYS[1] - namespace
            -- KEYS[2] - topic

            -- ARGV:
            -- ARGV[1 ... n] - optional list of taskIds

            local namespace = KEYS[1]
            local topic = KEYS[2]
            
            local failedKey = namespace .. ':' .. topic .. ':' .. 'failed'
            local waitingKey = namespace .. ':' .. topic .. ':' .. 'waiting'
          
            for i = 1, #ARGV do
                local taskId = ARGV[i]
                local priority = redis.call('ZSCORE', failedKey, taskId);

                if priority then
                    redis.call('ZREM', failedKey, taskId)
                    redis.call('ZADD', waitingKey, priority, taskId)
                end
            end
          
            -- Return success
            return 1
            `,
    });

    this.redis.defineCommand("retryAllTasks", {
      numberOfKeys: 2, // Number of KEYS the script expects
      lua: `
            -- Lua script to update task error

            -- KEYS:
            -- KEYS[1] - namespace
            -- KEYS[2] - topic

            local namespace = KEYS[1]
            local topic = KEYS[2]
            
            local failedKey = namespace .. ':' .. topic .. ':' .. 'failed'
            local waitingKey = namespace .. ':' .. topic .. ':' .. 'waiting'

            local tasks = redis.call('ZRANGE', failedKey, 0, -1, 'WITHSCORES')
          
            for i = 1, #tasks, 2 do
                local taskId = tasks[i]
                local priority = tasks[i + 1]

                redis.call('ZREM', failedKey, taskId)
                redis.call('ZADD', waitingKey, priority, taskId)
            end
          
            -- Return success
            return 1
            `,
    });

    this.redis.defineCommand("listTasks", {
      numberOfKeys: 2,
      lua: `
            -- Lua script to list tasks with optional field fetching

            -- KEYS:
            -- KEYS[1] - sorted_set_key (e.g., "topic:status")
            -- KEYS[2] - dataKeyPrefix (e.g., "topic:data")

            -- ARGV:
            -- ARGV[1] - sort direction (1 for ZRANGEBYSCORE, -1 for ZREVRANGEBYSCORE)
            -- ARGV[2] - offset (number of elements to skip)
            -- ARGV[3] - limit (maximum number of elements to retrieve)
            -- ARGV[4 ... n] - optional list of fields to fetch from each hash

            -- Extract parameters
            local sorted_set_key = KEYS[1]
            local dataKeyPrefix = KEYS[2]
            local sort_dir = tonumber(ARGV[1])
            local offset = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])

            -- Extract fields list if provided
            local fields = {}
            for i = 4, #ARGV do
                table.insert(fields, ARGV[i])
            end

            -- Validate sort_dir
            if sort_dir ~= 1 and sort_dir ~= -1 then
                error("Invalid sort direction. Use 1 for ascending or -1 for descending.")
            end

            -- Retrieve task IDs based on sort direction
            local ids
            if sort_dir > 0 then
                ids = redis.call('ZRANGEBYSCORE', sorted_set_key, '-inf', '+inf', 'LIMIT', offset, limit)
            else
                ids = redis.call('ZREVRANGEBYSCORE', sorted_set_key, '+inf', '-inf', 'LIMIT', offset, limit)
            end

            -- Initialize the result table
            local tasks = {}

            -- Iterate over each ID and fetch task details
            for i, id in ipairs(ids) do
                local dataKey = dataKeyPrefix .. ':' .. id
                if #fields > 0 then
                    -- Fetch specified fields using HMGET
                    local task = redis.call('HMGET', dataKey, unpack(fields))
                    table.insert(tasks, task)
                else
                    -- Fetch all fields using HGETALL
                    local task = redis.call('HGETALL', dataKey)
                    table.insert(tasks, task)
                end
            end

            -- Return the list of tasks
            return tasks
            `,
    });

    this.redis.defineCommand("deleteKeysWithPattern", {
      numberOfKeys: 1,
      lua: `
            -- Lua script to delete keys matching a pattern without returning the keys

            -- KEYS[1]: The key pattern to match (e.g., "namespace:*")
            -- ARGV[1]: The batch size (number of keys to delete per iteration)

            local pattern = KEYS[1]
            local batch_size = tonumber(ARGV[1]) or 10000 -- Default batch size

            local cursor = "0"
            local deleted_count = 0

            repeat
                -- SCAN to iterate over keys matching the pattern
                local result = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", batch_size)
                cursor = result[1]
                local keys = result[2]

                if #keys > 0 then
                    -- Delete keys in the current batch
                    local deleted = redis.call("DEL", unpack(keys))
                    deleted_count = deleted_count + (deleted or 0)
                end
            until cursor == "0"

            -- Return the total number of deleted keys
            return deleted_count
            `,
    });
  }

  protected static log(type: LogType, args: () => any[]) {
    if (this.Logs) {
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

  protected static resolveKey(
    key?: string | string[],
    namespace?: string | string[],
  ): string {
    const Namespace = namespace
      ? namespace instanceof Array ? namespace : [namespace]
      : this.Namespace;

    if (key === undefined) return Namespace.join(":");

    const Key = key instanceof Array ? key : [key];

    return [...Namespace, ...Key].join(":");
  }

  protected static tick(onTick: () => void | Promise<void>) {
    this.Ticker = setTimeout(
      async () => {
        if (this.Subscriptions.size) {
          if (await this.isPaused()) {
            this.log(
              LogType.WARN,
              () => ["Queue status paused!"],
            );

            await new Promise((_) => setTimeout(_, this.PauseSleepMs));
          } else await onTick();
        }

        if (this.IdleCount >= this.SlowdownIteration) {
          await new Promise((_) =>
            setTimeout(
              _,
              (this.TickMs * this.SlowdownTimes) - this.TickMs,
            )
          );
        }

        this.tick(onTick);
        this.Ticks++;
      },
      this.TickMs,
    );
  }

  protected static async recover(topic: string) {
    if (this.Ticks % this.RecoveryIteration) return;

    const failedIds = await this.redis.moveExpiredTasks(
      this.resolveKey([topic, QueueTaskStatus.PROCESSING]),
      this.resolveKey([topic, QueueTaskStatus.FAILED]),
      this.resolveKey([topic, "data"]),
      Date.now(),
      this.ExpiredTaskMs,
    );

    failedIds.length &&
      this.log(LogType.INFO, () => ["Recovered Tasks:", failedIds]);
  }

  protected static async consume(
    topic: string,
    handlerOpts: IQueueEventHandlerOptions<Record<string, unknown>>,
  ) {
    const timestamp = Date.now();

    if (await this.isPaused(topic)) {
      this.log(LogType.WARN, () => ["Topic status paused:", topic]);

      return;
    }

    // Crash recovery
    await this.recover(topic);

    const concurrency =
      (typeof handlerOpts.concurrency === "function"
        ? await handlerOpts.concurrency()
        : typeof handlerOpts.concurrency === "number"
        ? handlerOpts.concurrency
        : 1) || 1;

    const movedTaskIds = await this.redis.fetchAndMoveTasks(
      this.resolveKey([topic, QueueTaskStatus.DELAYED]),
      this.resolveKey([topic, QueueTaskStatus.WAITING]),
      this.resolveKey([topic, QueueTaskStatus.PROCESSING]),
      concurrency,
      timestamp,
      handlerOpts.sort ?? -1,
    );

    this.log(LogType.INFO, () => ["Captured Tasks:", movedTaskIds]);

    if (movedTaskIds.length) {
      this.Consumptions++;
      this.IdleCount = 0;

      // Fetch tasks data
      const pl = this.redis.pipeline();

      movedTaskIds.forEach((id) => {
        pl.hgetall(this.resolveKey([topic, "data", id]));
      });

      const tasks = await pl.exec() as [
        Error | null,
        TQueueTaskDetails,
      ][];

      await this.processTasks(tasks, topic, handlerOpts);
    } else {
      this.Missed++;
      this.IdleCount++;
    }

    this.log(
      LogType.INFO,
      () => ["Tick In:", `${Date.now() - timestamp}ms`],
    );
  }

  protected static async processTasks(
    tasks: [Error | null, TQueueTaskDetails][],
    topic: string,
    handlerOpts: IQueueEventHandlerOptions<Record<string, unknown>>,
  ) {
    if (!tasks.length) return;

    await Promise.all(
      tasks.map(async ([error, task]) => {
        if (error) return;

        const progress: TQueueProgressFunction = async (
          percentage,
          log,
        ) => {
          const dataKey = this.resolveKey([topic, "data", task.id]);
          const progressListKey = this.resolveKey(
            "progress",
            dataKey,
          );

          await this.redis.updateTaskProgress(
            dataKey,
            progressListKey,
            crypto.randomUUID(),
            percentage,
            log ?? "Progress update",
            Date.now(),
          );
        };

        const lockInterval = setInterval(
          async () => {
            await this.redis.zadd(
              this.resolveKey([
                topic,
                QueueTaskStatus.PROCESSING,
              ]),
              Date.now(),
              task.id,
            );
          },
          (this.ExpiredTaskMs / 4) * 3,
        );

        const retryCount = Number(task.retryCount ?? 0);

        let currentAttempts = Number(task.attempt ?? 0);

        do {
          currentAttempts++;

          const processingKey = this.resolveKey([
            topic,
            QueueTaskStatus.PROCESSING,
          ]);
          const completedKey = this.resolveKey([
            topic,
            QueueTaskStatus.COMPLETED,
          ]);
          const dataKey = this.resolveKey([topic, "data", task.id]);

          try {
            // Mark attempt
            const timestamp = Date.now();
            const startTx = this.redis.multi();

            startTx.hincrby(dataKey, "attempt", 1);

            if (currentAttempts > 1) {
              startTx.hset(
                dataKey,
                "retriedOn",
                timestamp,
              );

              task.retriedOn = timestamp.toString();
            } else {
              startTx.hset(
                dataKey,
                "processedOn",
                timestamp,
              );

              task.processedOn = timestamp.toString();
            }

            await startTx.exec();

            task.attempt = currentAttempts.toString();

            // Handle event
            await handlerOpts.handler({
              details: {
                ...task,
                data: JSON.parse(task.data),
              },
              progress,
            });

            // Mark completion
            const endTx = this.redis.multi();

            endTx.zrem(processingKey, task.id);
            endTx.zadd(completedKey, 0, task.id);
            endTx.hset(dataKey, "completedOn", Date.now());

            await endTx.exec();

            break;
          } catch (err) {
            const error: any = err;

            this.log(
              LogType.ERROR,
              () => [
                "Stack:",
                error,
                "Attempt:",
                currentAttempts,
                "Remaining Attempts:",
                retryCount - currentAttempts,
              ],
            );

            await this.redis.updateTaskError(
              this.resolveKey(),
              topic,
              task.id,
              crypto.randomUUID(),
              String(error?.message ?? error),
              String(error?.stack),
              Date.now(),
              currentAttempts >= retryCount ? 1 : 0,
              Number(task.priority ?? 0),
            );
          }
        } while (currentAttempts < retryCount);

        clearInterval(lockInterval);
      }),
    );
  }

  static start(opts?: IQueueInitOptions, silent = false) {
    if (this.Ready) {
      if (silent) return;
      else throw new Error("Queue is already initialized!");
    }

    // Customize options
    if (typeof opts === "object") {
      this.RedisOpts = opts.redis;
      opts.namespace && this.Namespace.push(opts.namespace);
      opts.tickMs && (this.TickMs = opts.tickMs);
      opts.pauseSleepMs && (this.PauseSleepMs = opts.pauseSleepMs);
      opts.recoveryIteration &&
        (this.RecoveryIteration = opts.recoveryIteration);
      opts.slowdownIteration &&
        (this.SlowdownIteration = opts.slowdownIteration);
      opts.slowdownTimes &&
        (this.SlowdownTimes = opts.slowdownTimes);
      typeof opts.logs === "boolean" && (this.Logs = opts.logs);
    }

    this.Ready = true;

    // Register custom redis commands
    this.prepareRedis();

    this.tick(async () => {
      await Promise.all(
        this.Subscriptions.entries().map(
          ([topic, { handlerOpts }]) => this.consume(topic, handlerOpts),
        ),
      );
    });

    this.log(LogType.INFO, () => ["Queue started!"]);
  }

  static async stop(endConnection = false) {
    clearTimeout(this.Ticker);

    await Promise.all(
      this.Subscriptions.entries().map(
        ([, { unsubscribe }]) => unsubscribe(),
      ),
    );

    if (endConnection) {
      this.redis.disconnect();
      this.Redis = undefined;
    }

    this.log(LogType.INFO, () => ["Queue stopped!"]);
  }

  static isPaused: (topic?: string | undefined) => number | Promise<number> =
    Throttler.throttle(
      "checkIsPaused",
      async (topic?: string) => {
        return await this.redis.getbit(
          this.resolveKey(topic ? [topic, "isPaused"] : "isPaused"),
          1,
        );
      },
      5000,
    );

  static async pause(topic?: string) {
    await this.redis.setbit(
      this.resolveKey(topic ? [topic, "isPaused"] : "isPaused"),
      1,
      1,
    );
  }

  static async resume(topic?: string) {
    await this.redis.setbit(
      this.resolveKey(topic ? [topic, "isPaused"] : "isPaused"),
      1,
      0,
    );
  }

  static async enqueue<T extends TTaskData>(
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

  static async updateTask<T extends TTaskData>(
    topic: string,
    payload: IQueueTaskPayload<T>,
  ) {
    const { id, ...rest } = payload;

    const dataKey = this.resolveKey([topic, "data", id]);

    await this.redis.hmset(dataKey, {
      ...rest,
      data: typeof rest.data === "object" && rest.data !== null
        ? JSON.stringify(rest.data)
        : "{}",
      updatedOn: Date.now(),
    });
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
  ) {
    const subscribe = (unsubscribe: () => any) => {
      this.Subscriptions.set(topic, { unsubscribe, handlerOpts });

      this.log(
        LogType.INFO,
        () => ["Subscription added:", topic, handlerOpts],
      );
    };

    const unsubscribe = () => {
      this.Subscriptions.delete(topic);
    };

    if (handlerOpts.shared) subscribe(unsubscribe);
    else await this.acquireLock(topic, subscribe, unsubscribe);
  }

  static async unsubscribe(topic: string) {
    await this.Subscriptions.get(topic)?.unsubscribe();
  }

  static async listTaskIds(topic: string, status: QueueTaskStatus, opts?: {
    offset?: number;
    limit?: number;
    sort?: -1 | 1;
  }): Promise<string[]> {
    const sort = opts?.sort ?? 1;
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
    sort?: -1 | 1;
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
