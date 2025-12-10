import { throttleCache } from "../common/utils.ts";
import {
  type IQueueEventHandlerOptions,
  LogType,
  type Queue,
  QueueTaskStatus,
  type TQueueProgressFunction,
  type TQueueTaskDetails,
  type TTaskData,
} from "./queue.ts";
import { Ticker } from "./ticker.ts";
import type { TSort } from "./types.ts";

export enum QueueWorkerEvent {
  POOL_STOP = "__onOriduneJobPoolStop",
  INCR_SLOT = "__onOriduneJobPoolIncrSlot",
}

export class QueueWorker<T extends TTaskData> {
  protected stopped = false;

  protected async withHeartbeat<T extends unknown>(
    id: string,
    callback: () => T,
  ) {
    const heartbeatKey = this.queue.resolveKey([this.topic, "heartbeat"]);

    const heartbeat = async () => {
      await this.queue.redis.zadd(
        heartbeatKey,
        Date.now(),
        id,
      );
    };

    heartbeat();

    const beatInterval = setInterval(
      heartbeat,
      this.queue["taskExpiryMs"] / 2,
    );

    const results = await callback();

    await this.queue.redis.zrem(heartbeatKey, id);

    clearInterval(beatInterval);

    return results;
  }

  protected async withTimeout<T extends unknown>(
    callback: (signal?: AbortSignal) => T,
    timeoutMs?: number,
  ) {
    let timer: number | undefined;
    let controller: AbortController | undefined;

    if (timeoutMs) {
      controller = new AbortController();

      timer = setTimeout(
        () => controller!.abort(),
        timeoutMs,
      );
    }

    const results = await callback(controller?.signal);

    clearTimeout(timer);

    return results;
  }

  protected async process(
    index: number,
    taskDetails: TQueueTaskDetails<unknown>,
  ) {
    await this.withHeartbeat(taskDetails.id, async () => {
      const processingKey = this.queue.resolveKey([
        this.topic,
        QueueTaskStatus.PROCESSING,
      ]);
      const completedKey = this.queue.resolveKey([
        this.topic,
        QueueTaskStatus.COMPLETED,
      ]);
      const dataKey = this.queue.resolveKey([
        this.topic,
        "data",
        taskDetails.id,
      ]);

      const progress: TQueueProgressFunction = async (
        percentage,
        log,
      ) => {
        await this.queue.redis.updateTaskProgress(
          dataKey,
          crypto.randomUUID(),
          percentage,
          log ?? "Progress update",
          Date.now(),
        );
      };

      const attempt = Number(taskDetails.attempt ?? 0) + 1;
      const retryCount = Number(taskDetails.retryCount ?? 0);
      const retryDelayMs = Number(taskDetails.retryDelayMs ?? 3000);

      await this.withTimeout(async (signal) => {
        try {
          // Mark attempt
          const timestamp = Date.now();

          const startTx = this.queue.redis.multi();

          startTx.hincrby(dataKey, "attempt", 1);

          if (attempt > 1) {
            startTx.hset(
              dataKey,
              "retriedOn",
              timestamp,
            );

            taskDetails.retriedOn = timestamp.toString();
          } else {
            startTx.hset(
              dataKey,
              "processedOn",
              timestamp,
            );

            taskDetails.processedOn = timestamp.toString();
          }

          await startTx.exec();

          taskDetails.attempt = attempt.toString();

          let data = taskDetails.data as T;

          if (typeof data === "string") {
            try {
              data = JSON.parse(data);
            } catch {
              // Do nothing...
            }
          }

          // Handle task execution
          const results = await this.handlerOpts.handler({
            details: {
              ...taskDetails,
              data,
            },
            progress,
            signal,
          });

          // Mark task completion
          const endTx = this.queue.redis.multi();

          endTx.zadd(completedKey, Number(taskDetails.createdOn), taskDetails.id);
          endTx.zrem(processingKey, taskDetails.id);
          endTx.hmset(dataKey, {
            results: JSON.stringify(results),
            completedOn: Date.now(),
          });

          await endTx.exec();

          return true;
        } catch (err) {
          // deno-lint-ignore no-explicit-any
          const error: any = err;

          this.queue.log(
            LogType.ERROR,
            () => [
              "Topic:",
              this.topic,
              "Index:",
              index,
              "Stack:",
              error,
              "Attempt:",
              attempt,
              "Remaining Attempts:",
              retryCount - attempt,
            ],
          );

          const now = Date.now();
          const failed = attempt >= retryCount;

          await this.queue.redis.updateTaskError(
            this.queue.resolveKey(),
            this.topic,
            taskDetails.id,
            crypto.randomUUID(),
            String(error?.message ?? error),
            String(error?.stack),
            now,
            failed ? 1 : 0,
            failed
              ? Number(taskDetails.priority ?? 0)
              : now + retryDelayMs * attempt,
          );
        }

        await this.cleanup();

        return true;
      }, taskDetails.timeoutMs ?? this.handlerOpts.timeoutMs);
    });
  }

  protected async runJobPool(concurrency: number, sort: TSort) {
    const activeSlots: Array<Promise<unknown>> = [];
    const controllers: Array<AbortController> = [];
    const tickers: Array<Ticker> = [];

    const createSlot = (index: number) => {
      const controller = new AbortController();
      const ticker = new Ticker(this.handlerOpts.executionSchedules);

      return [
        controller,
        ticker,
        new Promise((resolve) => {
          ticker.start(async () => {
            if (this.stopped || controller.signal.aborted) {
              resolve(this.topic);

              this.queue.log(
                LogType.WARN,
                () => [index, this.topic, "Ticker stopped"],
              );

              return {
                exit: true,
              };
            }

            if (await this.queue.isPaused(this.topic)) {
              return {
                sleep: 5000,
              };
            }

            if (!(ticker.count % this.queue["taskRecoveryIteration"])) {
              await this.queue.crashRecovery(this.topic);
            }

            if (typeof this.handlerOpts.rateLimit === "object") {
              const rateLimitKey = this.queue.resolveKey([
                this.topic,
                "rateLimit",
              ]);

              const maxLimit = this.handlerOpts.rateLimit.limit;
              const ttl = this.handlerOpts.rateLimit.ttl;

              const limit = await this.queue.redis.rateLimitIncr(
                rateLimitKey,
                maxLimit,
                ttl,
              );

              if (limit >= maxLimit) {
                return {
                  sleep: 5000,
                };
              }
            }

            const [task] = await this.queue.getNextTasks(
              this.topic,
              1,
              sort,
            );

            if (task && !task[0]) {
              await this.process(index, task[1]);
            }

            return {};
          });
        }),
      ] as const;
    };

    const incrSlot = (index: number) => {
      const [controller, ticker, slot] = createSlot(index);

      controllers.push(controller);
      tickers.push(ticker);
      activeSlots.push(slot);
    };

    const decrSlot = async () => {
      if (activeSlots.length > 1) {
        const controller = controllers.pop();
        const slot = activeSlots.pop();
        const ticker = tickers.pop();

        controller?.abort();
        ticker?.stop();

        await slot;
      }
    };

    for (let i = 0; i < concurrency; i++) incrSlot(i);

    const incrEvent = QueueWorkerEvent.INCR_SLOT + this.topic;

    const redisSubscriber = this.queue.redis.duplicate();

    redisSubscriber.subscribe(incrEvent);
    redisSubscriber.on("message", (channel, message) => {
      if (channel === incrEvent) {
        if (message === "incr") incrSlot(activeSlots.length);
        else if (message === "decr") decrSlot();
      }
    });

    await Promise.all(activeSlots);

    redisSubscriber.disconnect();
  }

  protected cleanup: QueueWorker<T>["_cleanup"];
  protected async _cleanup() {
    await this.cleanupCompletedTasks();
    await this.cleanupFailedTasks();
  }

  protected async cleanupCompletedTasks() {
    if (typeof this.handlerOpts.completedCap !== "number") return;

    const completeCount = await this.queue.countTasks(
      this.topic,
      QueueTaskStatus.COMPLETED,
    );

    if (completeCount > this.handlerOpts.completedCap) {
      await this.deleteLastTasks(
        QueueTaskStatus.COMPLETED,
        completeCount - this.handlerOpts.completedCap,
      );
    }
  }

  protected async cleanupFailedTasks() {
    if (typeof this.handlerOpts.failedCap !== "number") return;

    const failedCount = await this.queue.countTasks(
      this.topic,
      QueueTaskStatus.FAILED,
    );

    if (failedCount > this.handlerOpts.failedCap) {
      await this.deleteLastTasks(
        QueueTaskStatus.FAILED,
        failedCount - this.handlerOpts.failedCap,
      );
    }
  }

  protected async deleteLastTasks(status: QueueTaskStatus, limit: number) {
    if(limit <= 0) return;

    const taskIds = await this.queue.listTaskIds(this.topic, {
      status,
      sort: 1,
      offset: 0,
      limit,
    });

    await this.queue.delete(this.topic, ...taskIds);
  }

  constructor(
    protected queue: typeof Queue,
    protected topic: string,
    protected handlerOpts: IQueueEventHandlerOptions<T>,
  ) {
    this.cleanup = throttleCache(this._cleanup.bind(this), 60000); // Throttle to once per minute
  }

  public async run() {
    this.stopped = false;

    const concurrency =
      (typeof this.handlerOpts.concurrency === "function"
        ? await this.handlerOpts.concurrency()
        : typeof this.handlerOpts.concurrency === "number"
        ? this.handlerOpts.concurrency
        : 1) || 1;
    const sort = this.handlerOpts.sort ?? 1;

    await this.runJobPool(concurrency, sort);

    this.queue.log(
      LogType.INFO,
      () => ["Attempting to stop:", this.topic],
    );

    dispatchEvent(new CustomEvent(QueueWorkerEvent.POOL_STOP + this.topic));
  }

  public async stop() {
    const promise = new Promise((resolve) => {
      addEventListener(QueueWorkerEvent.POOL_STOP + this.topic, resolve, {
        once: true,
      });
    });

    this.stopped = true;

    await promise;
  }
}
