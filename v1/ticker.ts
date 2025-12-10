import type { TSchedules } from "./types.ts";

export interface ITickerResult {
  sleep?: number;
  exit?: boolean;
}

export class Ticker {
  public timeoutId: ReturnType<typeof setTimeout> | null = null;
  public count = 0;
  public delayMs = 1000;
  public paused = false;

  constructor(public schedule?: TSchedules) {}

  public get isRunning(): boolean {
    return this.timeoutId !== null && !this.paused;
  }

  public get isPaused(): boolean {
    return this.paused;
  }

  public get isStopped(): boolean {
    return !this.isRunning && !this.isPaused;
  }

  public resume() {
    this.paused = false;
  }

  public pause() {
    this.paused = true;
  }

  public start(
    onTick: () => ITickerResult | Promise<ITickerResult>,
    opts?: {
      delayMs?: number;
      sleep?: number;
      immediate?: boolean;
    },
  ) {
    this.stop();

    const executeAndSchedule = async () => {
      if (this.paused) {
        // Reschedule to check again after a short interval
        this.timeoutId = setTimeout(executeAndSchedule, this.delayMs);
        return;
      }

      if (!await this.isInSchedule()) {
        this.timeoutId = setTimeout(executeAndSchedule, 60000); // 1 minute delay
        return;
      }

      try {
        const { sleep, exit } = await onTick();

        this.count++;

        if (!exit) {
          this.start(onTick, {
            delayMs: opts?.delayMs,
            sleep,
          });
        }
      } catch (e) {
        console.error("Ticker error:", e);

        this.start(onTick, {
          delayMs: opts?.delayMs,
        });
      }
    };

    if (opts?.immediate) {
      executeAndSchedule();
    } else {
      const baseDelay = opts?.delayMs ?? this.delayMs;
      const extraSleep = opts?.sleep ?? 0;
      const delayMs = extraSleep || baseDelay;

      this.timeoutId = setTimeout(executeAndSchedule, delayMs);
    }
  }

  public stop() {
    if (this.timeoutId !== null) {
      clearTimeout(this.timeoutId);

      this.timeoutId = null;
      this.count = 0;
      this.paused = false;
    }
  }

  public async isInSchedule(date: Date = new Date()): Promise<boolean> {
    if (!this.schedule) return true;

    const time = date.getTime();
    const schedule = typeof this.schedule === "function"
      ? await this.schedule()
      : this.schedule;

    for (const [start, end] of schedule) {
      if (time >= start.getTime() && time <= end.getTime()) {
        return true;
      }
    }

    return false;
  }
}
