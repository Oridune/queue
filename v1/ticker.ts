export interface ITickerResult {
  sleep?: number;
  exit?: true;
}

export class Ticker {
  public timeoutId: ReturnType<typeof setTimeout> | null = null;
  public count = 0;
  public delayMs = 1000;
  public paused = false;

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
    },
  ) {
    this.stop();

    const baseDelay = opts?.delayMs ?? this.delayMs;
    const extraSleep = opts?.sleep ?? 0;
    const delayMs = extraSleep || baseDelay;

    this.timeoutId = setTimeout(async () => {
      if (this.paused) return;

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
    }, delayMs);
  }

  public stop() {
    if (this.timeoutId !== null) {
      clearTimeout(this.timeoutId);

      this.timeoutId = null;
      this.count = 0;
      this.paused = false;
    }
  }
}
