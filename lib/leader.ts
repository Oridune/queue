import type { Redis } from "ioredis";
import { createSafeRedisLeader } from "safe-redis-leader";

export type LeaderOpts = {
  ttl?: number;
  wait?: number;
};

export const leader = (redis: Redis, key: string, opts?: LeaderOpts): Promise<{
  on(ev: string, callback: () => void): void;
  elect(): Promise<void>;
  shutdown(): Promise<void>;
  removeAllListeners(): void;
}> =>
  createSafeRedisLeader({
    asyncRedis: redis,
    ttl: opts?.ttl ?? 8000,
    wait: opts?.wait ?? 10000,
    key,
  });
