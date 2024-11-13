import type { Redis } from "ioredis";
import { createSafeRedisLeader } from "safe-redis-leader";

export const leader = (redis: Redis, key: string): Promise<{
    on(ev: string, callback: () => void): void;
    elect(): Promise<void>;
    shutdown(): Promise<void>;
    removeAllListeners(): void;
}> =>
    createSafeRedisLeader({
        asyncRedis: redis,
        ttl: 8000,
        wait: 10000,
        key,
    });
