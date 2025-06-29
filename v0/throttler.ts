// deno-lint-ignore-file no-explicit-any
import { LRUCache } from "./lru.ts";

type Awaited<T> = T extends PromiseLike<infer U> ? U : T;

type CacheEntry<R> = {
    value: R;
    timestamp: number;
};

export class Throttler {
    /**
     * The throttled function caches results based on arguments and respects the specified TTL.
     * Utilizes an LRU cache to manage cache size.
     *
     * @param callback - The function to throttle. Can be synchronous or asynchronous.
     * @param ttlMs - Time-to-live for each cache entry in milliseconds.
     * @param cacheSize - Maximum number of unique argument sets to cache. Defaults to 100.
     * @returns A throttled version of the callback function.
     */
    static throttle<T extends (...args: any[]) => any>(
        label: string,
        callback: T,
        ttlMs: number,
        cacheSize = 100,
    ): (
        ...args: Parameters<T>
    ) => Awaited<ReturnType<T>> | Promise<Awaited<ReturnType<T>>> {
        // Nested Maps for argument-based caching
        const cache = new LRUCache<string, CacheEntry<Awaited<ReturnType<T>>>>(
            cacheSize,
        );

        return function (this: any, ...args: Parameters<T>): any {
            const key = [label, JSON.stringify(args)].join(":");
            const currentTime = Date.now();
            const cached = cache.get(key);

            if (cached && (currentTime - cached.timestamp < ttlMs)) {
                return cached.value;
            }

            const result = callback.apply(this, args);

            if (result instanceof Promise) {
                // Cache the pending promise
                cache.set(key, {
                    value: result as Awaited<ReturnType<T>>,
                    timestamp: currentTime,
                });

                // Upon resolution or rejection, update the cache accordingly
                result
                    .then((resolvedValue) => {
                        cache.set(key, {
                            value: resolvedValue,
                            timestamp: Date.now(),
                        });
                    })
                    .catch(() => {
                        // On error, remove the cache entry to allow retries
                        cache.delete(key);
                    });

                return result;
            } else {
                // For synchronous results, cache the value directly
                cache.set(key, { value: result, timestamp: currentTime });

                return result;
            }
        };
    }
}
