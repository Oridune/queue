// deno-lint-ignore-file no-explicit-any
import "ioredis";

declare module "ioredis" {
    interface Redis {
        fetchAndMoveTasks(
            delayedKey: string,
            waitingKey: string,
            processingKey: string,
            concurrency: number,
            timestamp: number,
            sort: 1 | 0 | -1,
        ): Promise<string[]>;

        moveExpiredTasks(
            processingKey: string,
            failedKey: string,
            dataKeyPrefix: string,
            currentMs: number,
            thresholdMs: number,
        ): Promise<string[]>;

        updateTaskProgress(
            dataKey: string,
            progressListKey: string,
            uuid: string,
            percentage: number,
            log: string,
            timestamp: number,
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

        retryTask(
            namespace: string,
            topic: string,
            taskId: string,
            ...taskIds: string[]
        ): Promise<1>;

        retryAllTasks(
            namespace: string,
            topic: string
        ): Promise<1>;

        listTasks<T extends Record<string, any>>(
            sorted_set_key: string,
            dataKeyPrefix: string,
            sort_dir: 1 | -1,
            offset: number,
            limit: number,
            ...args: string[]
        ): Promise<T[]>;

        deleteKeysWithPattern(
            pattern: string,
            batch_size?: number,
        ): Promise<number>;
    }
}
