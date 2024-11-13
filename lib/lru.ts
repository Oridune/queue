export class LRUCache<K, V> {
    private cache: Map<K, V>;
    private capacity: number;

    constructor(capacity = 100) {
        this.cache = new Map();
        this.capacity = capacity;
    }

    get(key: K): V | undefined {
        if (!this.cache.has(key)) return undefined;

        const value = this.cache.get(key)!;

        // Move key to the end to mark it as recently used
        this.cache.delete(key);
        this.cache.set(key, value);

        return value;
    }

    set(key: K, value: V): void {
        if (this.cache.has(key)) this.cache.delete(key);
        else if (this.cache.size >= this.capacity) {
            // Remove the least recently used item (first item in the Map)
            const lruKey = this.cache.keys().next().value;
            lruKey && this.cache.delete(lruKey);
        }

        this.cache.set(key, value);
    }

    delete(key: K): void {
        this.cache.delete(key);
    }

    clear(): void {
        this.cache.clear();
    }
}
