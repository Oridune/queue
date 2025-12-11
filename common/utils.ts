export function throttleCache<T extends (...args: unknown[]) => unknown>(
  callback: T,
  ttl: number,
): (...args: Parameters<T>) => ReturnType<T> {
  let lastTime = 0;
  let cachedResult: ReturnType<T>;

  return function (...args: Parameters<T>) {
    const now = Date.now();

    if (now - lastTime > ttl) {
      lastTime = now;
      cachedResult = callback(...args) as typeof cachedResult;
    }

    return cachedResult;
  };
}

export const paginated = async (
  limit: number,
  callback: (offset: number, limit: number) => Promise<number>,
) => {
  let length = 0;
  let offset = 0;

  do {
    length = await callback(offset, limit);

    offset += limit;
  } while (length === limit);
};
