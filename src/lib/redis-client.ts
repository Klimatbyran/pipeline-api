import Redis from "ioredis";
import redisConfig from "../config/redis";

let client: Redis | null = null;

function createRedisClient(): Redis {
  return new Redis({
    host: redisConfig.host,
    port: redisConfig.port,
    ...(redisConfig.password ? { password: redisConfig.password } : {}),
    maxRetriesPerRequest: 3,
    // Fail immediately while Redis is down instead of buffering commands
    // in-process and flushing them as one burst on reconnect.
    enableOfflineQueue: false,
    retryStrategy: (attemptCount) => Math.min(attemptCount * 200, 10_000),
    keepAlive: 30_000,
  });
}

export function getRedisClient(): Redis {
  if (!client) {
    client = createRedisClient();
  }
  return client;
}

/**
 * Wait until the ioredis client is ready to accept commands.
 *
 * Required for one-shot CronJobs when `enableOfflineQueue` is false: the
 * first command (e.g. `INFO memory`) can race the TCP/auth handshake and
 * throw "Stream isn't writeable and enableOfflineQueue options is false",
 * failing the Job in a few seconds.
 */
export async function waitForRedisReady(
  redis: Pick<Redis, "status" | "once" | "off">,
  options: { timeoutMs?: number } = {},
): Promise<void> {
  if (redis.status === "ready") return;

  const timeoutMs = options.timeoutMs ?? 15_000;

  await new Promise<void>((resolve, reject) => {
    let settled = false;

    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      redis.off("ready", onReady);
      fn();
    };

    const onReady = () => finish(() => resolve());

    const timer = setTimeout(() => {
      finish(() =>
        reject(
          new Error(
            `Redis not ready within ${timeoutMs}ms (status=${redis.status})`,
          ),
        ),
      );
    }, timeoutMs);

    // Only wait for `ready`. Do not reject on `close` — ioredis emits that
    // during normal reconnect while retryStrategy is still trying, and a
    // later attempt can still reach ready within the timeout.
    redis.once("ready", onReady);

    // Status may have flipped between the early return and listener attach.
    if (redis.status === "ready") {
      finish(() => resolve());
    }
  });
}

export function isRedisUnavailableError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const message = error.message.toLowerCase();
  return (
    message.includes("enableofflinequeue") ||
    message.includes("connection is closed") ||
    message.includes("econnrefused") ||
    message.includes("etimedout")
  );
}
