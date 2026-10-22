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

