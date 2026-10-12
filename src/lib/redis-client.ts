import Redis from "ioredis";
import redisConfig from "../config/redis";

let client: Redis | null = null;

export function getRedisClient(): Redis {
  if (!client) {
    client = new Redis({
      host: redisConfig.host,
      port: redisConfig.port,
      ...(redisConfig.password ? { password: redisConfig.password } : {}),
      // BullMQ uses ioredis too; keep retry behavior reasonable for API.
      maxRetriesPerRequest: 3,
      // Caps reconnect backoff at 10s (ioredis's own default caps at 2s but
      // retries forever either way -- this only smooths the retry cadence,
      // it does NOT address the bigger risk: enableOfflineQueue defaults to
      // true, so every command issued during an outage queues client-side
      // instead of failing fast, and flushes as one burst the moment Redis
      // reconnects. Fixing that needs enableOfflineQueue: false, which
      // requires auditing every call site through this singleton to handle
      // rejected commands during an outage -- a bigger change, left for a
      // deliberate follow-up rather than bundled in here.
      retryStrategy: (times) => Math.min(times * 200, 10_000),
      // ioredis's own default is keepAlive: 0 -- technically "on" (it still
      // calls socket.setKeepAlive(true, 0)), but a 0 delay means Node falls
      // back to the OS's own keepalive timer, which defaults to 2+ hours on
      // most Linux systems. In practice that means no proactive detection
      // of a dead connection at all: this singleton could hold a socket to
      // a Redis pod that's long gone, believing it's fine, until it
      // actually tries to use it and gets a failure. 30s makes the OS
      // actively probe and notice a dead peer promptly instead.
      keepAlive: 30_000,
    });
  }
  return client;
}

