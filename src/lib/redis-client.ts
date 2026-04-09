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
    });
  }
  return client;
}

