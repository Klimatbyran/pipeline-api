import type Redis from 'ioredis'

export type RedisMemorySnapshot = {
  usedMemoryBytes: number
  maxMemoryBytes: number
  /** `used / max` when maxmemory is set; otherwise null (unlimited). */
  ratio: number | null
}

/** Parse Redis `INFO memory` text into used/max bytes. */
export function parseRedisMemoryInfo(info: string): RedisMemorySnapshot {
  const usedMemoryBytes = readInfoInteger(info, 'used_memory') ?? 0
  const maxMemoryBytes = readInfoInteger(info, 'maxmemory') ?? 0
  const ratio =
    maxMemoryBytes > 0 ? usedMemoryBytes / maxMemoryBytes : null

  return { usedMemoryBytes, maxMemoryBytes, ratio }
}

export async function getRedisMemorySnapshot(
  redis: Redis,
): Promise<RedisMemorySnapshot> {
  const info = await redis.info('memory')
  return parseRedisMemoryInfo(info)
}

function readInfoInteger(info: string, key: string): number | null {
  const match = info.match(new RegExp(`^${key}:(\\d+)\\r?$`, 'm'))
  if (!match) return null
  const value = Number(match[1])
  return Number.isFinite(value) ? value : null
}
