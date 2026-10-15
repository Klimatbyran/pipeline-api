import type { RedisMemorySnapshot } from './redisMemory'

/** Warn / notify when Redis memory reaches this fraction of maxmemory. */
export const DEFAULT_REDIS_MEMORY_ALERT_RATIO = 0.7

/**
 * Prune every non-live (terminal) BullMQ run when Redis memory reaches this
 * fraction of maxmemory. Prefer clearing finished work over Redis OOM.
 */
export const DEFAULT_REDIS_MEMORY_PRUNE_RATIO = 0.8

export type RedisMemoryGuardDecision = {
  ratio: number | null
  usedMemoryBytes: number
  maxMemoryBytes: number
  /** True at/above the alert threshold (and when maxmemory is configured). */
  shouldAlert: boolean
  /** True at/above the prune threshold — drop all non-active terminal runs. */
  shouldPruneAllTerminal: boolean
  /** Human-readable reason when no ratio-based action is possible. */
  skipReason: string | null
}

export function evaluateRedisMemoryGuard(
  snapshot: RedisMemorySnapshot,
  options: {
    alertRatio?: number
    pruneRatio?: number
  } = {},
): RedisMemoryGuardDecision {
  const alertRatio = options.alertRatio ?? DEFAULT_REDIS_MEMORY_ALERT_RATIO
  const pruneRatio = options.pruneRatio ?? DEFAULT_REDIS_MEMORY_PRUNE_RATIO

  if (snapshot.maxMemoryBytes <= 0 || snapshot.ratio == null) {
    return {
      ratio: null,
      usedMemoryBytes: snapshot.usedMemoryBytes,
      maxMemoryBytes: snapshot.maxMemoryBytes,
      shouldAlert: false,
      shouldPruneAllTerminal: false,
      skipReason: 'maxmemory is unset or zero; cannot compute pressure ratio',
    }
  }

  return {
    ratio: snapshot.ratio,
    usedMemoryBytes: snapshot.usedMemoryBytes,
    maxMemoryBytes: snapshot.maxMemoryBytes,
    shouldAlert: snapshot.ratio >= alertRatio,
    shouldPruneAllTerminal: snapshot.ratio >= pruneRatio,
    skipReason: null,
  }
}

export function formatMemoryMiB(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)}MiB`
}

export function formatRatioPercent(ratio: number): string {
  return `${(ratio * 100).toFixed(1)}%`
}
