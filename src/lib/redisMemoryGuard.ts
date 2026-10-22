import type { RedisMemorySnapshot } from './redisMemory'

/** Warn / notify when Redis memory reaches this fraction of maxmemory. */
export const DEFAULT_REDIS_MEMORY_ALERT_RATIO = 0.6

/**
 * Prune every non-live (terminal) BullMQ run when Redis memory reaches this
 * fraction of maxmemory. Prefer clearing finished work over Redis OOM.
 */
export const DEFAULT_REDIS_MEMORY_PRUNE_RATIO = 0.7

/**
 * Emergency fail-safe: clear every BullMQ pipeline run — including live /
 * active / waiting jobs — when Redis memory reaches this fraction of
 * maxmemory. Last resort before noeviction write failures / OOM.
 */
export const DEFAULT_REDIS_MEMORY_FORCE_CLEAR_RATIO = 0.85

export type RedisMemoryGuardDecision = {
  ratio: number | null
  usedMemoryBytes: number
  maxMemoryBytes: number
  /** True at/above the alert threshold (and when maxmemory is configured). */
  shouldAlert: boolean
  /** True at/above the prune threshold — drop all non-active terminal runs. */
  shouldPruneAllTerminal: boolean
  /**
   * True at/above the force-clear threshold — drop every run including live
   * ones. Supersedes the terminal-only prune when both apply.
   */
  shouldForceClearAll: boolean
  /** Human-readable reason when no ratio-based action is possible. */
  skipReason: string | null
}

export function evaluateRedisMemoryGuard(
  snapshot: RedisMemorySnapshot,
  options: {
    alertRatio?: number
    pruneRatio?: number
    forceClearRatio?: number
  } = {},
): RedisMemoryGuardDecision {
  const alertRatio = options.alertRatio ?? DEFAULT_REDIS_MEMORY_ALERT_RATIO
  const pruneRatio = options.pruneRatio ?? DEFAULT_REDIS_MEMORY_PRUNE_RATIO
  const forceClearRatio =
    options.forceClearRatio ?? DEFAULT_REDIS_MEMORY_FORCE_CLEAR_RATIO

  if (snapshot.maxMemoryBytes <= 0 || snapshot.ratio == null) {
    return {
      ratio: null,
      usedMemoryBytes: snapshot.usedMemoryBytes,
      maxMemoryBytes: snapshot.maxMemoryBytes,
      shouldAlert: false,
      shouldPruneAllTerminal: false,
      shouldForceClearAll: false,
      skipReason: 'maxmemory is unset or zero; cannot compute pressure ratio',
    }
  }

  return {
    ratio: snapshot.ratio,
    usedMemoryBytes: snapshot.usedMemoryBytes,
    maxMemoryBytes: snapshot.maxMemoryBytes,
    shouldAlert: snapshot.ratio >= alertRatio,
    shouldPruneAllTerminal: snapshot.ratio >= pruneRatio,
    shouldForceClearAll: snapshot.ratio >= forceClearRatio,
    skipReason: null,
  }
}

export function formatMemoryMiB(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)}MiB`
}

export function formatRatioPercent(ratio: number): string {
  return `${(ratio * 100).toFixed(1)}%`
}
