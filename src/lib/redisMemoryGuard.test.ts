import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { parseRedisMemoryInfo } from './redisMemory'
import {
  DEFAULT_REDIS_MEMORY_ALERT_RATIO,
  DEFAULT_REDIS_MEMORY_FORCE_CLEAR_RATIO,
  DEFAULT_REDIS_MEMORY_PRUNE_RATIO,
  evaluateRedisMemoryGuard,
} from './redisMemoryGuard'

describe('parseRedisMemoryInfo', () => {
  it('reads used_memory and maxmemory', () => {
    const snapshot = parseRedisMemoryInfo(
      ['# Memory', 'used_memory:3221225472', 'maxmemory:4294967296', ''].join(
        '\r\n',
      ),
    )

    assert.equal(snapshot.usedMemoryBytes, 3221225472)
    assert.equal(snapshot.maxMemoryBytes, 4294967296)
    assert.ok(snapshot.ratio != null)
    assert.ok(Math.abs(snapshot.ratio! - 0.75) < 1e-9)
  })

  it('returns null ratio when maxmemory is unlimited', () => {
    const snapshot = parseRedisMemoryInfo(
      'used_memory:1000\r\nmaxmemory:0\r\n',
    )
    assert.equal(snapshot.ratio, null)
  })
})

describe('evaluateRedisMemoryGuard', () => {
  it('alerts at 60% and does not prune yet', () => {
    const decision = evaluateRedisMemoryGuard({
      usedMemoryBytes: 2_500_000_000,
      maxMemoryBytes: 4_000_000_000,
      ratio: 0.625,
    })

    assert.equal(decision.shouldAlert, true)
    assert.equal(decision.shouldPruneAllTerminal, false)
    assert.equal(decision.shouldForceClearAll, false)
    assert.equal(decision.skipReason, null)
  })

  it('prunes all terminal runs at 70% but does not force-clear', () => {
    const decision = evaluateRedisMemoryGuard({
      usedMemoryBytes: 3_000_000_000,
      maxMemoryBytes: 4_000_000_000,
      ratio: 0.75,
    })

    assert.equal(decision.shouldAlert, true)
    assert.equal(decision.shouldPruneAllTerminal, true)
    assert.equal(decision.shouldForceClearAll, false)
  })

  it('force-clears all runs at 85%', () => {
    const decision = evaluateRedisMemoryGuard({
      usedMemoryBytes: 3_500_000_000,
      maxMemoryBytes: 4_000_000_000,
      ratio: 0.875,
    })

    assert.equal(decision.shouldAlert, true)
    assert.equal(decision.shouldPruneAllTerminal, true)
    assert.equal(decision.shouldForceClearAll, true)
  })

  it('stays quiet below the alert threshold', () => {
    const decision = evaluateRedisMemoryGuard({
      usedMemoryBytes: 2_000_000_000,
      maxMemoryBytes: 4_000_000_000,
      ratio: 0.5,
    })

    assert.equal(decision.shouldAlert, false)
    assert.equal(decision.shouldPruneAllTerminal, false)
    assert.equal(decision.shouldForceClearAll, false)
  })

  it('skips when maxmemory is unset', () => {
    const decision = evaluateRedisMemoryGuard({
      usedMemoryBytes: 1_000,
      maxMemoryBytes: 0,
      ratio: null,
    })

    assert.equal(decision.shouldAlert, false)
    assert.equal(decision.shouldPruneAllTerminal, false)
    assert.equal(decision.shouldForceClearAll, false)
    assert.match(decision.skipReason ?? '', /maxmemory/)
  })

  it('uses default thresholds', () => {
    assert.equal(DEFAULT_REDIS_MEMORY_ALERT_RATIO, 0.6)
    assert.equal(DEFAULT_REDIS_MEMORY_PRUNE_RATIO, 0.7)
    assert.equal(DEFAULT_REDIS_MEMORY_FORCE_CLEAR_RATIO, 0.85)
  })
})
