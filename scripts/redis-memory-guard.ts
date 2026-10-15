/**
 * Periodic Redis memory guard for BullMQ.
 *
 * - At >= 70% of maxmemory: emit a loud alert log (and optional webhook).
 * - At >= 80%: prune every non-live (terminal) pipeline run — completed,
 *   failed, skipped — so Redis can recover. Live/active runs are never removed.
 *
 * Unlike the garbo post-success prune hook (which only fires on successful /
 * no-emissions finishes) and the nightly keep-15 cron, the 80% path clears
 * *all* finished work. Rerunning a report is cheaper than Redis OOM.
 */
import { getRedisClient } from '../src/lib/redis-client'
import { getRedisMemorySnapshot } from '../src/lib/redisMemory'
import {
  DEFAULT_REDIS_MEMORY_ALERT_RATIO,
  DEFAULT_REDIS_MEMORY_PRUNE_RATIO,
  evaluateRedisMemoryGuard,
  formatMemoryMiB,
  formatRatioPercent,
} from '../src/lib/redisMemoryGuard'
import { RunRetentionService } from '../src/services/RunRetentionService'

function readRatioEnv(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw == null || raw.trim() === '') return fallback
  const value = Number(raw)
  if (!Number.isFinite(value) || value <= 0 || value > 1) {
    throw new Error(`${name} must be a number in (0, 1], got ${raw}`)
  }
  return value
}

async function postAlertWebhook(message: string): Promise<void> {
  const webhookUrl = process.env.REDIS_MEMORY_ALERT_WEBHOOK_URL?.trim()
  if (!webhookUrl) return

  try {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ content: message }),
    })
    if (!response.ok) {
      console.error('[RedisMemoryGuard] alert webhook failed', {
        status: response.status,
        body: await response.text().catch(() => ''),
      })
    }
  } catch (error) {
    console.error('[RedisMemoryGuard] alert webhook error', error)
  }
}

async function main() {
  const alertRatio = readRatioEnv(
    'REDIS_MEMORY_ALERT_RATIO',
    DEFAULT_REDIS_MEMORY_ALERT_RATIO,
  )
  const pruneRatio = readRatioEnv(
    'REDIS_MEMORY_PRUNE_RATIO',
    DEFAULT_REDIS_MEMORY_PRUNE_RATIO,
  )
  const dryRun = process.argv.includes('--dry-run')

  const redis = getRedisClient()
  const snapshot = await getRedisMemorySnapshot(redis)
  const decision = evaluateRedisMemoryGuard(snapshot, {
    alertRatio,
    pruneRatio,
  })

  const ratioLabel =
    decision.ratio == null ? 'n/a' : formatRatioPercent(decision.ratio)

  console.info('[RedisMemoryGuard] snapshot', {
    used: formatMemoryMiB(decision.usedMemoryBytes),
    max: formatMemoryMiB(decision.maxMemoryBytes),
    ratio: ratioLabel,
    alertRatio,
    pruneRatio,
    dryRun,
    skipReason: decision.skipReason,
  })

  if (decision.skipReason) {
    console.warn(`[RedisMemoryGuard] skipping: ${decision.skipReason}`)
    await redis.quit()
    return
  }

  if (decision.shouldAlert) {
    const message = [
      '🚨 Redis memory pressure',
      `${ratioLabel} used (${formatMemoryMiB(decision.usedMemoryBytes)} / ${formatMemoryMiB(decision.maxMemoryBytes)})`,
      decision.shouldPruneAllTerminal
        ? `≥ ${formatRatioPercent(pruneRatio)} — pruning all non-active BullMQ runs`
        : `≥ ${formatRatioPercent(alertRatio)} alert threshold`,
      dryRun ? '(dry-run)' : '',
    ]
      .filter(Boolean)
      .join(' · ')

    console.error(`[RedisMemoryGuard] ALERT ${message}`)
    await postAlertWebhook(message)
  }

  if (decision.shouldPruneAllTerminal) {
    const service = await RunRetentionService.getRunRetentionService()
    // keepCount 0 = no protected terminal runs; live runs are still skipped.
    const result = await service.pruneRuns({ keepCount: 0, dryRun })
    console.info('[RedisMemoryGuard] emergency prune result', result)
  } else {
    console.info('[RedisMemoryGuard] below prune threshold; no prune')
  }

  try {
    await redis.quit()
  } catch {
    // ignore
  }
  // QueueService/BullMQ keep the event loop alive; cron must exit.
  process.exit(0)
}

main().catch(async (err) => {
  console.error('[RedisMemoryGuard] fatal', err)
  try {
    await getRedisClient().quit()
  } catch {
    // ignore
  }
  process.exit(1)
})
