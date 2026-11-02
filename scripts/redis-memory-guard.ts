/**
 * Periodic Redis memory guard for BullMQ.
 *
 * - At >= 70%: prune every non-live (terminal) pipeline run — completed,
 *   failed, skipped — so Redis can recover. Live/active runs are never removed
 *   on this path. Discord webhook fires after with run/job counts.
 * - At >= 85%: emergency fail-safe — clear every pipeline run including live /
 *   active / waiting jobs (unlock + remove). Prefer interrupting in-flight
 *   work over Redis OOM / noeviction write failures. Discord webhook fires after.
 *
 * There is no soft alert-only band below prune — Discord only fires when a
 * prune or force-clear actually runs.
 *
 * Runtime is bounded (see MAX_RUNTIME_MS + CronJob activeDeadlineSeconds) so a
 * hung Redis/BullMQ connection cannot block every future scheduled run forever
 * under concurrencyPolicy: Forbid — same failure mode as prune-runs-by-company.
 */
import { getRedisClient, waitForRedisReady } from '../src/lib/redis-client'
import { getRedisMemorySnapshot } from '../src/lib/redisMemory'
import {
  DEFAULT_REDIS_MEMORY_FORCE_CLEAR_RATIO,
  DEFAULT_REDIS_MEMORY_PRUNE_RATIO,
  evaluateRedisMemoryGuard,
  formatMemoryMiB,
  formatRatioPercent,
} from '../src/lib/redisMemoryGuard'
import { RunRetentionService } from '../src/services/RunRetentionService'
import type { PruneRunsResult } from '../src/services/RunRetentionService'

/**
 * Quiet ticks finish in seconds. Terminal prune and especially the 85%
 * force-clear (unlock + remove every live job) can take minutes under load.
 * Stay under CronJob activeDeadlineSeconds (600) so k8s kills us first only
 * as a last resort; concurrencyPolicy: Forbid still prevents overlap.
 */
const MAX_RUNTIME_MS = 8 * 60 * 1000
const WEBHOOK_TIMEOUT_MS = 8_000

type WebhookPostResult = 'ok' | 'failed' | 'skipped_no_url'

function readRatioEnv(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw == null || raw.trim() === '') return fallback
  const value = Number(raw)
  if (!Number.isFinite(value) || value <= 0 || value > 1) {
    throw new Error(`${name} must be a number in (0, 1], got ${raw}`)
  }
  return value
}

function webhookConfigured(): boolean {
  return Boolean(process.env.REDIS_MEMORY_ALERT_WEBHOOK_URL?.trim())
}

async function postAlertWebhook(message: string): Promise<WebhookPostResult> {
  const webhookUrl = process.env.REDIS_MEMORY_ALERT_WEBHOOK_URL?.trim()
  if (!webhookUrl) {
    console.warn(
      '[RedisMemoryGuard] Discord webhook skipped: REDIS_MEMORY_ALERT_WEBHOOK_URL is unset in this CronJob pod. ' +
        'Expected key REDIS_MEMORY_ALERT_WEBHOOK_URL on Secret `env` in the same namespace as the CronJob ' +
        '(garbo for prod, garbo-stage for stage).',
    )
    return 'skipped_no_url'
  }

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS)
  try {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ content: message }),
      signal: controller.signal,
    })
    if (!response.ok) {
      console.error('[RedisMemoryGuard] Discord webhook failed', {
        status: response.status,
        body: await response.text().catch(() => ''),
      })
      return 'failed'
    }
    console.info('[RedisMemoryGuard] Discord webhook posted OK')
    return 'ok'
  } catch (error) {
    console.error('[RedisMemoryGuard] Discord webhook error', error)
    return 'failed'
  } finally {
    clearTimeout(timer)
  }
}

function formatPruneResultLine(result: PruneRunsResult): string {
  return [
    `runs cleared ${result.runsPruned}`,
    `jobs removed ${result.jobsRemoved}`,
    `live skipped ${result.skippedLiveRuns}`,
  ].join(', ')
}

async function main() {
  const pruneRatio = readRatioEnv(
    'REDIS_MEMORY_PRUNE_RATIO',
    DEFAULT_REDIS_MEMORY_PRUNE_RATIO,
  )
  const forceClearRatio = readRatioEnv(
    'REDIS_MEMORY_FORCE_CLEAR_RATIO',
    DEFAULT_REDIS_MEMORY_FORCE_CLEAR_RATIO,
  )
  const dryRun = process.argv.includes('--dry-run')
  const discordConfigured = webhookConfigured()

  const redis = getRedisClient()
  // enableOfflineQueue:false — must not INFO before the handshake completes.
  await waitForRedisReady(redis)
  const snapshot = await getRedisMemorySnapshot(redis)
  const decision = evaluateRedisMemoryGuard(snapshot, {
    pruneRatio,
    forceClearRatio,
  })

  const ratioLabel =
    decision.ratio == null ? 'n/a' : formatRatioPercent(decision.ratio)

  console.info('[RedisMemoryGuard] snapshot', {
    used: formatMemoryMiB(decision.usedMemoryBytes),
    max: formatMemoryMiB(decision.maxMemoryBytes),
    ratio: ratioLabel,
    pruneRatio,
    forceClearRatio,
    dryRun,
    discordWebhookConfigured: discordConfigured,
    skipReason: decision.skipReason,
  })

  if (decision.skipReason) {
    console.warn(`[RedisMemoryGuard] skipping: ${decision.skipReason}`)
    try {
      await redis.quit()
    } catch {
      // ignore
    }
    process.exit(0)
  }

  if (decision.shouldForceClearAll) {
    const service = await RunRetentionService.getRunRetentionService()
    const result = await service.forceClearAllRuns({ dryRun })
    console.info('[RedisMemoryGuard] emergency force-clear result', result)

    const message = [
      '🚨 Redis memory FAIL-SAFE',
      `${ratioLabel} used (${formatMemoryMiB(decision.usedMemoryBytes)} / ${formatMemoryMiB(decision.maxMemoryBytes)})`,
      `≥ ${formatRatioPercent(forceClearRatio)} — FORCE-cleared ALL BullMQ runs (including live/active)`,
      formatPruneResultLine(result),
      dryRun ? '(dry-run)' : '',
    ]
      .filter(Boolean)
      .join(' · ')
    console.error(`[RedisMemoryGuard] ALERT ${message}`)
    await postAlertWebhook(message)
  } else if (decision.shouldPruneAllTerminal) {
    const service = await RunRetentionService.getRunRetentionService()
    // keepCount 0 = no protected terminal runs; live runs are still skipped.
    const result = await service.pruneRuns({ keepCount: 0, dryRun })
    console.info('[RedisMemoryGuard] emergency prune result', result)

    const message = [
      '🚨 Redis memory prune',
      `${ratioLabel} used (${formatMemoryMiB(decision.usedMemoryBytes)} / ${formatMemoryMiB(decision.maxMemoryBytes)})`,
      `≥ ${formatRatioPercent(pruneRatio)} — pruned all non-active BullMQ runs`,
      formatPruneResultLine(result),
      dryRun ? '(dry-run)' : '',
    ]
      .filter(Boolean)
      .join(' · ')
    console.error(`[RedisMemoryGuard] ALERT ${message}`)
    await postAlertWebhook(message)
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

const timeout = new Promise<never>((_, reject) => {
  setTimeout(
    () =>
      reject(
        new Error(
          `redis-memory-guard: timed out after ${MAX_RUNTIME_MS}ms — likely Redis unreachable (see CronJob activeDeadlineSeconds / prune-runs-by-company)`,
        ),
      ),
    MAX_RUNTIME_MS,
  ).unref()
})

Promise.race([main(), timeout]).catch(async (err) => {
  console.error('[RedisMemoryGuard] fatal', err)
  try {
    await getRedisClient().quit()
  } catch {
    // ignore
  }
  process.exit(1)
})
