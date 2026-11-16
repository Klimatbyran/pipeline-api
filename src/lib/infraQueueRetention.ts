import { Queue } from "bullmq";
import redis from "../config/redis";

/**
 * Garbo infra queues have no threadId, so RunRetentionService never touches
 * them. Cap completed/failed jobs here so they cannot fill Redis under
 * maxmemory + noeviction when per-job removeOnComplete fails to fire.
 *
 * Keep counts in sync with garbo `src/lib/infraQueueRetention.ts`.
 */
export const INFRA_QUEUE_RETENTION = {
  pipelineAutoRun: { keepCompleted: 20, keepFailed: 50 },
  chromaSweep: { keepCompleted: 24, keepFailed: 50 },
} as const;

/** Cap removals per status per cron pass — huge tick backlogs must not OOM. */
export const INFRA_TRIM_BATCH_SIZE = 500;

export type InfraQueueTrimResult = {
  queue: string;
  completedRemoved: number;
  failedRemoved: number;
  completedRemaining: number;
  failedRemaining: number;
  error?: string;
};

/** @internal exported for unit tests */
export async function trimJobSet(
  queue: Queue,
  status: "completed" | "failed",
  keep: number,
  batchSize = INFRA_TRIM_BATCH_SIZE,
): Promise<{ removed: number; failures: number }> {
  const counts = await queue.getJobCounts(status);
  const total = counts[status] ?? 0;
  const excess = total - keep;
  if (excess <= 0) return { removed: 0, failures: 0 };

  const toRemove = Math.min(excess, batchSize);
  // asc:true → oldest first for scored finished sets.
  const oldest = await queue.getJobs([status], 0, toRemove - 1, true);
  let removed = 0;
  let failures = 0;
  for (const job of oldest) {
    try {
      await job.remove();
      removed += 1;
    } catch {
      failures += 1;
    }
  }
  if (failures > 0) {
    console.warn("[infraQueueRetention] some jobs could not be removed", {
      queue: queue.name,
      status,
      failures,
      removed,
    });
  }
  return { removed, failures };
}

export async function trimInfraQueues(
  options: { dryRun?: boolean } = {},
): Promise<InfraQueueTrimResult[]> {
  const dryRun = options.dryRun ?? false;
  const results: InfraQueueTrimResult[] = [];

  for (const [queueName, retention] of Object.entries(INFRA_QUEUE_RETENTION)) {
    const queue = new Queue(queueName, { connection: redis });
    try {
      await queue.waitUntilReady();
      const before = await queue.getJobCounts("completed", "failed");
      const completedExcess = Math.max(
        0,
        (before.completed ?? 0) - retention.keepCompleted,
      );
      const failedExcess = Math.max(
        0,
        (before.failed ?? 0) - retention.keepFailed,
      );

      let completedRemoved = 0;
      let failedRemoved = 0;
      if (!dryRun) {
        completedRemoved = (
          await trimJobSet(queue, "completed", retention.keepCompleted)
        ).removed;
        failedRemoved = (
          await trimJobSet(queue, "failed", retention.keepFailed)
        ).removed;
      } else {
        completedRemoved = Math.min(completedExcess, INFRA_TRIM_BATCH_SIZE);
        failedRemoved = Math.min(failedExcess, INFRA_TRIM_BATCH_SIZE);
      }

      const after = dryRun
        ? {
            completed: Math.max(
              0,
              (before.completed ?? 0) - completedRemoved,
            ),
            failed: Math.max(0, (before.failed ?? 0) - failedRemoved),
          }
        : await queue.getJobCounts("completed", "failed");

      results.push({
        queue: queueName,
        completedRemoved,
        failedRemoved,
        completedRemaining: after.completed ?? 0,
        failedRemaining: after.failed ?? 0,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error("[infraQueueRetention] queue trim failed", {
        queue: queueName,
        error,
      });
      results.push({
        queue: queueName,
        completedRemoved: 0,
        failedRemoved: 0,
        completedRemaining: -1,
        failedRemaining: -1,
        error: message,
      });
    } finally {
      await queue.close().catch(() => undefined);
    }
  }

  console.info("[infraQueueRetention] trimInfraQueues", { dryRun, results });
  return results;
}
