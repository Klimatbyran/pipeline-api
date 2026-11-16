import { Queue } from "bullmq";
import redis from "../config/redis";

/**
 * Garbo infra queues have no threadId, so RunRetentionService never touches
 * them. Cap completed/failed jobs here so they cannot fill Redis under
 * maxmemory + noeviction when per-job removeOnComplete fails to fire.
 */
export const INFRA_QUEUE_RETENTION = {
  pipelineAutoRun: { keepCompleted: 20, keepFailed: 50 },
  chromaSweep: { keepCompleted: 24, keepFailed: 50 },
} as const;

export type InfraQueueTrimResult = {
  queue: string;
  completedRemoved: number;
  failedRemoved: number;
  completedRemaining: number;
  failedRemaining: number;
};

async function trimJobSet(
  queue: Queue,
  status: "completed" | "failed",
  keep: number,
): Promise<number> {
  const counts = await queue.getJobCounts(status);
  const total = counts[status] ?? 0;
  const excess = total - keep;
  if (excess <= 0) return 0;

  // asc:true → oldest first for scored finished sets.
  const oldest = await queue.getJobs([status], 0, excess - 1, true);
  let removed = 0;
  for (const job of oldest) {
    try {
      await job.remove();
      removed += 1;
    } catch (error) {
      console.error("[infraQueueRetention] failed to remove job", {
        queue: queue.name,
        status,
        jobId: job.id,
        error,
      });
    }
  }
  return removed;
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
        completedRemoved = await trimJobSet(
          queue,
          "completed",
          retention.keepCompleted,
        );
        failedRemoved = await trimJobSet(queue, "failed", retention.keepFailed);
      } else {
        completedRemoved = completedExcess;
        failedRemoved = failedExcess;
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
    } finally {
      await queue.close();
    }
  }

  console.info("[infraQueueRetention] trimInfraQueues", { dryRun, results });
  return results;
}
