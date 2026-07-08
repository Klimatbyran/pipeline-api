import { DataJob } from "../schemas/types";
import {
  DEFAULT_KEEP_RUN_COUNT,
  RetentionJobRef,
  SelectRunsToPruneOptions,
  buildRunSummaries,
  runMatchesCompanyFilter,
  selectRunsToPrune,
} from "../lib/runRetention";
import { QueueService } from "./QueueService";

export type PruneRunsOptions = SelectRunsToPruneOptions & {
  keepCount?: number;
  dryRun?: boolean;
};

export type PruneRunsResult = {
  companiesProcessed: number;
  runsPruned: number;
  jobsRemoved: number;
  skippedLiveRuns: number;
  protectedThreadIds: string[];
  prunedThreadIds: string[];
  dryRun: boolean;
};

function toRetentionJobRef(job: DataJob): RetentionJobRef | null {
  const threadId =
    job.threadId ?? job.data?.threadId ?? job.processId ?? undefined;
  if (!threadId || !job.id) return null;

  return {
    id: job.id,
    queue: job.queue,
    threadId,
    companyName:
      typeof job.data?.companyName === "string"
        ? job.data.companyName
        : undefined,
    companyId:
      typeof job.data?.companyId === "string" ? job.data.companyId : undefined,
    status: job.status,
    finishedOn: job.finishedOn,
    timestamp: job.timestamp,
  };
}

export class RunRetentionService {
  private queueService: QueueService;

  constructor(queueService: QueueService) {
    this.queueService = queueService;
  }

  public static async getRunRetentionService(): Promise<RunRetentionService> {
    const queueService = await QueueService.getQueueService();
    return new RunRetentionService(queueService);
  }

  private async removeJobRef(ref: RetentionJobRef): Promise<boolean> {
    try {
      const queue = await this.queueService.getQueue(ref.queue);
      const job = await queue.getJob(ref.id);
      if (!job) return false;
      await job.remove();
      return true;
    } catch (error) {
      console.error("[RunRetentionService] failed to remove job", {
        queue: ref.queue,
        jobId: ref.id,
        threadId: ref.threadId,
        error,
      });
      return false;
    }
  }

  private async jobRefStillExists(ref: RetentionJobRef): Promise<boolean> {
    try {
      const queue = await this.queueService.getQueue(ref.queue);
      const job = await queue.getJob(ref.id);
      return job != null;
    } catch {
      return true;
    }
  }

  public async pruneRuns(options: PruneRunsOptions = {}): Promise<PruneRunsResult> {
    const keepCount = options.keepCount ?? DEFAULT_KEEP_RUN_COUNT;
    const dryRun = options.dryRun ?? false;

    const allJobs = await this.queueService.getDataJobs();
    const retentionJobs = allJobs
      .map(toRetentionJobRef)
      .filter((job): job is RetentionJobRef => job != null);

    const runs = buildRunSummaries(retentionJobs);
    const selection = selectRunsToPrune(runs, {
      keepCount,
      companyName: options.companyName,
      excludeThreadId: options.excludeThreadId,
    });

    const companiesProcessed = new Set(
      (options.companyName
        ? runs.filter((run) =>
            runMatchesCompanyFilter(run, options.companyName!),
          )
        : runs
      ).map((run) => run.companyKey),
    ).size;

    const refsByThreadId = new Map<string, RetentionJobRef[]>();
    for (const ref of selection.jobRefsToRemove) {
      const list = refsByThreadId.get(ref.threadId) ?? [];
      list.push(ref);
      refsByThreadId.set(ref.threadId, list);
    }

    let jobsRemoved = 0;
    const prunedThreadIds: string[] = [];
    const partialPruneThreadIds: string[] = [];

    if (!dryRun) {
      for (const threadId of selection.threadIdsToPrune) {
        const refs = refsByThreadId.get(threadId) ?? [];
        const pending = [...refs];

        for (const ref of pending) {
          if (await this.removeJobRef(ref)) {
            jobsRemoved += 1;
          }
        }

        const stillPending: RetentionJobRef[] = [];
        for (const ref of pending) {
          if (await this.jobRefStillExists(ref)) {
            stillPending.push(ref);
          }
        }

        for (const ref of stillPending) {
          if (await this.removeJobRef(ref)) {
            jobsRemoved += 1;
          }
        }

        const hasRemaining = await Promise.all(
          refs.map((ref) => this.jobRefStillExists(ref)),
        );
        if (hasRemaining.some(Boolean)) {
          partialPruneThreadIds.push(threadId);
        } else {
          prunedThreadIds.push(threadId);
        }
      }
    }

    console.info("[RunRetentionService] pruneRuns", {
      dryRun,
      companyName: options.companyName ?? null,
      excludeThreadId: options.excludeThreadId ?? null,
      keepCount,
      companiesProcessed,
      runsPruned: dryRun
        ? selection.threadIdsToPrune.length
        : prunedThreadIds.length,
      partialPruneThreadIds,
      jobsRemoved: dryRun ? selection.jobRefsToRemove.length : jobsRemoved,
      skippedLiveRuns: selection.skippedLiveRuns,
    });

    return {
      companiesProcessed,
      runsPruned: dryRun
        ? selection.threadIdsToPrune.length
        : prunedThreadIds.length,
      jobsRemoved: dryRun ? 0 : jobsRemoved,
      skippedLiveRuns: selection.skippedLiveRuns,
      protectedThreadIds: selection.protectedThreadIds,
      prunedThreadIds: dryRun
        ? selection.threadIdsToPrune
        : prunedThreadIds,
      dryRun,
    };
  }
}
