import { DataJob } from "../schemas/types";
import {
  DEFAULT_KEEP_RUN_COUNT,
  RetentionJobRef,
  SelectRunsToPruneOptions,
  buildRunSummaries,
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
      runs
        .filter((run) =>
          options.companyName
            ? run.companyKey.toLowerCase() ===
              options.companyName.trim().toLowerCase()
            : true,
        )
        .map((run) => run.companyKey),
    ).size;

    let jobsRemoved = 0;
    if (!dryRun) {
      for (const ref of selection.jobRefsToRemove) {
        try {
          const queue = await this.queueService.getQueue(ref.queue);
          const job = await queue.getJob(ref.id);
          if (!job) continue;
          await job.remove();
          jobsRemoved += 1;
        } catch (error) {
          console.error("[RunRetentionService] failed to remove job", {
            queue: ref.queue,
            jobId: ref.id,
            threadId: ref.threadId,
            error,
          });
        }
      }
    }

    console.info("[RunRetentionService] pruneRuns", {
      dryRun,
      companyName: options.companyName ?? null,
      excludeThreadId: options.excludeThreadId ?? null,
      keepCount,
      companiesProcessed,
      runsPruned: selection.threadIdsToPrune.length,
      jobsRemoved: dryRun ? selection.jobRefsToRemove.length : jobsRemoved,
      skippedLiveRuns: selection.skippedLiveRuns,
    });

    return {
      companiesProcessed,
      runsPruned: selection.threadIdsToPrune.length,
      jobsRemoved: dryRun ? 0 : jobsRemoved,
      skippedLiveRuns: selection.skippedLiveRuns,
      protectedThreadIds: selection.protectedThreadIds,
      prunedThreadIds: selection.threadIdsToPrune,
      dryRun,
    };
  }
}
