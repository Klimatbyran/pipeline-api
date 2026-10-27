import { DataJob } from "../schemas/types";
import {
  DEFAULT_FAILED_MAX_AGE_MS,
  DEFAULT_KEEP_SUCCESSFUL_RUN_COUNT,
  PruneSelectionResult,
  RetentionJobRef,
  SelectRunsToPruneOptions,
  buildRunSummaries,
  runMatchesCompanyFilter,
  selectRunsToPrune,
  selectThreadIdsToClear,
} from "../lib/runRetention";
import { QueueService } from "./QueueService";

export type PruneRunsOptions = SelectRunsToPruneOptions & {
  dryRun?: boolean;
  /**
   * When true with includeLive, unlock locked (active) jobs before remove so
   * in-flight work can be deleted. Used only by the Redis memory fail-safe.
   */
  forceRemoveLocked?: boolean;
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

export type ClearRunsOptions = {
  /** Clear these thread ids (terminal only; live runs are skipped). */
  threadIds?: string[];
  /** Clear every non-live terminal run (same as keepCount 0). */
  clearAllTerminal?: boolean;
  dryRun?: boolean;
};

export type ClearRunsResult = PruneRunsResult & {
  notFoundThreadIds: string[];
  skippedLiveThreadIds: string[];
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

  /**
   * Drop BullMQ locks for a job (and its dependency tree) so a locked/active
   * job can be removed. Emergency fail-safe only — workers may error on
   * completion after this.
   */
  private async clearJobLocks(
    queue: Awaited<ReturnType<QueueService["getQueue"]>>,
    jobId: string,
  ): Promise<void> {
    const client = await queue.client;
    const jobKey = queue.toKey(jobId);
    const visited = new Set<string>();
    const stack = [jobKey];

    while (stack.length > 0) {
      const key = stack.pop()!;
      if (visited.has(key)) continue;
      visited.add(key);

      await client.del(`${key}:lock`);
      const deps = await client.smembers(`${key}:dependencies`);
      for (const depKey of deps) {
        if (typeof depKey === "string" && depKey.length > 0) {
          stack.push(depKey);
        }
      }
    }
  }

  private async removeJobRef(
    ref: RetentionJobRef,
    forceRemoveLocked = false,
  ): Promise<boolean> {
    try {
      const queue = await this.queueService.getQueue(ref.queue);
      const job = await queue.getJob(ref.id);
      if (!job) return false;
      try {
        await job.remove({ removeChildren: true });
        return true;
      } catch (error) {
        if (!forceRemoveLocked) throw error;
        await this.clearJobLocks(queue, ref.id);
        await job.remove({ removeChildren: true });
        return true;
      }
    } catch (error) {
      console.error("[RunRetentionService] failed to remove job", {
        queue: ref.queue,
        jobId: ref.id,
        threadId: ref.threadId,
        forceRemoveLocked,
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

  private async loadRunSummaries() {
    const allJobs = await this.queueService.getDataJobs(
      undefined,
      undefined,
      undefined,
      undefined,
      false,
      "processList",
    );
    const retentionJobs = allJobs
      .map(toRetentionJobRef)
      .filter((job): job is RetentionJobRef => job != null);
    return buildRunSummaries(retentionJobs);
  }

  private async applySelection(
    selection: PruneSelectionResult,
    dryRun: boolean,
    forceRemoveLocked = false,
  ): Promise<{
    jobsRemoved: number;
    prunedThreadIds: string[];
    partialPruneThreadIds: string[];
  }> {
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
          if (await this.removeJobRef(ref, forceRemoveLocked)) {
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
          if (await this.removeJobRef(ref, forceRemoveLocked)) {
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

    return {
      jobsRemoved: dryRun ? selection.jobRefsToRemove.length : jobsRemoved,
      prunedThreadIds: dryRun
        ? selection.threadIdsToPrune
        : prunedThreadIds,
      partialPruneThreadIds,
    };
  }

  public async pruneRuns(options: PruneRunsOptions = {}): Promise<PruneRunsResult> {
    const dryRun = options.dryRun ?? false;
    const includeLive = options.includeLive === true;
    const forceRemoveLocked = options.forceRemoveLocked === true;
    // Explicit keepCount (incl. 0) → legacy global keep. Otherwise outcome
    // policy: keep N successful, prune failed older than max age.
    const useLegacyKeepCount = options.keepCount !== undefined;

    const runs = await this.loadRunSummaries();
    const selection = selectRunsToPrune(runs, {
      ...(useLegacyKeepCount
        ? { keepCount: options.keepCount }
        : {
            keepSuccessfulCount:
              options.keepSuccessfulCount ?? DEFAULT_KEEP_SUCCESSFUL_RUN_COUNT,
            failedMaxAgeMs:
              options.failedMaxAgeMs ?? DEFAULT_FAILED_MAX_AGE_MS,
            nowMs: options.nowMs,
          }),
      companyName: options.companyName,
      excludeThreadId: options.excludeThreadId,
      includeLive,
    });

    const companiesProcessed = new Set(
      (options.companyName
        ? runs.filter((run) =>
            runMatchesCompanyFilter(run, options.companyName!),
          )
        : runs
      ).map((run) => run.companyKey),
    ).size;

    const applied = await this.applySelection(
      selection,
      dryRun,
      forceRemoveLocked,
    );

    console.info("[RunRetentionService] pruneRuns", {
      dryRun,
      companyName: options.companyName ?? null,
      excludeThreadId: options.excludeThreadId ?? null,
      keepCount: useLegacyKeepCount ? options.keepCount : null,
      keepSuccessfulCount: useLegacyKeepCount
        ? null
        : (options.keepSuccessfulCount ?? DEFAULT_KEEP_SUCCESSFUL_RUN_COUNT),
      failedMaxAgeMs: useLegacyKeepCount
        ? null
        : (options.failedMaxAgeMs ?? DEFAULT_FAILED_MAX_AGE_MS),
      includeLive,
      forceRemoveLocked,
      companiesProcessed,
      runsPruned: applied.prunedThreadIds.length,
      partialPruneThreadIds: applied.partialPruneThreadIds,
      jobsRemoved: dryRun ? 0 : applied.jobsRemoved,
      skippedLiveRuns: selection.skippedLiveRuns,
    });

    return {
      companiesProcessed,
      runsPruned: applied.prunedThreadIds.length,
      jobsRemoved: dryRun ? 0 : applied.jobsRemoved,
      skippedLiveRuns: selection.skippedLiveRuns,
      protectedThreadIds: selection.protectedThreadIds,
      prunedThreadIds: applied.prunedThreadIds,
      dryRun,
    };
  }

  /**
   * Redis memory fail-safe: remove every pipeline run with a threadId,
   * including live/active jobs. Unlocks active jobs so BullMQ will allow
   * removal. Does not touch jobs without a threadId (e.g. infra ticks).
   */
  public async forceClearAllRuns(options: {
    dryRun?: boolean;
  } = {}): Promise<PruneRunsResult> {
    return this.pruneRuns({
      keepCount: 0,
      includeLive: true,
      forceRemoveLocked: true,
      dryRun: options.dryRun,
    });
  }

  /**
   * Staff clear from Goffer: remove selected terminal runs, or all finished runs.
   * Never removes runs that still have a live/active job.
   */
  public async clearRuns(options: ClearRunsOptions): Promise<ClearRunsResult> {
    const dryRun = options.dryRun ?? false;
    const clearAllTerminal = options.clearAllTerminal === true;
    const threadIds = options.threadIds ?? [];

    if (!clearAllTerminal && threadIds.length === 0) {
      throw new Error("Provide threadIds (non-empty) or clearAllTerminal: true");
    }

    const runs = await this.loadRunSummaries();

    if (clearAllTerminal) {
      const selection = selectRunsToPrune(runs, { keepCount: 0 });
      const applied = await this.applySelection(selection, dryRun);
      const skippedLiveThreadIds = runs
        .filter((run) => run.hasLiveJob)
        .map((run) => run.threadId);
      console.info("[RunRetentionService] clearRuns(allTerminal)", {
        dryRun,
        runsPruned: applied.prunedThreadIds.length,
        jobsRemoved: applied.jobsRemoved,
        skippedLiveRuns: selection.skippedLiveRuns,
      });
      return {
        companiesProcessed: new Set(runs.map((run) => run.companyKey)).size,
        runsPruned: applied.prunedThreadIds.length,
        jobsRemoved: applied.jobsRemoved,
        skippedLiveRuns: selection.skippedLiveRuns,
        protectedThreadIds: selection.protectedThreadIds,
        prunedThreadIds: applied.prunedThreadIds,
        dryRun,
        notFoundThreadIds: [],
        skippedLiveThreadIds,
      };
    }

    const selection = selectThreadIdsToClear(runs, threadIds);
    const applied = await this.applySelection(selection, dryRun);
    const prunedIdSet = new Set(selection.threadIdsToPrune);
    console.info("[RunRetentionService] clearRuns(threadIds)", {
      dryRun,
      requested: threadIds.length,
      runsPruned: applied.prunedThreadIds.length,
      jobsRemoved: applied.jobsRemoved,
      skippedLiveThreadIds: selection.skippedLiveThreadIds,
      notFoundThreadIds: selection.notFoundThreadIds,
    });

    return {
      companiesProcessed: new Set(
        runs
          .filter((run) => prunedIdSet.has(run.threadId))
          .map((run) => run.companyKey),
      ).size,
      runsPruned: applied.prunedThreadIds.length,
      jobsRemoved: applied.jobsRemoved,
      skippedLiveRuns: selection.skippedLiveRuns,
      protectedThreadIds: selection.protectedThreadIds,
      prunedThreadIds: applied.prunedThreadIds,
      dryRun,
      notFoundThreadIds: selection.notFoundThreadIds,
      skippedLiveThreadIds: selection.skippedLiveThreadIds,
    };
  }
}
