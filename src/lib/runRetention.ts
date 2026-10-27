import { NON_BLOCKING_FAILURE_QUEUES } from "./bullmq";

export const LIVE_JOB_STATUSES = new Set([
  "active",
  "waiting",
  "delayed",
  "paused",
  "prioritized",
  "waiting-children",
]);

/** Job-level terminals. Emissions-gate skips complete as `completed` (with
 * gated:true returnvalue) — they count as successful for retention. */
export const TERMINAL_JOB_STATUSES = new Set(["completed", "failed"]);

/** @deprecated Prefer keepSuccessfulCount + failedMaxAgeMs. Kept for
 * emergency keepCount:0 / explicit legacy callers. */
export const DEFAULT_KEEP_RUN_COUNT = 15;

/** Routine cron: newest successful / skipped runs to keep globally. */
export const DEFAULT_KEEP_SUCCESSFUL_RUN_COUNT = 5;

/** Routine cron: failed runs older than this are pruned. */
export const DEFAULT_FAILED_MAX_AGE_MS = 24 * 60 * 60 * 1000;

export type RetentionJobRef = {
  id: string;
  queue: string;
  threadId: string;
  companyName?: string;
  companyId?: string;
  status?: string;
  finishedOn?: number;
  timestamp: number;
};

export type RunSummary = {
  threadId: string;
  companyKey: string;
  sortMs: number;
  hasLiveJob: boolean;
  jobRefs: RetentionJobRef[];
};

export type RunOutcome = "live" | "failed" | "successful";

export type PruneSelectionResult = {
  threadIdsToPrune: string[];
  protectedThreadIds: string[];
  skippedLiveRuns: number;
  jobRefsToRemove: RetentionJobRef[];
};

export function normalizeCompanyName(name: string | undefined): string | null {
  const trimmed = name?.trim();
  return trimmed ? trimmed : null;
}

/** Stable per-company bucket for retention grouping. */
export function deriveCompanyKey(
  jobs: RetentionJobRef[],
  threadId: string,
): string {
  for (const job of jobs) {
    const name = normalizeCompanyName(job.companyName);
    if (name) return name.toLowerCase();
  }
  for (const job of jobs) {
    const id = job.companyId?.trim();
    if (id) return `companyId:${id}`;
  }
  return `unknown:${threadId}`;
}

export function isLiveJobStatus(status: string | undefined): boolean {
  return status != null && LIVE_JOB_STATUSES.has(status);
}

export function runSortMs(jobs: RetentionJobRef[]): number {
  let max = 0;
  for (const job of jobs) {
    const anchor =
      typeof job.finishedOn === "number" && job.finishedOn > 0
        ? job.finishedOn
        : job.timestamp;
    if (anchor > max) max = anchor;
  }
  return max;
}

export function buildRunSummaries(jobs: RetentionJobRef[]): RunSummary[] {
  const byThread = new Map<string, RetentionJobRef[]>();
  for (const job of jobs) {
    const list = byThread.get(job.threadId) ?? [];
    list.push(job);
    byThread.set(job.threadId, list);
  }

  const runs: RunSummary[] = [];
  for (const [threadId, threadJobs] of byThread) {
    const hasLiveJob = threadJobs.some((job) => isLiveJobStatus(job.status));
    runs.push({
      threadId,
      companyKey: deriveCompanyKey(threadJobs, threadId),
      sortMs: runSortMs(threadJobs),
      hasLiveJob,
      jobRefs: threadJobs,
    });
  }
  return runs;
}

/**
 * Classify a run for outcome-based retention.
 * Failed = any blocking queue failure. Successful = terminal with no
 * blocking failure (includes emissions-gate skips and completed runs).
 */
export function classifyRunOutcome(run: RunSummary): RunOutcome {
  if (run.hasLiveJob) return "live";
  const hasBlockingFailure = run.jobRefs.some(
    (job) =>
      job.status === "failed" && !NON_BLOCKING_FAILURE_QUEUES.has(job.queue),
  );
  return hasBlockingFailure ? "failed" : "successful";
}

export function companyKeyMatches(
  runCompanyKey: string,
  filterCompanyName: string,
): boolean {
  const normalized = normalizeCompanyName(filterCompanyName);
  if (!normalized) return false;
  return runCompanyKey.toLowerCase() === normalized.toLowerCase();
}

export function runMatchesCompanyFilter(
  run: RunSummary,
  filterCompanyName: string,
): boolean {
  if (companyKeyMatches(run.companyKey, filterCompanyName)) return true;

  const normalized = normalizeCompanyName(filterCompanyName)?.toLowerCase();
  if (!normalized) return false;

  return run.jobRefs.some(
    (job) =>
      normalizeCompanyName(job.companyName)?.toLowerCase() === normalized,
  );
}

export type SelectRunsToPruneOptions = {
  /**
   * Legacy global keep of newest terminal runs (any outcome). When set,
   * outcome-based keepSuccessfulCount / failedMaxAgeMs are ignored.
   * keepCount 0 = prune every non-live terminal run.
   */
  keepCount?: number;
  /** Newest successful/skipped runs to keep (routine cron). */
  keepSuccessfulCount?: number;
  /** Failed runs older than this (ms) are pruned (routine cron). */
  failedMaxAgeMs?: number;
  /** Clock for failedMaxAgeMs (tests). Defaults to Date.now(). */
  nowMs?: number;
  /** When set, limits pruning to one company (manual script only). Default is global. */
  companyName?: string;
  excludeThreadId?: string;
  /**
   * Emergency path only: also select runs that still have live/active jobs.
   * Default false — normal prune/clear never touches in-flight work.
   */
  includeLive?: boolean;
};

export type SelectThreadIdsToClearResult = PruneSelectionResult & {
  /** Requested ids with no matching Redis run. */
  notFoundThreadIds: string[];
  /** Requested ids still containing a live/active job (not cleared). */
  skippedLiveThreadIds: string[];
};

/**
 * Staff/manual clear: remove only the listed terminal runs.
 * Live runs in the request are skipped; unknown ids are reported as not found.
 */
export function selectThreadIdsToClear(
  runs: RunSummary[],
  threadIds: string[],
): SelectThreadIdsToClearResult {
  const wanted = new Set(
    threadIds.map((id) => id.trim()).filter((id) => id.length > 0),
  );
  const byThreadId = new Map(runs.map((run) => [run.threadId, run]));

  const threadIdsToPrune: string[] = [];
  const skippedLiveThreadIds: string[] = [];
  const notFoundThreadIds: string[] = [];
  const jobRefsToRemove: RetentionJobRef[] = [];

  for (const threadId of wanted) {
    const run = byThreadId.get(threadId);
    if (!run) {
      notFoundThreadIds.push(threadId);
      continue;
    }
    if (run.hasLiveJob) {
      skippedLiveThreadIds.push(threadId);
      continue;
    }
    threadIdsToPrune.push(threadId);
    jobRefsToRemove.push(...run.jobRefs);
  }

  return {
    threadIdsToPrune,
    protectedThreadIds: [],
    skippedLiveRuns: skippedLiveThreadIds.length,
    jobRefsToRemove,
    notFoundThreadIds,
    skippedLiveThreadIds,
  };
}

export function selectRunsToPrune(
  runs: RunSummary[],
  options: SelectRunsToPruneOptions = {},
): PruneSelectionResult {
  const scoped =
    options.companyName != null
      ? runs.filter((run) => runMatchesCompanyFilter(run, options.companyName!))
      : runs;

  if (options.keepCount !== undefined) {
    return selectRunsToPruneLegacy(scoped, options);
  }

  return selectRunsToPruneByOutcome(scoped, options);
}

function selectionFromThreadIds(
  runs: RunSummary[],
  threadIdsToPrune: Set<string>,
  protectedThreadIds: Set<string>,
  skippedLiveRuns: number,
): PruneSelectionResult {
  const jobRefsToRemove: RetentionJobRef[] = [];
  for (const run of runs) {
    if (!threadIdsToPrune.has(run.threadId)) continue;
    jobRefsToRemove.push(...run.jobRefs);
  }

  return {
    threadIdsToPrune: [...threadIdsToPrune],
    protectedThreadIds: [...protectedThreadIds],
    skippedLiveRuns,
    jobRefsToRemove,
  };
}

/** Legacy: keep N newest terminal runs of any outcome. */
function selectRunsToPruneLegacy(
  runs: RunSummary[],
  options: SelectRunsToPruneOptions,
): PruneSelectionResult {
  const keepCount = options.keepCount ?? DEFAULT_KEEP_RUN_COUNT;
  const threadIdsToPrune = new Set<string>();
  const protectedThreadIds = new Set<string>();
  let skippedLiveRuns = 0;

  const sorted = [...runs].sort((a, b) => b.sortMs - a.sortMs);
  const protectedForGroup = new Set(
    sorted.slice(0, keepCount).map((run) => run.threadId),
  );

  for (const threadId of protectedForGroup) {
    protectedThreadIds.add(threadId);
  }

  for (const run of sorted) {
    if (run.hasLiveJob && !options.includeLive) {
      skippedLiveRuns += 1;
      continue;
    }
    if (run.threadId === options.excludeThreadId) continue;
    if (protectedForGroup.has(run.threadId)) continue;
    threadIdsToPrune.add(run.threadId);
  }

  return selectionFromThreadIds(
    runs,
    threadIdsToPrune,
    protectedThreadIds,
    skippedLiveRuns,
  );
}

/**
 * Routine policy: keep newest N successful/skipped runs; prune failed runs
 * older than failedMaxAgeMs; never touch live runs (unless includeLive).
 */
function selectRunsToPruneByOutcome(
  runs: RunSummary[],
  options: SelectRunsToPruneOptions,
): PruneSelectionResult {
  const keepSuccessfulCount =
    options.keepSuccessfulCount ?? DEFAULT_KEEP_SUCCESSFUL_RUN_COUNT;
  const failedMaxAgeMs =
    options.failedMaxAgeMs ?? DEFAULT_FAILED_MAX_AGE_MS;
  const nowMs = options.nowMs ?? Date.now();
  const failedCutoffMs = nowMs - failedMaxAgeMs;

  const threadIdsToPrune = new Set<string>();
  const protectedThreadIds = new Set<string>();
  let skippedLiveRuns = 0;

  const successful: RunSummary[] = [];
  const failed: RunSummary[] = [];

  for (const run of runs) {
    const outcome = classifyRunOutcome(run);
    if (outcome === "live") {
      // Live runs are only cleared via legacy keepCount + includeLive
      // (memory-guard force-clear). Routine cron never touches them.
      skippedLiveRuns += 1;
      continue;
    }
    if (outcome === "failed") {
      failed.push(run);
    } else {
      successful.push(run);
    }
  }

  successful.sort((a, b) => b.sortMs - a.sortMs);
  for (const run of successful.slice(0, keepSuccessfulCount)) {
    if (run.threadId === options.excludeThreadId) continue;
    protectedThreadIds.add(run.threadId);
  }

  for (const run of successful) {
    if (run.threadId === options.excludeThreadId) continue;
    if (protectedThreadIds.has(run.threadId)) continue;
    threadIdsToPrune.add(run.threadId);
  }

  for (const run of failed) {
    if (run.threadId === options.excludeThreadId) {
      protectedThreadIds.add(run.threadId);
      continue;
    }
    if (run.sortMs >= failedCutoffMs) {
      protectedThreadIds.add(run.threadId);
      continue;
    }
    threadIdsToPrune.add(run.threadId);
  }

  return selectionFromThreadIds(
    runs,
    threadIdsToPrune,
    protectedThreadIds,
    skippedLiveRuns,
  );
}
