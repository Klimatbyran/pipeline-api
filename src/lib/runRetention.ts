export const LIVE_JOB_STATUSES = new Set([
  "active",
  "waiting",
  "delayed",
  "paused",
  "prioritized",
  "waiting-children",
]);

export const TERMINAL_JOB_STATUSES = new Set(["completed", "failed"]);

export const DEFAULT_KEEP_RUN_COUNT = 15;

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
  keepCount?: number;
  /** When set, limits pruning to one company (manual script only). Default is global. */
  companyName?: string;
  excludeThreadId?: string;
};

export function selectRunsToPrune(
  runs: RunSummary[],
  options: SelectRunsToPruneOptions = {},
): PruneSelectionResult {
  const keepCount = options.keepCount ?? DEFAULT_KEEP_RUN_COUNT;
  const threadIdsToPrune = new Set<string>();
  const protectedThreadIds = new Set<string>();
  let skippedLiveRuns = 0;

  if (options.companyName) {
    const filtered = runs.filter((run) =>
      runMatchesCompanyFilter(run, options.companyName!),
    );
    pruneRunGroup(filtered, keepCount, options, {
      threadIdsToPrune,
      protectedThreadIds,
      skippedLiveRunsRef: () => skippedLiveRuns,
      setSkippedLiveRuns: (n) => {
        skippedLiveRuns = n;
      },
    });
  } else {
    pruneRunGroup(runs, keepCount, options, {
      threadIdsToPrune,
      protectedThreadIds,
      skippedLiveRunsRef: () => skippedLiveRuns,
      setSkippedLiveRuns: (n) => {
        skippedLiveRuns = n;
      },
    });
  }

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

function pruneRunGroup(
  runs: RunSummary[],
  keepCount: number,
  options: SelectRunsToPruneOptions,
  state: {
    threadIdsToPrune: Set<string>;
    protectedThreadIds: Set<string>;
    skippedLiveRunsRef: () => number;
    setSkippedLiveRuns: (n: number) => void;
  },
): void {
  const sorted = [...runs].sort((a, b) => b.sortMs - a.sortMs);
  const protectedForGroup = new Set(
    sorted.slice(0, keepCount).map((run) => run.threadId),
  );

  for (const threadId of protectedForGroup) {
    state.protectedThreadIds.add(threadId);
  }

  let skippedLiveRuns = state.skippedLiveRunsRef();
  for (const run of sorted) {
    if (run.hasLiveJob) {
      skippedLiveRuns += 1;
      continue;
    }
    if (run.threadId === options.excludeThreadId) continue;
    if (protectedForGroup.has(run.threadId)) continue;
    state.threadIdsToPrune.add(run.threadId);
  }
  state.setSkippedLiveRuns(skippedLiveRuns);
}
