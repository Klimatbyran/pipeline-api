import { NON_BLOCKING_FAILURE_QUEUES, QUEUE_NAMES } from "../lib/bullmq";
import {
  BaseJob,
  CompanyProcess,
  DataJob,
  Process,
  ProcessStatus,
} from "../schemas/types";
import { QueueService } from "./QueueService";

export class ProcessService {
  private static processService: ProcessService;
  private queueService: QueueService;
  private constructor(queueService: QueueService) {
    this.queueService = queueService;
  }

  // getProcessesGroupedByCompany does an unfiltered fetch across every
  // queue (getDataJobs(undefined, ...) defaults to all ~25 queues) and
  // regroups every job into companies, from scratch, on every call.
  // getPagedCompanyProcesses calls it fresh per page with no sharing
  // between calls -- so paging through "all companies" multiplies an
  // already-expensive full-queue scan by however many pages get
  // requested. As total job volume grows over time, each of those scans
  // gets more expensive too, which is why this got worse over time
  // rather than staying at a fixed cost. Caching the grouped result for
  // a short window means a full page-through only pays for the scan
  // once, not once per page -- cheap, safe staleness for a status view
  // (nothing here needs to be real-time to the second).
  private groupedByCompanyCache = new Map<
    string,
    { expiresAt: number; data: Promise<CompanyProcess[]> }
  >();
  // Short TTL while anything in the result could still change -- covers
  // both the in-flight fetch itself (stampede protection: concurrent
  // callers share this promise instead of each starting their own) and
  // any result where at least one process is still active/waiting.
  private static readonly ACTIVE_CACHE_TTL_MS = 30_000;
  // A batchId whose every process has already reached a terminal state
  // won't change again on its own -- only a retrigger (new jobs added to
  // that batchId) would move it, and that's rare enough that a much
  // longer TTL is safe. Long enough to meaningfully cut repeat-browsing
  // cost on historical/completed batches, short enough that a retrigger
  // doesn't stay invisible for too long.
  private static readonly DONE_CACHE_TTL_MS = 10 * 60_000;
  private static readonly TERMINAL_PROCESS_STATUSES = new Set([
    "completed",
    "failed",
    "skipped_no_emissions",
  ]);

  public static async getProcessService(): Promise<ProcessService> {
    if (!ProcessService.processService) {
      const queueService = await QueueService.getQueueService();
      ProcessService.processService = new ProcessService(queueService);
    }
    return ProcessService.processService;
  }

  public async getProcess(id: string): Promise<Process> {
    const jobs = await this.queueService.getDataJobs(
      undefined,
      undefined,
      id,
      undefined,
      false,
      "processList",
    );
    return this.createProcess(jobs);
  }

  public async getProcesses(batchId?: string): Promise<Process[]> {
    const jobs = await this.queueService.getDataJobs(
      undefined,
      undefined,
      undefined,
      batchId,
      false,
      "processList",
    );
    console.info("[ProcessService] getProcesses: jobs fetched", {
      count: jobs.length,
    });
    const jobProcesses: Record<string, DataJob[]> = {};
    for (const job of jobs) {
      const key = this.processJobsGroupKey(job);
      if (!jobProcesses[key]) {
        jobProcesses[key] = [];
      }
      jobProcesses[key].push(job);
    }
    const processes: Process[] = [];
    for (const jobProcess of Object.values(jobProcesses)) {
      processes.push(this.createProcess(jobProcess));
    }
    console.info("[ProcessService] getProcesses: processes built", {
      count: processes.length,
    });
    return processes;
  }

  public async getProcessesGroupedByCompany(
    batchId?: string,
  ): Promise<CompanyProcess[]> {
    const cacheKey = batchId ?? "";
    const cached = this.groupedByCompanyCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.data;
    }

    const promise = this.computeProcessesGroupedByCompany(batchId);
    // Store the in-flight promise immediately, short TTL, so concurrent
    // callers arriving before this resolves share this one fetch instead
    // of each starting their own -- this is what actually stops a burst
    // of near-simultaneous page requests from each triggering a full scan.
    this.groupedByCompanyCache.set(cacheKey, {
      expiresAt: Date.now() + ProcessService.ACTIVE_CACHE_TTL_MS,
      data: promise,
    });
    promise
      .then((result) => {
        // Once resolved, extend the TTL if everything in it has already
        // reached a terminal state -- only re-set if this is still the
        // cache entry for this key (a newer call may have replaced it).
        if (this.groupedByCompanyCache.get(cacheKey)?.data !== promise) return;
        const ttl = this.allProcessesTerminal(result)
          ? ProcessService.DONE_CACHE_TTL_MS
          : ProcessService.ACTIVE_CACHE_TTL_MS;
        this.groupedByCompanyCache.set(cacheKey, {
          expiresAt: Date.now() + ttl,
          data: promise,
        });
      })
      // A rejected fetch must not poison the cache for the rest of the TTL
      // window -- drop it immediately so the next call retries instead of
      // silently repeating the same failure.
      .catch(() => {
        if (this.groupedByCompanyCache.get(cacheKey)?.data === promise) {
          this.groupedByCompanyCache.delete(cacheKey);
        }
      });
    return promise;
  }

  private allProcessesTerminal(companies: CompanyProcess[]): boolean {
    return companies.every((company) =>
      company.processes.every(
        (process) =>
          process.status != null &&
          ProcessService.TERMINAL_PROCESS_STATUSES.has(process.status),
      ),
    );
  }

  private async computeProcessesGroupedByCompany(
    batchId?: string,
  ): Promise<CompanyProcess[]> {
    const jobs = await this.queueService.getDataJobs(
      undefined,
      undefined,
      undefined,
      batchId,
      false,
      "processList",
    );

    const processJobsByKey: Record<string, DataJob[]> = {};
    for (const job of jobs) {
      const key = this.processJobsGroupKey(job);
      if (!processJobsByKey[key]) {
        processJobsByKey[key] = [];
      }
      processJobsByKey[key].push(job);
    }

    const companyProcesses: Record<string, CompanyProcess> = {};
    const companyJobs: Record<string, DataJob[]> = {};

    for (const processJobs of Object.values(processJobsByKey)) {
      const process = this.createProcess(processJobs);
      const groupCompanyId = this.pickCompanyId(processJobs);
      const key = groupCompanyId
        ? `id:${groupCompanyId}`
        : `name:${this.pickRawCompanyName(processJobs) ?? "unknown"}`;

      if (!companyProcesses[key]) {
        companyProcesses[key] = {
          company: process.company,
          companyId: process.companyId,
          wikidataId: process.wikidataId,
          processes: [],
        };
        companyJobs[key] = [];
      }

      companyProcesses[key].processes.push({
        id: process.id,
        year: process.year,
        status: process.status,
        jobs: process.jobs,
        startedAt: process.startedAt,
        finishedAt: process.finishedAt,
      });

      companyJobs[key].push(...processJobs);

      if (process.wikidataId && process.company !== "unknown") {
        companyProcesses[key].wikidataId = process.wikidataId;
      }
    }

    const grouped = Object.entries(companyProcesses).map(([key, entry]) => {
      const jobsForGroup = companyJobs[key] ?? [];
      const canonicalName = this.pickCanonicalCompanyName(jobsForGroup);
      if (canonicalName) {
        entry.company = canonicalName;
      }
      const companyId = this.pickCompanyId(jobsForGroup);
      if (companyId) {
        entry.companyId = companyId;
      }
      return entry;
    });

    const mergedByCompanyId = new Map<string, CompanyProcess>();
    const unmerged: CompanyProcess[] = [];
    for (const entry of grouped) {
      if (entry.companyId) {
        const existing = mergedByCompanyId.get(entry.companyId);
        if (existing) {
          existing.processes.push(...entry.processes);
          if (!existing.wikidataId && entry.wikidataId) {
            existing.wikidataId = entry.wikidataId;
          }
        } else {
          mergedByCompanyId.set(entry.companyId, entry);
        }
      } else {
        unmerged.push(entry);
      }
    }

    const result = [...mergedByCompanyId.values(), ...unmerged];

    console.info(
      "[ProcessService] getProcessesGroupedByCompany: companies grouped",
      { count: result.length },
    );
    return result;
  }

  public async getPagedCompanyProcesses(
    page: number,
    pageSize: number,
    batchId?: string,
  ): Promise<CompanyProcess[]> {
    const allCompanyProcesses =
      await this.getProcessesGroupedByCompany(batchId);
    const sortedCompanyProcesses =
      this.sortCompanyProcessesByName(allCompanyProcesses);
    return this.getCompanyProcessesPage(sortedCompanyProcesses, page, pageSize);
  }

  /**
   * Returns unique batch IDs present in job data. Scans all jobs (no index);
   * may be slow with very large job counts.
   */
  public async getAvailableBatches(): Promise<string[]> {
    const jobs = await this.queueService.getDataJobs(
      undefined,
      undefined,
      undefined,
      undefined,
      false,
      "processList",
    );
    const batchIds = new Set<string>();
    for (const job of jobs) {
      const bid = (job.data as { batchId?: string })?.batchId;
      if (bid && typeof bid === "string") batchIds.add(bid);
    }
    return Array.from(batchIds).sort();
  }

  /**
   * One pipeline run (process) per upload thread. Company-level grouping uses
   * companyId separately in getProcessesGroupedByCompany — do not key runs by
   * companyId here or multiple PDFs on the same company collapse into one run.
   */
  private processJobsGroupKey(job: DataJob): string {
    if (job.data?.threadId) {
      return job.data.threadId;
    }
    const companyName = job.data?.companyName ?? "unknown";
    return `unknown-${companyName}`;
  }

  private pickCompanyId(jobs: DataJob[]): string | undefined {
    for (const job of jobs) {
      const id = job.data?.companyId;
      if (typeof id === "string" && id.trim()) return id.trim();
    }
    return undefined;
  }

  private pickRawCompanyName(jobs: DataJob[]): string | undefined {
    const sorted = [...jobs].sort((a, b) => b.timestamp - a.timestamp);
    for (const job of sorted) {
      const name = job.data?.companyName;
      if (typeof name === "string" && name.trim()) return name.trim();
    }
    return undefined;
  }

  private pickCanonicalCompanyName(jobs: DataJob[]): string | undefined {
    for (const job of jobs) {
      const approval = job.approval;
      if (approval?.approved && approval.type === "companyLink") {
        const newValue = approval.data?.newValue as
          Record<string, unknown> | undefined;
        const displayName = newValue?.displayName;
        if (typeof displayName === "string" && displayName.trim()) {
          return displayName.trim();
        }
      }
    }

    const sorted = [...jobs].sort((a, b) => b.timestamp - a.timestamp);
    for (const job of sorted) {
      const name = job.data?.companyName;
      if (typeof name === "string" && name.trim()) return name.trim();
    }
    return undefined;
  }

  private createProcess(jobs: DataJob[]): Process {
    let id: string | undefined;
    let wikidataId: string | undefined;
    let company: string | undefined;
    let companyId: string | undefined;
    let year: number | undefined;

    for (const job of jobs) {
      if (job.data?.threadId) {
        id = job.data.threadId;
      }
      if (job.data?.wikidata) {
        wikidataId = job.data.wikidata.node;
      }
      if (job.data?.companyId) {
        companyId = job.data.companyId;
      }
      if (job.data?.companyName) {
        company = job.data.companyName;
      }
      const jobYear = job.data?.reportYear ?? job.data?.documentReportYear;
      if (jobYear !== undefined && jobYear !== null) {
        year = Number(jobYear);
      }
    }

    company = this.pickCanonicalCompanyName(jobs) ?? company;
    companyId = this.pickCompanyId(jobs) ?? companyId;

    const startedAt = Math.min(...jobs.map((job) => job.timestamp));
    const finishedAt = jobs.reduce<number | undefined>((completionTime, job) => {
      if (job.finishedOn === undefined || completionTime === undefined) {
        return undefined;
      }
      return Math.max(completionTime, job.finishedOn);
    }, 0);

    const baseJobs: BaseJob[] = jobs.map((job) => {
      const { data, returnvalue, ...rest } = job;
      return {
        ...rest,
        // Keep returnvalue so Validate can show emissions-gate skips (gated:true).
        returnvalue,
        companyId:
          typeof data?.companyId === "string" ? data.companyId : undefined,
        companyName:
          typeof data?.companyName === "string" ? data.companyName : undefined,
      };
    });

    const processId = id ?? (company ? `unknown-${company}` : "unknown");

    const process: Process = {
      id: processId,
      jobs: baseJobs,
      wikidataId,
      company,
      companyId,
      year,
      startedAt,
      finishedAt,
      status: this.getProcessStatus(jobs),
    };
    return process;
  }

  private getProcessStatus(jobs: DataJob[]): ProcessStatus {
    const hasBlockingFailure = jobs.some(
      (job) =>
        job.status === "failed" &&
        job.queue &&
        !NON_BLOCKING_FAILURE_QUEUES.has(job.queue),
    );
    if (hasBlockingFailure) {
      return "failed";
    }
    // Prefer emissions-gate terminal over any leftover waiting jobs so live
    // Jobbstatus does not stay on "waiting" after a gated skip.
    if (
      jobs.find(
        (job) =>
          job.queue === QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE &&
          job.status === "completed" &&
          (job.returnvalue as { gated?: boolean } | undefined)?.gated === true,
      )
    ) {
      return "skipped_no_emissions";
    }
    if (
      jobs.find((job) =>
        ["waiting", "delayed", "paused", "waiting-children"].includes(
          job.status ?? "",
        ),
      )
    ) {
      return "waiting";
    }
    if (
      jobs.find(
        (job) =>
          job.queue === QUEUE_NAMES.SEND_COMPANY_LINK &&
          job.status === "completed",
      )
    ) {
      return "completed";
    }
    return "active";
  }

  private sortCompanyProcessesByName(
    companyProcesses: CompanyProcess[],
  ): CompanyProcess[] {
    return [...companyProcesses].sort((firstCompany, secondCompany) => {
      const firstName = firstCompany.company ?? "";
      const secondName = secondCompany.company ?? "";
      return firstName.localeCompare(secondName, "en", { sensitivity: "base" });
    });
  }

  private getCompanyProcessesPage(
    companyProcesses: CompanyProcess[],
    page: number,
    pageSize: number,
  ): CompanyProcess[] {
    const safePage = page > 0 ? page : 1;
    const safePageSize = pageSize > 0 ? pageSize : 100;
    const startIndex = (safePage - 1) * safePageSize;
    const endIndex = startIndex + safePageSize;
    return companyProcesses.slice(startIndex, endIndex);
  }
}
