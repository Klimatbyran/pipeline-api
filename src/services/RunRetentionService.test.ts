import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DataJob } from "../schemas/types.js";
import { QueueService } from "./QueueService.js";
import { RunRetentionService } from "./RunRetentionService.js";

type FakeJob = {
  remove: (opts?: { removeChildren?: boolean }) => Promise<void>;
};

function dataJob(
  partial: Partial<DataJob> &
    Pick<DataJob, "id" | "queue" | "timestamp" | "status"> & {
      threadId: string;
      companyName?: string;
    },
): DataJob {
  const { companyName, threadId, ...rest } = partial;
  return {
    ...rest,
    threadId,
    data: {
      threadId,
      ...(companyName ? { companyName } : {}),
    },
  } as DataJob;
}

function createService(
  jobs: DataJob[],
  options: { lockedJobIds?: Set<string> } = {},
) {
  const removed = new Set<string>();
  const locks = new Set(options.lockedJobIds ?? []);
  const deletedLockKeys: string[] = [];

  const queueService = {
    getDataJobs: async () =>
      jobs.filter((job) => !removed.has(`${job.queue}:${job.id}`)),
    getQueue: async (queueName: string) => ({
      toKey: (id: string) => `bull:${queueName}:${id}`,
      client: Promise.resolve({
        del: async (key: string) => {
          deletedLockKeys.push(key);
          const match = key.match(/^bull:([^:]+):(.+):lock$/);
          if (match) {
            locks.delete(`${match[1]}:${match[2]}`);
          }
          return 1;
        },
        smembers: async () => [] as string[],
      }),
      getJob: async (id: string): Promise<FakeJob | null> => {
        const key = `${queueName}:${id}`;
        if (removed.has(key)) return null;
        const exists = jobs.some((job) => job.queue === queueName && job.id === id);
        if (!exists) return null;
        return {
          remove: async () => {
            if (locks.has(key)) {
              throw new Error(
                `Job ${id} could not be removed because it is locked by another worker`,
              );
            }
            removed.add(key);
          },
        };
      },
    }),
  } as unknown as QueueService;

  return {
    service: new RunRetentionService(queueService),
    removed,
    deletedLockKeys,
  };
}

describe("RunRetentionService.clearRuns", () => {
  it("clears requested terminal thread ids and skips live / missing", async () => {
    const { service, removed } = createService([
      dataJob({
        id: "1",
        queue: "parsePdf",
        threadId: "clear-me",
        companyName: "Acme AB",
        status: "completed",
        finishedOn: 100,
        timestamp: 100,
      }),
      dataJob({
        id: "2",
        queue: "parsePdf",
        threadId: "live-me",
        companyName: "Beta AB",
        status: "active",
        timestamp: 200,
      }),
      dataJob({
        id: "3",
        queue: "parsePdf",
        threadId: "keep-me",
        companyName: "Gamma AB",
        status: "failed",
        finishedOn: 300,
        timestamp: 300,
      }),
    ]);

    const result = await service.clearRuns({
      threadIds: ["clear-me", "live-me", "missing", "  clear-me  "],
    });

    assert.deepEqual(result.prunedThreadIds, ["clear-me"]);
    assert.deepEqual(result.skippedLiveThreadIds, ["live-me"]);
    assert.deepEqual(result.notFoundThreadIds, ["missing"]);
    assert.equal(result.runsPruned, 1);
    assert.equal(result.jobsRemoved, 1);
    assert.equal(result.companiesProcessed, 1);
    assert.ok(removed.has("parsePdf:1"));
    assert.ok(!removed.has("parsePdf:2"));
    assert.ok(!removed.has("parsePdf:3"));
  });

  it("clearAllTerminal removes every non-live run and reports skipped live ids", async () => {
    const { service, removed } = createService([
      dataJob({
        id: "ok",
        queue: "parsePdf",
        threadId: "success-run",
        companyName: "Acme AB",
        status: "completed",
        finishedOn: 100,
        timestamp: 100,
      }),
      dataJob({
        id: "bad",
        queue: "extractEmissions",
        threadId: "failed-run",
        companyName: "Beta AB",
        status: "failed",
        finishedOn: 200,
        timestamp: 200,
      }),
      dataJob({
        id: "live",
        queue: "parsePdf",
        threadId: "live-run",
        companyName: "Gamma AB",
        status: "waiting",
        timestamp: 300,
      }),
    ]);

    const result = await service.clearRuns({ clearAllTerminal: true });

    assert.deepEqual(
      [...result.prunedThreadIds].sort(),
      ["failed-run", "success-run"],
    );
    assert.deepEqual(result.skippedLiveThreadIds, ["live-run"]);
    assert.equal(result.skippedLiveRuns, 1);
    assert.equal(result.runsPruned, 2);
    assert.equal(result.jobsRemoved, 2);
    assert.ok(removed.has("parsePdf:ok"));
    assert.ok(removed.has("extractEmissions:bad"));
    assert.ok(!removed.has("parsePdf:live"));
  });

  it("dryRun projects removals without deleting jobs", async () => {
    const { service, removed } = createService([
      dataJob({
        id: "1",
        queue: "parsePdf",
        threadId: "dry-run",
        companyName: "Acme AB",
        status: "completed",
        finishedOn: 100,
        timestamp: 100,
      }),
    ]);

    const result = await service.clearRuns({
      threadIds: ["dry-run"],
      dryRun: true,
    });

    assert.deepEqual(result.prunedThreadIds, ["dry-run"]);
    assert.equal(result.jobsRemoved, 1);
    assert.equal(result.dryRun, true);
    assert.equal(removed.size, 0);
  });
});

describe("RunRetentionService.forceClearAllRuns", () => {
  it("removes live runs including locked/active jobs", async () => {
    const { service, removed, deletedLockKeys } = createService(
      [
        dataJob({
          id: "ok",
          queue: "parsePdf",
          threadId: "success-run",
          companyName: "Acme AB",
          status: "completed",
          finishedOn: 100,
          timestamp: 100,
        }),
        dataJob({
          id: "live",
          queue: "extractEmissions",
          threadId: "live-run",
          companyName: "Beta AB",
          status: "active",
          timestamp: 200,
        }),
      ],
      { lockedJobIds: new Set(["extractEmissions:live"]) },
    );

    const result = await service.forceClearAllRuns();

    assert.deepEqual(
      [...result.prunedThreadIds].sort(),
      ["live-run", "success-run"],
    );
    assert.equal(result.skippedLiveRuns, 0);
    assert.equal(result.runsPruned, 2);
    assert.equal(result.jobsRemoved, 2);
    assert.ok(removed.has("parsePdf:ok"));
    assert.ok(removed.has("extractEmissions:live"));
    assert.ok(
      deletedLockKeys.includes("bull:extractEmissions:live:lock"),
      `expected lock clear, got ${JSON.stringify(deletedLockKeys)}`,
    );
  });

  it("dryRun projects live removals without deleting", async () => {
    const { service, removed } = createService([
      dataJob({
        id: "live",
        queue: "parsePdf",
        threadId: "live-run",
        companyName: "Acme AB",
        status: "waiting",
        timestamp: 100,
      }),
    ]);

    const result = await service.forceClearAllRuns({ dryRun: true });

    assert.deepEqual(result.prunedThreadIds, ["live-run"]);
    // pruneRuns dry-run reports 0 removals (projection is in prunedThreadIds).
    assert.equal(result.jobsRemoved, 0);
    assert.equal(result.dryRun, true);
    assert.equal(removed.size, 0);
  });
});
