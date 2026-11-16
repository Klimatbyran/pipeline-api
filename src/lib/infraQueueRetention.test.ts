import assert from "node:assert/strict";
import { describe, it, mock } from "node:test";
import type { Queue } from "bullmq";
import {
  INFRA_QUEUE_RETENTION,
  INFRA_TRIM_BATCH_SIZE,
  trimJobSet,
} from "./infraQueueRetention.js";

function fakeJob(id: string, shouldFail = false) {
  return {
    id,
    remove: mock.fn(async () => {
      if (shouldFail) throw new Error("locked");
    }),
  };
}

describe("infraQueueRetention", () => {
  it("keeps retention caps aligned with garbo infra queues", () => {
    assert.deepEqual(INFRA_QUEUE_RETENTION, {
      pipelineAutoRun: { keepCompleted: 20, keepFailed: 50 },
      chromaSweep: { keepCompleted: 24, keepFailed: 50 },
    });
  });

  it("trimJobSet removes oldest excess completed jobs", async () => {
    const oldest = Array.from({ length: 7 }, (_, i) => fakeJob(String(i + 1)));
    const getJobs = mock.fn(async () => oldest);
    const queue = {
      name: "pipelineAutoRun",
      getJobCounts: mock.fn(async () => ({ completed: 12 })),
      getJobs,
    } as unknown as Queue;

    const { removed } = await trimJobSet(queue, "completed", 5);
    assert.equal(removed, 7);
    assert.deepEqual(getJobs.mock.calls[0].arguments, [
      ["completed"],
      0,
      6,
      true,
    ]);
    for (const job of oldest) {
      assert.equal(job.remove.mock.callCount(), 1);
    }
  });

  it("trimJobSet caps a single pass at INFRA_TRIM_BATCH_SIZE", async () => {
    const oldest = Array.from({ length: INFRA_TRIM_BATCH_SIZE }, (_, i) =>
      fakeJob(String(i)),
    );
    const getJobs = mock.fn(async () => oldest);
    const queue = {
      name: "pipelineAutoRun",
      getJobCounts: mock.fn(async () => ({ completed: 5000 })),
      getJobs,
    } as unknown as Queue;

    const { removed } = await trimJobSet(queue, "completed", 20);
    assert.equal(removed, INFRA_TRIM_BATCH_SIZE);
    assert.deepEqual(getJobs.mock.calls[0].arguments, [
      ["completed"],
      0,
      INFRA_TRIM_BATCH_SIZE - 1,
      true,
    ]);
  });

  it("trimJobSet is a no-op when already at/under keep", async () => {
    const getJobs = mock.fn(async () => []);
    const queue = {
      name: "pipelineAutoRun",
      getJobCounts: mock.fn(async () => ({ completed: 3 })),
      getJobs,
    } as unknown as Queue;

    const { removed } = await trimJobSet(queue, "completed", 5);
    assert.equal(removed, 0);
    assert.equal(getJobs.mock.callCount(), 0);
  });

  it("trimJobSet skips jobs that fail to remove", async () => {
    const locked = fakeJob("locked", true);
    const ok = fakeJob("ok");
    const queue = {
      name: "pipelineAutoRun",
      getJobCounts: mock.fn(async () => ({ failed: 4 })),
      getJobs: mock.fn(async () => [locked, ok]),
    } as unknown as Queue;

    const { removed, failures } = await trimJobSet(queue, "failed", 2);
    assert.equal(removed, 1);
    assert.equal(failures, 1);
    assert.equal(locked.remove.mock.callCount(), 1);
    assert.equal(ok.remove.mock.callCount(), 1);
  });
});
