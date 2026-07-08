import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  RetentionJobRef,
  buildRunSummaries,
  deriveCompanyKey,
  selectRunsToPrune,
} from "./runRetention.js";

function job(
  partial: Partial<RetentionJobRef> & Pick<RetentionJobRef, "id" | "threadId">,
): RetentionJobRef {
  return {
    queue: "parsePdf",
    timestamp: 1,
    ...partial,
  };
}

describe("runRetention", () => {
  it("derives company key from any job in the run", () => {
    const key = deriveCompanyKey(
      [
        job({ id: "1", threadId: "t1", timestamp: 1 }),
        job({
          id: "2",
          threadId: "t1",
          companyName: "Acme AB",
          timestamp: 2,
        }),
      ],
      "t1",
    );
    assert.equal(key, "acme ab");
  });

  it("keeps 15 newest runs globally across companies", () => {
    const runs: RetentionJobRef[] = [];
    for (let i = 1; i <= 18; i++) {
      runs.push(
        job({
          id: `job-${i}`,
          threadId: `thread-${i}`,
          companyName: `Company ${i}`,
          status: "completed",
          finishedOn: i * 1000,
          timestamp: i * 1000,
        }),
      );
    }

    const summaries = buildRunSummaries(runs);
    const selection = selectRunsToPrune(summaries, { keepCount: 15 });

    assert.equal(selection.protectedThreadIds.length, 15);
    assert.equal(selection.threadIdsToPrune.length, 3);
    assert.ok(selection.threadIdsToPrune.includes("thread-1"));
    assert.ok(selection.threadIdsToPrune.includes("thread-2"));
    assert.ok(selection.threadIdsToPrune.includes("thread-3"));
    assert.ok(!selection.threadIdsToPrune.includes("thread-18"));
  });

  it("scoped prune by company name only affects that company", () => {
    const summaries = buildRunSummaries([
      job({
        id: "1",
        threadId: "run-1",
        companyName: "Acme AB",
        status: "completed",
        finishedOn: 100,
        timestamp: 100,
      }),
      job({
        id: "2",
        threadId: "run-2",
        companyName: "Other Co",
        status: "completed",
        finishedOn: 200,
        timestamp: 200,
      }),
    ]);

    const selection = selectRunsToPrune(summaries, {
      companyName: "Acme AB",
      keepCount: 0,
    });

    assert.deepEqual(selection.threadIdsToPrune, ["run-1"]);
  });

  it("never prunes runs with any live job", () => {
    const summaries = buildRunSummaries([
      job({
        id: "live-1",
        threadId: "live-run",
        companyName: "Acme AB",
        status: "active",
        timestamp: 100,
      }),
      job({
        id: "live-2",
        threadId: "live-run",
        companyName: "Acme AB",
        status: "completed",
        finishedOn: 200,
        timestamp: 200,
      }),
      job({
        id: "old-1",
        threadId: "old-run",
        companyName: "Beta AB",
        status: "completed",
        finishedOn: 50,
        timestamp: 50,
      }),
    ]);

    const selection = selectRunsToPrune(summaries, { keepCount: 1 });

    assert.equal(selection.skippedLiveRuns, 1);
    assert.ok(!selection.threadIdsToPrune.includes("live-run"));
    assert.ok(selection.threadIdsToPrune.includes("old-run"));
  });

  it("excludes the thread that triggered the prune hook", () => {
    const summaries = buildRunSummaries([
      job({
        id: "new",
        threadId: "new-run",
        companyName: "Acme AB",
        status: "completed",
        finishedOn: 1000,
        timestamp: 1000,
      }),
      job({
        id: "old",
        threadId: "old-run",
        companyName: "Beta AB",
        status: "completed",
        finishedOn: 2000,
        timestamp: 2000,
      }),
    ]);

    const selection = selectRunsToPrune(summaries, {
      keepCount: 1,
      excludeThreadId: "new-run",
    });

    assert.ok(!selection.threadIdsToPrune.includes("new-run"));
    assert.ok(!selection.threadIdsToPrune.includes("old-run"));
  });
});
