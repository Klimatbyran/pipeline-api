import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  RetentionJobRef,
  buildRunSummaries,
  classifyRunOutcome,
  deriveCompanyKey,
  selectRunsToPrune,
  selectThreadIdsToClear,
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

  it("keepCount 0 prunes completed and failed terminal runs", () => {
    const summaries = buildRunSummaries([
      job({
        id: "ok",
        threadId: "success-run",
        companyName: "Acme AB",
        status: "completed",
        finishedOn: 100,
        timestamp: 100,
      }),
      job({
        id: "bad",
        threadId: "failed-run",
        companyName: "Beta AB",
        status: "failed",
        finishedOn: 200,
        timestamp: 200,
      }),
    ]);

    const selection = selectRunsToPrune(summaries, { keepCount: 0 });

    assert.deepEqual(
      [...selection.threadIdsToPrune].sort(),
      ["failed-run", "success-run"],
    );
    assert.equal(selection.protectedThreadIds.length, 0);
  });

  it("includeLive selects active runs when keepCount is 0", () => {
    const summaries = buildRunSummaries([
      job({
        id: "live-1",
        threadId: "live-run",
        companyName: "Acme AB",
        status: "active",
        timestamp: 100,
      }),
      job({
        id: "done-1",
        threadId: "done-run",
        companyName: "Beta AB",
        status: "completed",
        finishedOn: 200,
        timestamp: 200,
      }),
    ]);

    const selection = selectRunsToPrune(summaries, {
      keepCount: 0,
      includeLive: true,
    });

    assert.deepEqual(
      [...selection.threadIdsToPrune].sort(),
      ["done-run", "live-run"],
    );
    assert.equal(selection.skippedLiveRuns, 0);
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

  it("selectThreadIdsToClear removes only requested terminal runs", () => {
    const summaries = buildRunSummaries([
      job({
        id: "1",
        threadId: "keep-me",
        status: "completed",
        finishedOn: 100,
        timestamp: 100,
      }),
      job({
        id: "2",
        threadId: "clear-me",
        status: "failed",
        finishedOn: 200,
        timestamp: 200,
      }),
      job({
        id: "3",
        threadId: "live-me",
        status: "active",
        timestamp: 300,
      }),
    ]);

    const selection = selectThreadIdsToClear(summaries, [
      "clear-me",
      "live-me",
      "missing",
    ]);

    assert.deepEqual(selection.threadIdsToPrune, ["clear-me"]);
    assert.deepEqual(selection.skippedLiveThreadIds, ["live-me"]);
    assert.deepEqual(selection.notFoundThreadIds, ["missing"]);
    assert.ok(!selection.threadIdsToPrune.includes("keep-me"));
  });

  it("outcome policy keeps 5 newest successful and failed within max age", () => {
    // Use a real epoch so finishedOn stays > 0 (runSortMs ignores non-positive).
    const nowMs = Date.UTC(2026, 9, 27, 12, 0, 0);
    const dayMs = 24 * 60 * 60 * 1000;
    const refs: RetentionJobRef[] = [];

    for (let i = 1; i <= 7; i++) {
      refs.push(
        job({
          id: `ok-${i}`,
          threadId: `success-${i}`,
          companyName: `Co ${i}`,
          status: "completed",
          finishedOn: nowMs - i * 1000,
          timestamp: nowMs - i * 1000,
        }),
      );
    }

    refs.push(
      job({
        id: "fail-recent",
        threadId: "failed-recent",
        companyName: "Fail Co",
        status: "failed",
        finishedOn: nowMs - 60_000,
        timestamp: nowMs - 60_000,
      }),
      job({
        id: "fail-old",
        threadId: "failed-old",
        companyName: "Fail Co",
        status: "failed",
        finishedOn: nowMs - dayMs - 1000,
        timestamp: nowMs - dayMs - 1000,
      }),
      job({
        id: "live",
        threadId: "live-run",
        companyName: "Live Co",
        status: "active",
        timestamp: nowMs,
      }),
    );

    const summaries = buildRunSummaries(refs);
    const selection = selectRunsToPrune(summaries, {
      keepSuccessfulCount: 5,
      failedMaxAgeMs: dayMs,
      nowMs,
    });

    assert.equal(classifyRunOutcome(summaries.find((r) => r.threadId === "failed-recent")!), "failed");
    assert.equal(classifyRunOutcome(summaries.find((r) => r.threadId === "success-1")!), "successful");

    // Newest 5 successes protected (success-1 .. success-5); 6 and 7 pruned.
    assert.ok(selection.protectedThreadIds.includes("success-1"));
    assert.ok(selection.protectedThreadIds.includes("success-5"));
    assert.ok(selection.threadIdsToPrune.includes("success-6"));
    assert.ok(selection.threadIdsToPrune.includes("success-7"));

    assert.ok(selection.protectedThreadIds.includes("failed-recent"));
    assert.ok(selection.threadIdsToPrune.includes("failed-old"));
    assert.ok(!selection.threadIdsToPrune.includes("live-run"));
    assert.equal(selection.skippedLiveRuns, 1);
  });

  it("default outcome policy matches keep 5 successful / 24h failed", () => {
    const nowMs = Date.UTC(2026, 9, 27, 12, 0, 0);
    const summaries = buildRunSummaries([
      job({
        id: "old-ok",
        threadId: "old-success",
        status: "completed",
        finishedOn: nowMs - 10_000,
        timestamp: nowMs - 10_000,
      }),
      ...Array.from({ length: 5 }, (_, i) =>
        job({
          id: `keep-${i}`,
          threadId: `keep-success-${i}`,
          status: "completed",
          finishedOn: nowMs - i * 100,
          timestamp: nowMs - i * 100,
        }),
      ),
    ]);

    const selection = selectRunsToPrune(summaries, { nowMs });
    assert.ok(selection.threadIdsToPrune.includes("old-success"));
    assert.equal(selection.threadIdsToPrune.length, 1);
  });
});
