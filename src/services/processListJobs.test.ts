import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { QUEUE_NAMES } from "../lib/bullmq.js";
import {
  processListDataJobFromBullJob,
  slimProcessListApproval,
  slimProcessListJobData,
  slimProcessListReturnValue,
} from "./QueueService.js";

describe("slimProcessListApproval", () => {
  it("keeps only companyLink displayName for approved links", () => {
    const slim = slimProcessListApproval({
      summary: "link",
      type: "companyLink",
      approved: true,
      data: {
        oldValue: { huge: "x".repeat(10_000) },
        newValue: {
          displayName: "Acme AB",
          wikidataId: "Q1",
          extra: "drop-me",
        },
      },
      metadata: { comment: "n/a" },
    });

    assert.deepEqual(slim, {
      summary: "link",
      type: "companyLink",
      approved: true,
      data: {
        oldValue: undefined,
        newValue: { displayName: "Acme AB" },
      },
      metadata: {},
    });
  });

  it("drops non-companyLink approvals", () => {
    assert.equal(
      slimProcessListApproval({
        summary: "other",
        type: "wikidata",
        approved: false,
        data: { oldValue: 1, newValue: 2 },
        metadata: {},
      }),
      undefined,
    );
  });
});

describe("processListDataJobFromBullJob", () => {
  it("uses list status and omits stacktrace/opts/heavy returnvalue", () => {
    const job = {
      name: "extract",
      id: "job-1",
      queueName: QUEUE_NAMES.EXTRACT_EMISSIONS,
      timestamp: 100,
      processedOn: 110,
      processedBy: "worker-a",
      finishedOn: 120,
      attemptsMade: 1,
      failedReason: undefined,
      stacktrace: ["Error: boom", "at line 1", "x".repeat(50_000)],
      progress: 100,
      delay: 0,
      opts: { attempts: 3, backoff: { type: "exponential" } },
      data: {
        threadId: "thread-1",
        companyName: "Acme",
        companyId: "c1",
        url: "https://example.com/report.pdf",
        autoApprove: true,
        markdown: "# ".repeat(20_000),
        approval: {
          summary: "link",
          type: "companyLink",
          approved: true,
          data: {
            oldValue: null,
            newValue: { displayName: "Acme AB" },
          },
          metadata: {},
        },
      },
      returnvalue: {
        emissions: { scope1: 1 },
        markdown: "huge",
      },
      getState: async () => {
        throw new Error("processList must not call getState");
      },
    } as any;

    const dataJob = processListDataJobFromBullJob(
      job,
      QUEUE_NAMES.EXTRACT_EMISSIONS,
      "completed",
    );

    assert.equal(dataJob.status, "completed");
    assert.equal(dataJob.threadId, "thread-1");
    assert.equal(dataJob.data?.companyName, "Acme");
    assert.equal(dataJob.approval?.data?.newValue?.displayName, "Acme AB");
    assert.equal(dataJob.returnvalue, undefined);
    assert.equal(dataJob.stacktrace, undefined);
    assert.equal(dataJob.opts, undefined);
    assert.equal(dataJob.data?.markdown, undefined);
  });

  it("keeps gated flag only for checkEmissionsPresence", () => {
    const job = {
      name: "gate",
      id: "job-2",
      queueName: QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE,
      timestamp: 1,
      attemptsMade: 0,
      delay: 0,
      data: { threadId: "t2" },
      returnvalue: { gated: true, reason: "no emissions", details: "x".repeat(1000) },
      getState: async () => {
        throw new Error("processList must not call getState");
      },
    } as any;

    const dataJob = processListDataJobFromBullJob(
      job,
      QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE,
      "completed",
    );

    assert.deepEqual(dataJob.returnvalue, { gated: true });
    assert.deepEqual(
      slimProcessListReturnValue(QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE, {
        gated: true,
        reason: "nope",
      }),
      { gated: true },
    );
    assert.deepEqual(
      slimProcessListJobData({
        threadId: "t2",
        markdown: "drop",
        companyName: "X",
      }),
      { threadId: "t2", companyName: "X" },
    );
  });
});
