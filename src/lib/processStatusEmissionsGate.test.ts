import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { QUEUE_NAMES } from "./bullmq.js";

/**
 * Mirrors ProcessService.getProcessStatus terminal rules for the emissions gate.
 * Kept as a pure helper test so we don't need Redis/BullMQ fixtures.
 */
function getProcessStatus(
  jobs: Array<{
    status?: string;
    queue?: string;
    returnvalue?: { gated?: boolean };
  }>,
): string {
  const hasBlockingFailure = jobs.some((job) => job.status === "failed");
  if (hasBlockingFailure) return "failed";
  // Prefer emissions-gate terminal over leftover waiting jobs.
  if (
    jobs.find(
      (job) =>
        job.queue === QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE &&
        job.status === "completed" &&
        job.returnvalue?.gated === true,
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

describe("process status emissions gate", () => {
  it("returns skipped_no_emissions when the gate completed as gated", () => {
    assert.equal(
      getProcessStatus([
        {
          queue: QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE,
          status: "completed",
          returnvalue: { gated: true },
        },
      ]),
      "skipped_no_emissions",
    );
  });

  it("prefers skipped_no_emissions over leftover waiting jobs", () => {
    assert.equal(
      getProcessStatus([
        {
          queue: QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE,
          status: "completed",
          returnvalue: { gated: true },
        },
        { queue: QUEUE_NAMES.PRECHECK, status: "waiting" },
      ]),
      "skipped_no_emissions",
    );
  });

  it("stays active when the gate passed (precheck still running)", () => {
    assert.equal(
      getProcessStatus([
        {
          queue: QUEUE_NAMES.CHECK_EMISSIONS_PRESENCE,
          status: "completed",
          returnvalue: { gated: false },
        },
        { queue: QUEUE_NAMES.PRECHECK, status: "active" },
      ]),
      "active",
    );
  });
});
