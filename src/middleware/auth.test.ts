import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { normalizeRequestPath } from "./auth";

describe("normalizeRequestPath", () => {
  it("strips query string before matching", () => {
    assert.equal(normalizeRequestPath("/api/queues?skip=jwt"), "/api/queues");
  });

  it("decodes percent-encoded path segments", () => {
    assert.equal(normalizeRequestPath("/api%2fqueues"), "/api/queues");
  });

  it("returns path unchanged when decode fails", () => {
    assert.equal(normalizeRequestPath("/api/%E0%A4%A"), "/api/%E0%A4%A");
  });
});
