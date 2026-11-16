import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Queue, Worker } from "bullmq";
import IORedis from "ioredis";
import { trimInfraQueues } from "./infraQueueRetention";

describe("trimInfraQueues", () => {
  it("trims pipelineAutoRun completed jobs down to 20", async () => {
    const connection = new IORedis({
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: Number(process.env.REDIS_PORT || 6379),
      maxRetriesPerRequest: null,
    });
    await connection.flushdb();

    const queue = new Queue("pipelineAutoRun", {
      connection,
      defaultJobOptions: { removeOnComplete: false },
    });
    const worker = new Worker(
      "pipelineAutoRun",
      async () => ({ ok: true }),
      { connection, concurrency: 1 },
    );

    try {
      for (let i = 0; i < 25; i++) {
        await queue.add("tick", { i });
      }
      await new Promise((r) => setTimeout(r, 2000));
      assert.equal(await queue.getCompletedCount(), 25);

      const results = await trimInfraQueues();
      const autoRun = results.find((r) => r.queue === "pipelineAutoRun");
      assert.ok(autoRun);
      assert.equal(autoRun!.completedRemoved, 5);
      assert.equal(autoRun!.completedRemaining, 20);
      assert.equal(await queue.getCompletedCount(), 20);
    } finally {
      await worker.close();
      await queue.obliterate({ force: true });
      await queue.close();
      await connection.quit();
    }
  });
});
