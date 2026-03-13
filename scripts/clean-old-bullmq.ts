import { Queue } from "bullmq";
import redisConfig from "../src/config/redis";
import { QUEUE_NAMES } from "../src/lib/bullmq";

async function main() {
  // Roughly 5 months ≈ 150 days
  const graceDays = 150;
  const graceMs = graceDays * 24 * 60 * 60 * 1000;

  console.log(`Cleaning BullMQ jobs older than ${graceDays} days…`);

  const connection = {
    host: redisConfig.host,
    port: redisConfig.port,
    password: redisConfig.password,
  };

  const queueNames = Object.values(QUEUE_NAMES);

  let totalRemovedAllQueues = 0;

  for (const name of queueNames) {
    const queue = new Queue(name, { connection });
    console.log(`Queue: ${name}`);

    let totalRemovedThisQueue = 0;

    // Clean completed and failed jobs older than graceMs
    for (const status of ["completed", "failed"] as const) {
      try {
        const removed = await queue.clean(graceMs, 1000, status);
        const count = removed.length;
        totalRemovedThisQueue += count;
        totalRemovedAllQueues += count;
        console.log(`  ${status}: removed ${count} jobs`);
      } catch (err) {
        console.error(`  ${status}: error cleaning`, err);
      }
    }

    await queue.close();
    console.log(`  total removed (queue): ${totalRemovedThisQueue}`);
  }

  console.log(`BullMQ cleanup finished. Total removed (all queues): ${totalRemovedAllQueues}`);
}

main().catch((err) => {
  console.error("Error in clean-old-bullmq:", err);
  process.exit(1);
});

