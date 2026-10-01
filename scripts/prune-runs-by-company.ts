import { RunRetentionService } from "../src/services/RunRetentionService";

// This script runs as a daily CronJob with concurrencyPolicy: Forbid — if a
// run never exits, Kubernetes will never start the next day's run either,
// since it still considers the stuck one "active". That's exactly what
// happened in production: Redis was briefly unreachable, and the BullMQ
// connection this script goes through (src/lib/bullmq.ts) has no
// retryStrategy override, so ioredis fell back to its own default, which
// retries the TCP connection forever with no stopping condition — correct
// for the long-running API server (reconnect once Redis recovers, don't
// crash), wrong for a one-shot script, which silently hung for 63 days and
// blocked every subsequent scheduled prune. Bounding this script's own
// runtime — independent of *why* it might hang — fixes that regardless of
// the specific cause; see also activeDeadlineSeconds on the CronJob itself
// as a second, Kubernetes-level backstop.
const MAX_RUNTIME_MS = 2 * 60 * 1000;

function readArg(name: string): string | undefined {
  const prefix = `--${name}=`;
  const equalsMatch = process.argv.find((arg) => arg.startsWith(prefix));
  if (equalsMatch) return equalsMatch.slice(prefix.length);

  const flagIndex = process.argv.indexOf(`--${name}`);
  if (flagIndex >= 0) {
    const next = process.argv[flagIndex + 1];
    if (next && !next.startsWith("--")) return next;
  }

  return undefined;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

async function main() {
  const company = readArg("company");
  const dryRun = hasFlag("dry-run");
  const keepCountRaw = readArg("keep");
  const keepCount = keepCountRaw ? Number(keepCountRaw) : undefined;

  if (keepCountRaw != null && (!Number.isInteger(keepCount) || keepCount! <= 0)) {
    throw new Error("--keep must be a positive integer");
  }

  console.log(
    `Pruning Redis runs${company ? ` for company "${company}"` : " for all companies"}${dryRun ? " (dry-run)" : ""}…`,
  );

  const service = await RunRetentionService.getRunRetentionService();
  const result = await service.pruneRuns({
    companyName: company,
    keepCount,
    dryRun,
  });

  console.log("Prune summary:", result);
}

const timeout = new Promise<never>((_, reject) => {
  setTimeout(
    () =>
      reject(
        new Error(
          `prune-runs-by-company: timed out after ${MAX_RUNTIME_MS}ms — likely Redis unreachable (see comment above MAX_RUNTIME_MS)`,
        ),
      ),
    MAX_RUNTIME_MS,
  ).unref();
});

Promise.race([main(), timeout])
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Error in prune-runs-by-company:", err);
    process.exit(1);
  });
