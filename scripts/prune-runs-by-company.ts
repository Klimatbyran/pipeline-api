import {
  DEFAULT_FAILED_MAX_AGE_MS,
  DEFAULT_KEEP_SUCCESSFUL_RUN_COUNT,
} from "../src/lib/runRetention";
import { RunRetentionService } from "../src/services/RunRetentionService";

// This script runs as a CronJob with concurrencyPolicy: Forbid — if a
// run never exits, Kubernetes will never start the next scheduled run either,
// since it still considers the stuck one "active". Bounding this script's own
// runtime — independent of *why* it might hang — fixes that regardless of
// the specific cause; see also activeDeadlineSeconds on the CronJob itself
// as a second, Kubernetes-level backstop.
// Sequential retention scan can take longer than a parallel getDataJobs load;
// stay under CronJob activeDeadlineSeconds (600s).
const MAX_RUNTIME_MS = 8 * 60 * 1000;

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
  const keepSuccessfulRaw = readArg("keep-successful");
  const failedMaxAgeHoursRaw = readArg("failed-max-age-hours");

  const keepCount =
    keepCountRaw != null ? Number(keepCountRaw) : undefined;
  const keepSuccessfulCount =
    keepSuccessfulRaw != null ? Number(keepSuccessfulRaw) : undefined;
  const failedMaxAgeHours =
    failedMaxAgeHoursRaw != null ? Number(failedMaxAgeHoursRaw) : undefined;

  if (
    keepCountRaw != null &&
    (!Number.isInteger(keepCount) || keepCount! < 0)
  ) {
    throw new Error("--keep must be an integer >= 0 (0 = prune all terminal runs)");
  }
  if (
    keepSuccessfulRaw != null &&
    (!Number.isInteger(keepSuccessfulCount) || keepSuccessfulCount! < 0)
  ) {
    throw new Error("--keep-successful must be an integer >= 0");
  }
  if (
    failedMaxAgeHoursRaw != null &&
    (!Number.isFinite(failedMaxAgeHours) || failedMaxAgeHours! < 0)
  ) {
    throw new Error("--failed-max-age-hours must be a number >= 0");
  }

  const useLegacy = keepCountRaw != null;
  const failedMaxAgeMs =
    failedMaxAgeHours != null
      ? failedMaxAgeHours * 60 * 60 * 1000
      : undefined;

  console.log(
    useLegacy
      ? `Pruning Redis runs (legacy keepCount=${keepCount})${company ? ` for company "${company}"` : ""}${dryRun ? " (dry-run)" : ""}…`
      : `Pruning Redis runs (keep ${keepSuccessfulCount ?? DEFAULT_KEEP_SUCCESSFUL_RUN_COUNT} successful; failed max age ${failedMaxAgeHours ?? DEFAULT_FAILED_MAX_AGE_MS / 3_600_000}h)${company ? ` for company "${company}"` : ""}${dryRun ? " (dry-run)" : ""}…`,
  );

  const service = await RunRetentionService.getRunRetentionService();
  const result = await service.pruneRuns({
    companyName: company,
    ...(useLegacy
      ? { keepCount }
      : {
          keepSuccessfulCount,
          failedMaxAgeMs,
        }),
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
