import { RunRetentionService } from "../src/services/RunRetentionService";

function readArg(name: string): string | undefined {
  const prefix = `--${name}=`;
  const match = process.argv.find((arg) => arg.startsWith(prefix));
  return match ? match.slice(prefix.length) : undefined;
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

main().catch((err) => {
  console.error("Error in prune-runs-by-company:", err);
  process.exit(1);
});
