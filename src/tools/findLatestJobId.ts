import { STATUS } from "../lib/bullmq";
import { writeFileSync } from "fs";

/**
 * Hard-coded API base URL for the pipeline API you want to query.
 * Example: "https://pipeline-api.staging.klimatkollen.se"
 */
const API_BASE_URL = "https://stage-pipeline-api.klimatkollen.se";

/**
 * EDIT THESE VALUES BEFORE RUNNING
 */
const TARGET_WIKIDATA_ID = "Q1671804"; // e.g. "Q1337240"

/**
 * File where all fetched jobs will be written for manual inspection/search.
 */
const OUTPUT_PATH = "findLatestJobId-output.json";

/**
 * This must match the BullMQ queue name, e.g.
 *  - "followUpScope1"
 *  - "followUpScope2"
 *  - "extractEmissions"
 */
const TARGET_QUEUE_NAME = "followUpScope3";

/**
 * Optional: limit to a specific report year.
 * Set to undefined if you do not want to filter by year.
 */
const TARGET_REPORT_YEAR: number | undefined = 2024;

/**
 * Optional: filter by job status.
 * Common values: "completed", "failed", "waiting", "delayed", ...
 * Set to undefined to include all statuses.
 */
const TARGET_STATUS: STATUS | undefined = "completed";

type BaseJobFromApi = {
  id?: string;
  timestamp?: number;
  status?: string;
};

type DataJobFromApi = BaseJobFromApi & {
  data?: {
    companyName?: string;
    // Most jobs currently do not have a reportYear; they often have fiscalYear instead.
    // We keep reportYear as optional in case it is added in the future.
    reportYear?: number;
    fiscalYear?: {
      startMonth?: number;
      endMonth?: number;
      [key: string]: unknown;
    };
    wikidata?: {
      node?: string;
      [key: string]: unknown;
    };
    [key: string]: unknown;
  };
};

async function loadJobsFromRemoteApi(): Promise<BaseJobFromApi[]> {
  const baseUrl = API_BASE_URL.replace(/\/$/, "");
  const query = TARGET_STATUS ? `?status=${encodeURIComponent(TARGET_STATUS)}` : "";
  const url = `${baseUrl}/api/queues/${encodeURIComponent(TARGET_QUEUE_NAME)}${query}`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to load jobs from API: ${response.status} ${response.statusText}`);
  }

  const jobs = (await response.json()) as BaseJobFromApi[];
  return jobs;
}

async function loadJobDataFromRemoteApi(jobId: string): Promise<DataJobFromApi> {
  const baseUrl = API_BASE_URL.replace(/\/$/, "");
  const url = `${baseUrl}/api/queues/${encodeURIComponent(TARGET_QUEUE_NAME)}/${encodeURIComponent(
    jobId
  )}`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to load job data for id ${jobId}: ${response.status} ${response.statusText}`);
  }

  const job = (await response.json()) as DataJobFromApi;
  return job;
}

async function loadJobsWithData(): Promise<DataJobFromApi[]> {
  const baseJobs = await loadJobsFromRemoteApi();
  console.log(`Loaded ${baseJobs.length} jobs from API for queue "${TARGET_QUEUE_NAME}".`);

  const jobsWithIds = baseJobs.filter((job) => job.id !== undefined && job.id !== null) as Array<
    BaseJobFromApi & { id: string }
  >;

  const detailedJobs: DataJobFromApi[] = [];

  for (const baseJob of jobsWithIds) {
    try {
      const detailedJob = await loadJobDataFromRemoteApi(baseJob.id);
      detailedJobs.push(detailedJob);
    } catch (error) {
      console.warn(`Skipping job ${baseJob.id} due to error:`, error);
    }
  }

  return detailedJobs;
}

function saveJobsToFile(jobs: DataJobFromApi[]): void {
  try {
    const serialized = JSON.stringify(jobs, null, 2);
    writeFileSync(OUTPUT_PATH, serialized, { encoding: "utf8" });
    console.log(`Wrote ${jobs.length} jobs to file: ${OUTPUT_PATH}`);
  } catch (error) {
    console.warn("Failed to write jobs to file:", error);
  }
}

function printSampleCompanies(jobs: DataJobFromApi[], sampleSize: number = 10) {
  if (jobs.length === 0) {
    console.log("No jobs returned from API to inspect.");
    return;
  }

  console.log(`First ${Math.min(sampleSize, jobs.length)} jobs from API:`);
  jobs.slice(0, sampleSize).forEach((job, index) => {
    const jobData = job.data ?? {};
    const companyName: string | undefined = jobData.companyName;
    const reportYear: number | undefined = jobData.reportYear;
    const wikidataId: string | undefined = (jobData.wikidata as { node?: string } | undefined)?.node;
    console.log(
      `  [${index}] id=${job.id ?? "unknown"} companyName=${companyName ?? "unknown"} wikidataId=${
        wikidataId ?? "unknown"
      } reportYear=${reportYear ?? "unknown"}`
    );
  });
}

function filterJobsForCompanyAndYear(jobs: DataJobFromApi[]) {
  return jobs.filter((job) => {
    const jobData = job.data ?? {};
    const jobWikidataId: string | undefined = (jobData.wikidata as { node?: string } | undefined)?.node;
    const jobReportYear: number | undefined = jobData.reportYear;

    if (jobWikidataId !== TARGET_WIKIDATA_ID) {
      return false;
    }

    return true;
  });
}

function sortJobsByNewestFirst(jobs: DataJobFromApi[]) {
  return [...jobs].sort((firstJob, secondJob) => {
    const firstTimestamp: number = firstJob.timestamp ?? 0;
    const secondTimestamp: number = secondJob.timestamp ?? 0;
    return secondTimestamp - firstTimestamp;
  });
}

function printJobSummary(job: DataJobFromApi) {
  const jobData = job.data ?? {};
  const companyName: string | undefined = jobData.companyName;
  const reportYear: number | undefined = jobData.reportYear;
  const wikidataId: string | undefined = (jobData.wikidata as { node?: string } | undefined)?.node;

  const timestamp = job.timestamp ?? 0;
  const timestampIso = new Date(timestamp).toISOString();

  console.log("Latest job for wikidata id and queue:");
  console.log(`  Queue:       ${TARGET_QUEUE_NAME}`);
  console.log(`  Company:     ${companyName ?? "unknown"}`);
  console.log(`  Wikidata:    ${wikidataId ?? "unknown"}`);
  console.log(`  Report year: ${reportYear ?? "unknown"}`);
  console.log(`  Job ID:      ${job.id ?? "unknown"}`);
  console.log(`  Status:      ${job.status ?? "unknown"}`);
  console.log(`  Timestamp:   ${timestamp} (${timestampIso})`);
}

async function main() {
  console.log(
    `Searching for latest job in queue "${TARGET_QUEUE_NAME}" for wikidata id "${TARGET_WIKIDATA_ID}"` +
      (TARGET_REPORT_YEAR !== undefined ? ` and year ${TARGET_REPORT_YEAR}` : "")
  );

  if (TARGET_STATUS) {
    console.log(`Filtering by status: ${TARGET_STATUS}`);
  }

  const jobs = await loadJobsWithData();
  saveJobsToFile(jobs);
  printSampleCompanies(jobs);

  const matchingJobs = filterJobsForCompanyAndYear(jobs);

  if (matchingJobs.length === 0) {
    console.log("No matching jobs found.");
    return;
  }

  const sortedJobs = sortJobsByNewestFirst(matchingJobs);
  const latestJob = sortedJobs[0];

  printJobSummary(latestJob);
}

main().catch((error) => {
  console.error("Failed to find latest job ID", error);
  process.exit(1);
});

