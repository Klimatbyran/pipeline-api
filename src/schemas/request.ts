import { string, z } from "zod";
import { jobStatusSchema } from "./common";

export const readQueuePathParamsSchema = z.object({
    name: z.string()
});

export const readProcessPathParamsSchema = z.object({
    id: z.string()
});

export const readProcessesQueryStringSchema = z.object({
    batchId: z.string().optional().describe(
        'Filter to processes in this batch only (exact match on job `data.batchId`). Opaque string: often Garbo `Batch.id` from Validate, or a legacy human-readable label on older jobs. Garbo resolves when archiving (see Garbo queue archive).',
    ),
});

export const readQueueQueryStringSchema = z.object({
    status: jobStatusSchema.optional()
});

export const readQueueStatsQueryStringSchema = z.object({
    queue: string().optional()
});

export const readQueueJobPathParamsSchema = z.object({
    name: z.string(),
    id: z.string()
});

export const readProcessesByCompanyQueryStringSchema = z.object({
    page: z.coerce.number().int().min(1).optional(),
    pageSize: z.coerce.number().int().min(1).max(500).optional(),
    batchId: z.string().optional().describe(
        'Filter to processes (reports) in this batch only. Same string as job `data.batchId`; Garbo maps it to `Batch.batchName` when persisting archived runs.',
    ),
});

// Accept both camelCase and kebab-case for the reindex flag.
// Normalization to camelCase is done in the route handler to avoid schema transforms
// that can accidentally mark fields as required in JSON schema.
export const addQueueJobBodySchema = z.object({
    autoApprove: z.boolean().optional().default(false),
    urls: z.array(string().url()),
    forceReindex: z.boolean().optional().describe('Re-index markdown even if already indexed'),
    replaceAllEmissions: z.boolean().optional().default(false).describe('Replace all scope 1,2,3 emissions and totals (delete old ones from all periods) before adding new ones'),
    runOnly: z.array(z.string()).optional().describe('Array of worker/queue names to run (limits pipeline execution to specified steps)'),
    batchId: z.string().optional().describe(
        'Optional grouping key on job `data.batchId` (opaque string). Prefer Garbo `Batch.id` when enqueueing from Validate; Garbo resolves id or legacy name when saving archived `ReportRun` rows.',
    ),
    tags: z.array(z.string()).optional().describe('Optional tags to set on the job/report at creation (e.g. for filtering); passed through to Garbo API. Worker may set or extend tags later'),
    cachePdf: z.boolean().optional().default(false).describe('When true (parsePdf only), fetch each URL and cache the PDF to S3 before enqueueing, so pipeline reads from storage instead of the source URL'),
});

export const rerunQueueJobBodySchema = z.object({
    data: z.record(z.any()).optional().describe('Optional job data overrides. Will merge with existing job data before re-running'),
}); 

export const rerunJobsByWorkerBodySchema = z.object({
    workerName: z.string().describe('Name of the worker / pipeline step to re-run (e.g. "scope1+2")'),
    statuses: z.array(jobStatusSchema).optional().describe('Optional list of job statuses to consider (defaults to completed and failed jobs)'),
    queues: z.array(z.string()).optional().describe('Optional list of queue names to restrict the rerun to (defaults to all known queues)'),
    limit: z.union([z.number(), z.literal('all')]).optional().default(5).describe('Maximum number of companies to rerun (defaults to 5). Use "all" to rerun all companies.'),
});

export const rerunAndSaveQueueJobBodySchema = z.object({
    scopes: z.array(z.string())
        .min(1)
        .describe('Scopes to rerun, e.g. [\"scope1\"], [\"scope2\"], [\"scope1+2\"], [\"scope3\"], or [\"scope1\", \"scope2\", \"scope3\"]'),
});