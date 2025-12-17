import { string, z } from "zod";
import { jobStatusSchema } from "./common";

export const readQueuePathParamsSchema = z.object({
    name: z.string()
});

export const readProcessPathParamsSchema = z.object({
    id: z.string()
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
});

// Accept both camelCase and kebab-case for the reindex flag.
// Normalization to camelCase is done in the route handler to avoid schema transforms
// that can accidentally mark fields as required in JSON schema.
export const addQueueJobBodySchema = z.object({
    autoApprove: z.boolean().optional().default(false),
    urls: z.array(string().url()),
    forceReindex: z.boolean().optional().describe('Re-index markdown even if already indexed'),
    replaceAllEmissions: z.boolean().optional().default(false).describe('Replace all scope 1,2,3 emissions and totals (delete old ones from all periods) before adding new ones'),
    runOnly: z.array(z.string()).optional().describe('Array of worker/queue names to run (limits pipeline execution to specified steps)')
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