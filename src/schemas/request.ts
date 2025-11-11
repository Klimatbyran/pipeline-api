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

// Accept both camelCase and kebab-case for the reindex flag.
// Normalization to camelCase is done in the route handler to avoid schema transforms
// that can accidentally mark fields as required in JSON schema.
export const addQueueJobBodySchema = z.object({
    autoApprove: z.boolean().optional().default(false),
    urls: z.array(string().url()),
    forceReindex: z.boolean().optional().describe('Re-index markdown even if already indexed'),
    threadId: z.string().optional(),
    replaceAllEmissions: z.boolean().optional().default(false).describe('Replace all scope 1,2,3 emissions and totals (delete old ones from all periods) before adding new ones'),
    runOnly: z.array(z.string()).optional().describe('Array of worker/queue names to run (limits pipeline execution to specified steps)')
});