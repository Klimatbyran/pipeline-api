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
});