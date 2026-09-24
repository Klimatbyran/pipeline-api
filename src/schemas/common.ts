import { z } from "zod";

export const jobStatusSchema = z.enum(['active', 'waiting', 'waiting-children', 'prioritized', 'completed', 'failed', 'delayed', 'paused', 'repeat', 'wait']);

export const processStatusSchema = z.enum(['active', 'completed', 'failed', 'waiting', 'skipped_no_emissions']);

const pipelineQueueSchema = z.object({
    id: z.string(),
    name: z.string(),
    next: z.object({
        selection: z.boolean().default(false).optional(),
        target: z.array(z.string())
    }).optional()
});

export const pipelineSchema = z.array(pipelineQueueSchema);