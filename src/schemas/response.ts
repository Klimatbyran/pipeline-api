import { z } from 'zod';
import { jobStatusSchema, processStatusSchema } from './common';

export const approvalDataSchema = z.object({
  oldValue: z.any(),
  newValue: z.any()
});

export const approvalSchema = z.object({
  summary: z.string(),
  type: z.string(),
  data: approvalDataSchema,
  approved: z.boolean().default(false),
  metadata: z.object({
    comment: z.string().optional(),
    source: z.string().optional()
  })
})

export const baseJobSchema = z.object({
    name: z.string(),
    id: z.string().optional(),
    url: z.string().url().optional(),
    autoApprove: z.boolean().optional().default(false),
    timestamp: z.number(),
    processId: z.string().optional(),
    threadId: z.string().optional(),
    queue: z.string(),
    processedBy: z.string().optional(),
    finishedOn: z.number().optional(),
    attemptsMade: z.number(),
    failedReason: z.string().optional(),
    stacktrace: z.array(z.string()).optional(),
    progress: z.number().optional(),
    opts: z.record(z.string(), z.any()).optional(),
    delay: z.number().optional(),
    approval: approvalSchema.optional(),
    status: jobStatusSchema.optional(),
});

export const dataJobSchema = baseJobSchema.extend({
    data: z.any().optional(),    
    returnvalue: z.any().optional(),
});

export const queueStatusSchema = z.object({
  name: z.string(),
  status: z.record(jobStatusSchema, z.number().optional())
})

export const processSchema = z.object({
  id: z.string(),
  company: z.string().optional(),
  wikidataId: z.string().optional(),
  year: z.number().optional(),
  status: processStatusSchema.optional(),
  jobs: z.array(baseJobSchema),
  startedAt: z.number().optional(),
  finishedAt: z.number().optional()
})

export const companyProcessSchema = z.object({
  company: z.string().optional(),
  wikidataId: z.string().optional(),
  processes: z.array(processSchema.omit({company: true, wikidataId: true}))
});

export const queueStatsResponseSchema = z.array(queueStatusSchema);

export const queueResponseSchema = z.array(baseJobSchema);

export const processResponseSchema = processSchema;

export const queueAddJobResponseSchema = z.array(baseJobSchema);

export const queueJobResponseSchema = dataJobSchema;

export const processesResponseSchema = z.array(processSchema);

export const processesGroupedByCompanyResponseSchema = z.array(companyProcessSchema);

export const error404ResponseSchema = z.object({
  error: z.string(),
});