import { FastifyInstance, FastifyRequest } from "fastify";
import { randomUUID } from "crypto";
import { QueueService } from "../services/QueueService";
import { AddJobBody, BaseJob } from "../schemas/types";
import { error404ResponseSchema, queueAddJobResponseSchema, queueJobResponseSchema, queueResponseSchema, queueStatsResponseSchema } from "../schemas/response";
import { STATUS, QUEUE_NAMES } from "../lib/bullmq";
import { JobType } from "bullmq";
import { addQueueJobBodySchema, readQueueJobPathParamsSchema, readQueuePathParamsSchema, readQueueQueryStringSchema, readQueueStatsQueryStringSchema, rerunAndSaveQueueJobBodySchema, rerunJobsByWorkerBodySchema, rerunQueueJobBodySchema } from "../schemas/request";
import { z } from "zod";
import { type PdfUploadResult, uploadPdfAndGetUrls } from "../services/S3UploadService";
import { isS3Configured } from "../config/s3";
import { cachePdfFromUrl, type PdfCacheEntry } from "../services/PdfCacheService";

const pdfUploadMetaSchema = z.object({
  filename: z.string(),
  publicUrl: z.string().url(),
  bucket: z.string(),
  key: z.string(),
  sha256: z.string(),
  reusedExisting: z.boolean(),
  uploaded: z.boolean(),
});

const pdfCacheEntrySchema = z.object({
  env: z.string(),
  sourceUrl: z.string().url(),
  sha256: z.string(),
  bucket: z.string(),
  key: z.string(),
  publicUrl: z.string().url(),
  reusedExisting: z.boolean(),
  uploaded: z.boolean(),
  fetchedAt: z.string(),
  contentLength: z.number().optional(),
});

const pdfCacheUrlErrorSchema = z.object({
  url: z.string(),
  error: z.string(),
});

async function uploadAndEnqueueParsePdfJobs(params: {
  queueService: QueueService;
  files: { buffer: Buffer; filename: string }[];
  options: {
    autoApprove?: boolean;
    batchId?: string;
    forceReindex?: boolean;
    replaceAllEmissions?: boolean;
    runOnly?: string[];
    tags?: string[];
  };
  request: FastifyRequest;
  fileTooLargeMessage: string;
}): Promise<
  | { ok: true; jobs: BaseJob[]; uploads: Array<{ filename: string } & PdfUploadResult> }
  | { ok: false; status: 413 | 500; error: string }
> {
  const { queueService, files, options, request, fileTooLargeMessage } = params;

  const addedJobs: BaseJob[] = [];
  const uploads: Array<{ filename: string } & PdfUploadResult> = [];
  for (const { buffer, filename } of files) {
    let upload: PdfUploadResult;
    try {
      upload = await uploadPdfAndGetUrls(buffer, filename);
    } catch (err: any) {
      request.log.warn({ err, filename }, 'S3 upload failed');
      const isTooLarge = err?.message?.includes('too large') ?? false;
      return {
        ok: false,
        status: isTooLarge ? 413 : 500,
        error: isTooLarge ? fileTooLargeMessage : (err?.message ?? 'Failed to upload PDF to storage.'),
      };
    }

    request.log.info(
      { filename, url: upload.publicUrl, reusedExisting: upload.reusedExisting, uploaded: upload.uploaded },
      'S3 upload succeeded, adding to BullMQ'
    );
    uploads.push({ filename, ...upload });
    const perUrlThreadId = randomUUID();
    const addedJob = await queueService.addJob(QUEUE_NAMES.PARSE_PDF, upload.publicUrl, options.autoApprove ?? false, {
      forceReindex: options.forceReindex,
      threadId: perUrlThreadId,
      replaceAllEmissions: options.replaceAllEmissions,
      runOnly: options.runOnly,
      batchId: options.batchId,
      tags: options.tags,
      data: {
        sourceUrl: `uploaded:${filename}`,
        pdfCache: {
          sha256: upload.sha256,
          bucket: upload.bucket,
          key: upload.key,
          publicUrl: upload.publicUrl,
          reusedExisting: upload.reusedExisting,
          uploaded: upload.uploaded,
        },
      },
    });
    request.log.info({ filename, jobId: addedJob.id }, 'BullMQ job added successfully');
    addedJobs.push(addedJob);
  }

  return { ok: true, jobs: addedJobs, uploads };
}

async function parseParsePdfUpload(request: FastifyRequest): Promise<{
  options: {
    autoApprove?: boolean;
    batchId?: string;
    forceReindex?: boolean;
    replaceAllEmissions?: boolean;
    runOnly?: string[];
    tags?: string[];
  };
  files: { buffer: Buffer; filename: string }[];
}> {
  const options: {
    autoApprove?: boolean;
    batchId?: string;
    forceReindex?: boolean;
    replaceAllEmissions?: boolean;
    runOnly?: string[];
    tags?: string[];
  } = {};

  const files: { buffer: Buffer; filename: string }[] = [];

  const parts = (request as any).parts();
  for await (const part of parts) {
    if (part.type === 'field') {
      const raw = part.value;
      const value = typeof raw === 'string' ? raw : String(raw ?? '');

      switch (part.fieldname) {
        case 'autoApprove':
          options.autoApprove = value === 'true' || value === '1';
          break;
        case 'batchId': {
          const s = typeof raw === 'string' ? raw : raw == null ? undefined : String(raw);
          options.batchId = s ?? undefined;
          break;
        }
        case 'forceReindex':
          options.forceReindex = value === 'true' || value === '1';
          break;
        case 'replaceAllEmissions':
          options.replaceAllEmissions = value === 'true' || value === '1';
          break;
        case 'runOnly':
          try {
            options.runOnly = value ? JSON.parse(value) : undefined;
          } catch {
            /* ignore invalid JSON */
          }
          break;
        case 'tags':
          try {
            options.tags = value ? JSON.parse(value) : undefined;
          } catch {
            /* ignore invalid JSON */
          }
          break;
      }
    } else if (part.type === 'file') {
      const buffer = await part.toBuffer();
      const filename = part.filename ?? 'report.pdf';
      if (buffer.length > 0) {
        files.push({ buffer, filename });
      }
    }
  }

  return { options, files };
}

export async function readQueuesRoute(app: FastifyInstance) {
  app.get(
    '/',
    {
      schema: {
        summary: 'Get jobs',
        description: '',
        tags: ['Queues'],
        querystring: readQueueQueryStringSchema,
        response: {
          200: queueResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Querystring: {status?: STATUS}
      }>,
      reply
    ) => {
      const { status } = request.query;
      const queueService = await QueueService.getQueueService();
      const jobs = await queueService.getJobs([], status);      
      return reply.send(jobs)
    }
  );
  
  app.get(
    '/:name',
    {
      schema: {
        summary: 'Get jobs in requested queue',
        description: '',
        tags: ['Queues'],
        params: readQueuePathParamsSchema,
        querystring: readQueueQueryStringSchema,
        response: {
          200: queueResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: {name: string},
        Querystring: {status?: STATUS}
      }>,
      reply
    ) => {
      const { name } = request.params;
      const { status } = request.query;
      const queueService = await QueueService.getQueueService();
      const jobs = await queueService.getJobs([name], status);      
      return reply.send(jobs)
    }
  );

  // File upload for parsePdf: must be registered before POST /:name so path parsePdf/upload is matched
  app.post(
    '/parsePdf/upload',
    {
      schema: {
        summary: 'Upload PDFs and add parsePdf jobs',
        description: 'Accept multipart/form-data with PDF files and optional options (autoApprove, batchId, forceReindex, replaceAllEmissions, runOnly, tags). Same job shape as URL-based POST /queues/parsePdf. Requires S3_BUCKET to be set.',
        tags: ['Queues'],
        consumes: ['multipart/form-data'],
        response: {
          200: z.object({
            jobs: queueAddJobResponseSchema,
            uploads: z.array(pdfUploadMetaSchema),
          }),
          400: z.object({ error: z.string() }),
          413: z.object({ error: z.string() }),
          500: z.object({ error: z.string() }),
          503: z.object({ error: z.string() }),
        },
      },
    },
    async (request, reply) => {
      if (!isS3Configured()) {
        return reply.status(503).send({
          error: 'PDF upload is not configured. Set S3_BUCKET in the environment.',
        });
      }
      const FILE_TOO_LARGE_MSG = 'File too large. Maximum size is 400 MB per file.';
      const queueService = await QueueService.getQueueService();
      let options: {
        autoApprove?: boolean;
        batchId?: string;
        forceReindex?: boolean;
        replaceAllEmissions?: boolean;
        runOnly?: string[];
        tags?: string[];
      };
      let files: { buffer: Buffer; filename: string }[];

      try {
        ({ options, files } = await parseParsePdfUpload(request));
      } catch (err: any) {
        if (err?.statusCode === 413 || err?.code === 'FST_REQ_FILE_TOO_LARGE') {
          return reply.status(413).send({ error: FILE_TOO_LARGE_MSG });
        }
        throw err;
      }

      if (files.length === 0) {
        return reply.status(400).send({ error: 'At least one PDF file is required (multipart field name: file or files).' });
      }

      const uploadResult = await uploadAndEnqueueParsePdfJobs({
        queueService,
        files,
        options,
        request,
        fileTooLargeMessage: FILE_TOO_LARGE_MSG,
      });
      if (!uploadResult.ok) {
        return reply.status(uploadResult.status).send({ error: uploadResult.error });
      }

      app.log.info(
        {
          queue: 'parsePdf',
          uploadCount: files.length,
          ...options,
        },
        'ParsePdf upload request completed'
      );
      return reply.send({
        jobs: uploadResult.jobs,
        uploads: uploadResult.uploads,
      });
    }
  );

  app.post(
    '/:name',
    {
      schema: {
        summary: 'Add job to a queue',
        description: 'Enqueue one or more URLs into the specified queue. Optional flags include autoApprove, replaceAllEmissions and forceReindex (alias: force-reindex). For parsePdf only: set cachePdf=true to cache PDFs to S3 before enqueueing so workers read from storage.',
        tags: ['Queues'],
        params: readQueuePathParamsSchema,
        body: addQueueJobBodySchema,
        response: {
          200: z.union([
            queueAddJobResponseSchema,
            z.object({
              jobs: queueAddJobResponseSchema,
              cached: z.array(pdfCacheEntrySchema),
              errors: z.array(pdfCacheUrlErrorSchema).optional(),
            }),
          ]),
          400: z.object({
            error: z.string(),
            errors: z.array(pdfCacheUrlErrorSchema).optional(),
          }),
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: {name: string},
        Body: AddJobBody
      }>,
      reply
    ) => {
      const { name } = request.params;
      const resolvedName = Object.values(QUEUE_NAMES).find(q => q.toLowerCase() === name.toLowerCase());
      if (!resolvedName) {
        return reply.status(400).send({ error: `Unknown queue '${name}'. Valid queues: ${Object.values(QUEUE_NAMES).join(', ')}` });
      }
      const { urls, autoApprove, forceReindex, replaceAllEmissions, runOnly, batchId, tags, cachePdf, callbackUrl } = request.body;
      // Log enqueue request (sanitized)
      app.log.info(
        {
          queue: name,
          urlsCount: Array.isArray(urls) ? urls.length : 0,
          autoApprove: !!autoApprove,
          forceReindex: !!forceReindex,
          replaceAllEmissions: !!replaceAllEmissions,
          runOnly: runOnly,
          batchId: batchId ?? undefined,
          tags: tags ?? undefined,
          cachePdf: !!cachePdf,
        },
        'Enqueue request received'
      );
      const queueService = await QueueService.getQueueService();
      const addedJobs: BaseJob[] = [];
      const cached: PdfCacheEntry[] = [];
      const cacheErrors: Array<{ url: string; error: string }> = [];

      for (const url of urls) {
        if (resolvedName === QUEUE_NAMES.PARSE_PDF && cachePdf) {
          if (!isS3Configured()) {
            return reply.status(503).send({
              error: 'PDF caching is not configured. Set S3_BUCKET in the environment.',
            });
          }
          try {
            const entry = await cachePdfFromUrl(url);
            const perUrlThreadId = randomUUID();
            const addedJob = await queueService.addJob(resolvedName, entry.publicUrl, autoApprove, {
              forceReindex,
              threadId: perUrlThreadId,
              replaceAllEmissions,
              runOnly,
              batchId,
              tags,
              data: {
                sourceUrl: url,
                pdfCache: {
                  sha256: entry.sha256,
                  bucket: entry.bucket,
                  key: entry.key,
                  publicUrl: entry.publicUrl,
                  reusedExisting: entry.reusedExisting,
                  uploaded: entry.uploaded,
                  fetchedAt: entry.fetchedAt,
                },
              },
            });
            addedJobs.push(addedJob);
            cached.push(entry);
          } catch (err: any) {
            const message = err?.message ? String(err.message) : 'Failed to cache or enqueue PDF';
            request.log.warn({ err, url }, 'parsePdf cachePdf failed for URL');
            cacheErrors.push({ url, error: message });
          }
          continue;
        }
        const perUrlThreadId = randomUUID();
        const addedJob = await queueService.addJob(resolvedName, url, autoApprove, { forceReindex, threadId: perUrlThreadId, replaceAllEmissions, runOnly, batchId, tags, callbackUrl });
        addedJobs.push(addedJob);
      }

      if (resolvedName === QUEUE_NAMES.PARSE_PDF && cachePdf) {
        if (cacheErrors.length > 0 && addedJobs.length === 0) {
          return reply.status(400).send({
            error: 'Could not cache or enqueue any PDFs from the provided URLs.',
            errors: cacheErrors,
          });
        }
        if (cached.length > 0 || cacheErrors.length > 0) {
          return reply.send({
            jobs: addedJobs,
            cached,
            ...(cacheErrors.length > 0 ? { errors: cacheErrors } : {}),
          });
        }
      }

      return reply.send(addedJobs);
    }
  );

  app.get(
    '/stats',
    {
      schema: {
        summary: 'Get queue job stats',
        description: '',
        tags: ['Queues'],
        querystring: readQueueStatsQueryStringSchema,
        response: {
          200: queueStatsResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Querystring: {queue?: string},
      }>,
      reply
    ) => {
      const { queue } = request.query;
      const queueService = await QueueService.getQueueService();
      const stats = await queueService.getQueueStats(queue);      
      return reply.send(stats)
    }
  );

  app.get(
    '/:name/:id',
    {
      schema: {
        summary: 'Get job data',
        description: '',
        tags: ['Queues'],
        params: readQueueJobPathParamsSchema,
        response: {
          200: queueJobResponseSchema,
          400: error404ResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: {name: string, id: string}
      }>,
      reply
    ) => {
      const { name, id } = request.params;
      const queueService = await QueueService.getQueueService();
      try {
        const jobData = await queueService.getJobData(name, id);
        return reply.send(jobData)
      } catch (error) {
        return reply.status(404).send({ error: 'Job does not exist in this queue' })
      }     
    }
  );

  app.post(
    '/:name/:id/rerun',
    {
      schema: {
        summary: 'Re-run a job (resume delayed job or retry failed job)',
        description: 'Resumes a delayed job (e.g., approval pending) or retries a failed job. Optionally allows updating job data before re-running. For completed jobs, creates a new job with the same data.',
        tags: ['Queues'],
        params: readQueueJobPathParamsSchema,
        body: rerunQueueJobBodySchema,
        response: {
          200: queueJobResponseSchema,
          400: error404ResponseSchema,
          404: error404ResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: {name: string, id: string},
        Body: {
          data?: Record<string, any>;
        }
      }>,
      reply
    ) => {
      const { name, id } = request.params;
      const { data: dataOverrides } = request.body;
      
      const queueService = await QueueService.getQueueService();
      
      try {
        const updatedJob = await queueService.rerunJob(name, id, dataOverrides);
        return reply.send(updatedJob);
      } catch (error: any) {
        if (error.message?.includes('not found')) {
          return reply.status(404).send({ error: 'Job does not exist in this queue' });
        }
        if (error.message?.includes('already')) {
          return reply.status(400).send({ error: error.message });
        }
        app.log.error(error, 'Error re-running job');
        return reply.status(500).send({ error: 'Failed to re-run job' });
      }
    }
  );

  app.post(
    '/:name/:id/rerun-and-save',
    {
      schema: {
        summary: 'Re-run extract-emissions for this process and save results',
        description:
          'From a follow-up job (e.g. scope1, scope2, scope1+2, or scope3), find the original EXTRACT_EMISSIONS job and enqueue a new one with runOnly set to the requested scopes. This overwrites any existing runOnly value.',
        tags: ['Queues'],
        params: readQueueJobPathParamsSchema,
        body: rerunAndSaveQueueJobBodySchema,
        response: {
          200: queueJobResponseSchema,
          400: error404ResponseSchema,
          404: error404ResponseSchema,
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: { name: string; id: string };
        Body: { scopes: string[] };
      }>,
      reply
    ) => {
      const { name, id } = request.params;
      const { scopes } = request.body;

      const queueService = await QueueService.getQueueService();

      try {
        const newJob = await queueService.rerunExtractEmissionsFromFollowup(
          name,
          id,
          scopes
        );
        return reply.send(newJob);
      } catch (error: any) {
        const msg = error?.message ?? '';

        if (msg.includes('EXTRACT_EMISSIONS job') || msg.includes('threadId')) {
          return reply.status(404).send({ error: msg });
        }

        if (msg.includes('Unknown queue')) {
          return reply.status(400).send({ error: msg });
        }

        app.log.error(error, 'Error in rerun-and-save');
        return reply.status(500).send({ error: 'Failed to rerun and save emissions' });
      }
    }
  );

  app.post(
    '/rerun-by-worker',
    {
      schema: {
        summary: 'Re-run all jobs that match a given worker name (using rerun-and-save)',
        description: 'Re-runs all jobs across one or more queues whose data.runOnly[] contains the specified worker name using the rerun-and-save approach (finds original EXTRACT_EMISSIONS job and creates new one with specified scopes). Defaults to completed and failed jobs.',
        tags: ['Queues'],
        body: rerunJobsByWorkerBodySchema,
        response: {
          200: z.object({
            totalMatched: z.number().describe('Total number of jobs that matched the criteria'),
            perQueue: z.record(z.number()).describe('Number of matched jobs per queue name'),
          })
        },
      },
    },
    async (
      request: FastifyRequest<{
        Body: {
          workerName: string;
          statuses?: JobType[];
          queues?: string[];
          limit?: number | 'all';
        }
      }>,
      reply
    ) => {
      const { workerName, statuses, queues, limit } = request.body;

      const queueService = await QueueService.getQueueService();

      const resolvedQueues = queues && queues.length > 0
        ? queues
        : Object.values(QUEUE_NAMES);

      const result = await queueService.rerunJobsByWorkerName(workerName, {
        queueNames: resolvedQueues,
        statuses,
        limit,
      });

      return reply.send(result);
    }
  );
}