import { FastifyInstance, FastifyRequest } from "fastify";
import { randomUUID } from "crypto";
import { QueueService } from "../services/QueueService";
import { AddJobBody, BaseJob } from "../schemas/types";
import { error404ResponseSchema, queueAddJobResponseSchema, queueJobResponseSchema, queueResponseSchema, queueStatsResponseSchema } from "../schemas/response";
import { STATUS, QUEUE_NAMES } from "../lib/bullmq";
import { JobType } from "bullmq";
import { addQueueJobBodySchema, readQueueJobPathParamsSchema, readQueuePathParamsSchema, readQueueQueryStringSchema, readQueueStatsQueryStringSchema, rerunAndSaveQueueJobBodySchema, rerunJobsByWorkerBodySchema, rerunQueueJobBodySchema } from "../schemas/request";
import { z } from "zod";

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

  app.post(
    '/:name',
    {
      schema: {
        summary: 'Add job to a queue',
        description: 'Enqueue one or more URLs into the specified queue. Optional flags include autoApprove, replaceAllEmissions and forceReindex (alias: force-reindex).',
        tags: ['Queues'],
        params: readQueuePathParamsSchema,
        body: addQueueJobBodySchema,
        response: {
          200: queueAddJobResponseSchema
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
      const { urls, autoApprove, forceReindex, replaceAllEmissions, runOnly } = request.body as any;
      // Log enqueue request (sanitized)
      app.log.info(
        {
          queue: name,
          urlsCount: Array.isArray(urls) ? urls.length : 0,
          autoApprove: !!autoApprove,
          forceReindex: !!forceReindex,
          replaceAllEmissions: !!replaceAllEmissions,
          runOnly: runOnly,
        },
        'Enqueue request received'
      );
      const queueService = await QueueService.getQueueService();
      const addedJobs: BaseJob[] = [];
      for(const url of urls) {
        // Generate a unique threadId for each URL; client-provided threadId is ignored
        const perUrlThreadId = randomUUID();
        const addedJob = await queueService.addJob(resolvedName, url, autoApprove, { forceReindex, threadId: perUrlThreadId, replaceAllEmissions, runOnly });
        addedJobs.push(addedJob);
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
          'From a follow-up job (e.g. scope1+2 or scope3), find the original EXTRACT_EMISSIONS job and enqueue a new one with runOnly set to the requested scopes.',
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
        summary: 'Re-run all jobs that match a given worker name',
        description: 'Re-runs all jobs across one or more queues whose data.runOnly[] contains the specified worker name. Defaults to completed and failed jobs.',
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
        }
      }>,
      reply
    ) => {
      const { workerName, statuses, queues } = request.body;

      const queueService = await QueueService.getQueueService();

      const resolvedQueues = queues && queues.length > 0
        ? queues
        : Object.values(QUEUE_NAMES);

      const result = await queueService.rerunJobsByWorkerName(workerName, {
        queueNames: resolvedQueues,
        statuses,
      });

      return reply.send(result);
    }
  );
}