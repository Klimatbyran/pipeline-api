import { FastifyInstance, FastifyRequest } from "fastify";
import { QueueService } from "../services/QueueService";
import { AddJobBody, BaseJob } from "../schemas/types";
import { error404ResponseSchema, queueAddJobResponseSchema, queueJobResponseSchema, queueResponseSchema, queueStatsResponseSchema } from "../schemas/response";
import { STATUS, QUEUE_NAMES } from "../lib/bullmq";
import { JobType } from "bullmq";
import { addQueueJobBodySchema, readQueueJobPathParamsSchema, readQueuePathParamsSchema, readQueueQueryStringSchema, readQueueStatsQueryStringSchema } from "../schemas/request";

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
        description: 'Enqueue one or more URLs into the specified queue. Optional flags include autoApprove and forceReindex (alias: force-reindex).',
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
      const { urls, autoApprove, forceReindex, threadId } = request.body as any;
     
      // Resolve threadId: accept provided threadId/runId or generate a new one
      const providedOrCreatedThreadId = threadId || `run_${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
      // Log enqueue request (sanitized)
      app.log.info(
        {
          queue: name,
          urlsCount: Array.isArray(urls) ? urls.length : 0,
          autoApprove: !!autoApprove,
          forceReindex: !!forceReindex,
        },
        'Enqueue request received'
      );
      const queueService = await QueueService.getQueueService();
      const addedJobs: BaseJob[] = [];
      for(const url of urls) {
        const addedJob = await queueService.addJob(resolvedName, url, autoApprove, { forceReindex, threadId: providedOrCreatedThreadId });
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
}