import { FastifyInstance, FastifyRequest } from "fastify";
import { z } from "zod";
import { processesGroupedByCompanyResponseSchema, processesResponseSchema, processResponseSchema, queueResponseSchema } from "../schemas/response";
import { readProcessPathParamsSchema, readProcessesByCompanyQueryStringSchema, readProcessesQueryStringSchema } from "../schemas/request";
import { ProcessService } from "../services/ProcessService";
import { QueueService } from "../services/QueueService";
import { QUEUE_NAMES } from "../lib/bullmq";

export async function readProcessRoute(app: FastifyInstance) {
  app.get(
    '/',
    {
      schema: {
        summary: 'Get processes',
        description: 'Optional batchId filters to processes (reports) in that batch only.',
        tags: ['Process'],
        querystring: readProcessesQueryStringSchema,
        response: {
          200: processesResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{ Querystring: { batchId?: string } }>,
      reply
    ) => {
      const batchId = request.query.batchId;
      const processService = await ProcessService.getProcessService();
      const processes = await processService.getProcesses(batchId);      
      return reply.send(processes)
    }
  ); 
  
  app.get(
    '/batches',
    {
      schema: {
        summary: 'List available batch IDs',
        description: 'Returns unique batch IDs from all jobs. Use batchId query on /companies to filter by batch.',
        tags: ['Process'],
        response: {
          200: z.object({ batches: z.array(z.string()) }),
        },
      },
    },
    async (_request, reply) => {
      const processService = await ProcessService.getProcessService();
      const batches = await processService.getAvailableBatches();
      return reply.send({ batches });
    },
  );

  app.get(
    '/companies',
    {
      schema: {
        summary: 'Get processes by companies',
        description: '',
        tags: ['Process'],
        querystring: readProcessesByCompanyQueryStringSchema,
        response: {
          200: processesGroupedByCompanyResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Querystring: { page?: number; pageSize?: number; batchId?: string };
      }>,
      reply
    ) => {
      const requestedPage = request.query.page ?? 1;
      const requestedPageSize = request.query.pageSize ?? 100;
      const batchId = request.query.batchId;

      const processService = await ProcessService.getProcessService();
      const companyProcesses = await processService.getPagedCompanyProcesses(
        requestedPage,
        requestedPageSize,
        batchId,
      ); 
      return reply.send(companyProcesses)
    }
  ); 

  app.get(
    '/:id',
    {
      schema: {
        summary: 'Get jobs in requested process',
        description: '',
        tags: ['Process'],
        params: readProcessPathParamsSchema,
        response: {
          200: processResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: {id: string},
      }>,
      reply
    ) => {
      const { id } = request.params;
      const processService = await ProcessService.getProcessService();
      const process = await processService.getProcess(id);
      return reply.send(process)
    }
  );

  app.get(
    '/:id/pdf-parsing',
    {
      schema: {
        summary: 'Get PDF-parsing jobs (parsePdf/doclingParsePDF) for a threadId',
        description: 'Jobs with a callbackUrl (e.g. handed off to climate-plans-pipeline) are excluded from GET /:id\'s normal process view — this returns exactly those jobs, by threadId, so a caller that received the threadId via a different channel (e.g. a callback payload) can still look up how the PDF parsing went.',
        tags: ['Process'],
        params: readProcessPathParamsSchema,
        response: {
          200: queueResponseSchema
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: {id: string},
      }>,
      reply
    ) => {
      const { id } = request.params;
      const queueService = await QueueService.getQueueService();
      const jobs = await queueService.getDataJobs(
        [QUEUE_NAMES.PARSE_PDF, QUEUE_NAMES.DOCLING_PARSE_PDF],
        undefined,
        id,
        undefined,
        true,
      );
      return reply.send(jobs)
    }
  );
}