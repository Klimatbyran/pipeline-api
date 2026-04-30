import { FastifyInstance, FastifyRequest } from "fastify";
import { z } from "zod";
import { processesGroupedByCompanyResponseSchema, processesResponseSchema, processResponseSchema } from "../schemas/response";
import { readProcessPathParamsSchema, readProcessesByCompanyQueryStringSchema, readProcessesQueryStringSchema } from "../schemas/request";
import { ProcessService } from "../services/ProcessService";

export async function readProcessRoute(app: FastifyInstance) {
  app.get(
    '/',
    {
      schema: {
        summary: 'Get processes',
        description:
          'Optional batchId filters to processes (reports) in that batch only (exact match on job `data.batchId`). Values are opaque: often Garbo `Batch.id`, sometimes a legacy label. Garbo resolves when archiving.',
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
        description:
          'Returns unique `data.batchId` values from Redis-backed jobs (no Garbo join). Use as a live snapshot for operators; Validate uses Garbo `GET /queue-archive/batches` for the Postgres batch list.',
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
}