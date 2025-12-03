import { FastifyInstance, FastifyRequest } from "fastify";
import { QueueService } from "../services/QueueService";
import { processesGroupedByCompanyResponseSchema, processesResponseSchema, processResponseSchema } from "../schemas/response";
import { readProcessPathParamsSchema, readProcessesByCompanyQueryStringSchema } from "../schemas/request";
import { ProcessService } from "../services/ProcessService";

export async function readProcessRoute(app: FastifyInstance) {
  app.get(
    '/',
    {
      schema: {
        summary: 'Get processes',
        description: '',
        tags: ['Process'],
        response: {
          200: processesResponseSchema
        },
      },
    },
    async (
      _request,
      reply
    ) => {
      const processService = await ProcessService.getProcessService();
      const processes = await processService.getProcesses();      
      return reply.send(processes)
    }
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
        Querystring: { page?: number; pageSize?: number };
      }>,
      reply
    ) => {
      const requestedPage = request.query.page ?? 1;
      const requestedPageSize = request.query.pageSize ?? 100;

      const processService = await ProcessService.getProcessService();
      const companyProcesses = await processService.getPagedCompanyProcesses(
        requestedPage,
        requestedPageSize,
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