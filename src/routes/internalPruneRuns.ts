import { FastifyInstance } from "fastify";
import { z } from "zod";
import { validateInternalServiceToken } from "../middleware/internalServiceAuth";
import { RunRetentionService } from "../services/RunRetentionService";

const pruneRunsBodySchema = z.object({
  companyName: z.string().min(1).optional(),
  threadId: z.string().min(1).optional(),
  keepCount: z.number().int().positive().max(100).optional(),
  dryRun: z.boolean().optional(),
});

const pruneRunsResponseSchema = z.object({
  companiesProcessed: z.number(),
  runsPruned: z.number(),
  jobsRemoved: z.number(),
  skippedLiveRuns: z.number(),
  protectedThreadIds: z.array(z.string()),
  prunedThreadIds: z.array(z.string()),
  dryRun: z.boolean(),
});

export async function internalPruneRunsRoute(app: FastifyInstance) {
  app.route({
    url: "/prune-runs",
    method: "POST",
    preHandler: validateInternalServiceToken,
    schema: {
      body: pruneRunsBodySchema,
      response: {
        200: pruneRunsResponseSchema,
      },
      description:
        "Prune terminal pipeline runs beyond the per-company Redis retention cap",
      tags: ["Internal"],
    },
    handler: async (request) => {
      const body = pruneRunsBodySchema.parse(request.body);
      const service = await RunRetentionService.getRunRetentionService();
      return service.pruneRuns({
        companyName: body.companyName,
        excludeThreadId: body.threadId,
        keepCount: body.keepCount,
        dryRun: body.dryRun,
      });
    },
  });
}
