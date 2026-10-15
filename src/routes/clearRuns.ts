import { FastifyInstance } from "fastify";
import { z } from "zod";
import { RunRetentionService } from "../services/RunRetentionService";

const clearRunsBodySchema = z
  .object({
    threadIds: z.array(z.string().min(1)).max(500).optional(),
    clearAllTerminal: z.boolean().optional(),
    dryRun: z.boolean().optional(),
  })
  .refine(
    (body) =>
      body.clearAllTerminal === true ||
      (Array.isArray(body.threadIds) && body.threadIds.length > 0),
    {
      message: "Provide threadIds (non-empty) or clearAllTerminal: true",
    },
  );

const clearRunsResponseSchema = z.object({
  companiesProcessed: z.number(),
  runsPruned: z.number(),
  jobsRemoved: z.number(),
  skippedLiveRuns: z.number(),
  protectedThreadIds: z.array(z.string()),
  prunedThreadIds: z.array(z.string()),
  notFoundThreadIds: z.array(z.string()),
  skippedLiveThreadIds: z.array(z.string()),
  dryRun: z.boolean(),
});

/**
 * Staff JWT clear of finished BullMQ runs (Goffer Jobbstatus).
 * Mount under /api/runs — covered by the global write JWT hook.
 */
export async function clearRunsRoute(app: FastifyInstance) {
  app.route({
    url: "/clear",
    method: "POST",
    schema: {
      body: clearRunsBodySchema,
      response: {
        200: clearRunsResponseSchema,
      },
      description:
        "Clear finished BullMQ runs from Redis by threadId, or all non-live terminal runs. Active/waiting runs are never removed.",
      tags: ["Runs"],
    },
    handler: async (request) => {
      const body = clearRunsBodySchema.parse(request.body);
      const service = await RunRetentionService.getRunRetentionService();
      return service.clearRuns({
        threadIds: body.threadIds,
        clearAllTerminal: body.clearAllTerminal,
        dryRun: body.dryRun,
      });
    },
  });
}
