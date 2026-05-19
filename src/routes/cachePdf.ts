import { FastifyInstance, FastifyRequest } from "fastify";
import { z } from "zod";
import { cachePdfFromUrl } from "../services/PdfCacheService";
import { isS3Configured } from "../config/s3";

const cachePdfBodySchema = z.object({
  url: z.string().url("url must be a valid URL"),
});

const cachePdfResponseSchema = z.object({
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

export async function cachePdfRoute(app: FastifyInstance) {
  app.post(
    "/",
    {
      schema: {
        summary: "Cache a PDF to S3",
        description:
          "Fetch a PDF from the given URL, upload it to S3, and return the stable S3 URL and metadata. Reuses an existing cached copy if the same source URL was previously cached.",
        tags: ["PDF"],
        body: cachePdfBodySchema,
        response: {
          200: cachePdfResponseSchema,
          503: z.object({ error: z.string() }),
        },
      },
    },
    async (
      request: FastifyRequest<{ Body: { url: string } }>,
      reply,
    ) => {
      if (!isS3Configured()) {
        return reply.status(503).send({
          error: "PDF caching is not configured. Set S3_BUCKET in the environment.",
        });
      }

      const { url } = request.body;
      const entry = await cachePdfFromUrl(url);
      return reply.send(entry);
    },
  );
}
