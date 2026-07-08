import { FastifyReply, FastifyRequest } from "fastify";
import apiConfig from "../config/api";

export async function validateInternalServiceToken(
  request: FastifyRequest,
  reply: FastifyReply,
): Promise<void> {
  const expected = apiConfig.internalServiceToken;
  if (!expected) {
    return reply.status(503).send({
      error: "Internal service authentication is not configured",
    });
  }

  const provided = request.headers["x-internal-service-token"];
  if (typeof provided !== "string" || provided !== expected) {
    return reply.status(401).send({ error: "Unauthorized" });
  }
}
