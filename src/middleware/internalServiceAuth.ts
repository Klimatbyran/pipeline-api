import { FastifyReply, FastifyRequest } from "fastify";
import apiConfig from "../config/api";

export async function validateInternalServiceToken(
  request: FastifyRequest,
  reply: FastifyReply,
): Promise<void> {
  const expected = apiConfig.internalServiceToken;
  if (!expected) {
    reply.status(503).send({
      error: "Internal service authentication is not configured",
    });
    return;
  }

  const provided = request.headers["x-internal-service-token"];
  if (typeof provided !== "string" || provided !== expected) {
    reply.status(401).send({ error: "Unauthorized" });
  }
}
