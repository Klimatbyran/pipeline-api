import { FastifyRequest, FastifyReply } from "fastify";
import jwt from "jsonwebtoken";
import apiConfig from "../config/api";

/**
 * Normalize the request path for auth checks: strip query/hash, URL-decode when valid.
 */
export function normalizeRequestPath(url: string): string {
  const pathOnly = url.split("?")[0].split("#")[0];
  try {
    return decodeURIComponent(pathOnly);
  } catch {
    return pathOnly;
  }
}

/**
 * JWT Token Validation Middleware
 *
 * Validates JWT tokens from the Authorization header for protected API routes
 * (all HTTP methods). Prevents unauthorized access to pipeline data and mutations.
 *
 * Skipped when NODE_ENV is `development` (see app onRequest hook). Staging and
 * production must not run with NODE_ENV=development.
 *
 * Token format: Authorization: Bearer <token>
 */
export async function validateJWT(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  // Extract token from Authorization header
  const authHeader = request.headers.authorization;

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    reply.status(401).send({
      error: "No token provided",
      message: "Authorization header must be in format: Bearer <token>",
    });
    return;
  }

  const token = authHeader.substring(7); // Remove 'Bearer ' prefix

  try {
    // Verify token signature and expiration
    if (!apiConfig.jwtSecret) {
      reply.status(500).send({
        error: "JWT authentication not configured",
        message: "JWT_SECRET environment variable is required for protected API routes.",
      });
      return;
    }
    const decoded = jwt.verify(token, apiConfig.jwtSecret);

    // Attach user info to request for potential use in route handlers
    // Type assertion needed because jwt.verify returns string | JwtPayload
    (request as any).user = decoded;

    // Continue to route handler
    return;
  } catch (error: any) {
    // Handle different JWT error types
    if (error.name === "TokenExpiredError") {
      reply.status(401).send({
        error: "Token expired",
        message: "The authentication token has expired. Please log in again.",
      });
      return;
    }

    if (error.name === "JsonWebTokenError") {
      reply.status(401).send({
        error: "Invalid token",
        message: "The authentication token is invalid.",
      });
      return;
    }

    if (error.name === "NotBeforeError") {
      reply.status(401).send({
        error: "Token not active",
        message: "The authentication token is not yet active.",
      });
      return;
    }

    // Generic error for any other JWT verification failures
    reply.status(401).send({
      error: "Authentication failed",
      message: "Token validation failed.",
    });
    return;
  }
}
