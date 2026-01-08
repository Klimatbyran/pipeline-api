import { FastifyRequest, FastifyReply } from 'fastify';
import jwt from 'jsonwebtoken';
import apiConfig from '../config/api';

/**
 * JWT Token Validation Middleware
 * 
 * Validates JWT tokens from the Authorization header.
 * Used to protect write operations (POST, PUT, PATCH, DELETE) to prevent
 * unauthorized pipeline triggers and modifications.
 * 
 * Read operations (GET) are unprotected - frontend handles auth for UX.
 * 
 * Token format: Authorization: Bearer <token>
 */
export async function validateJWT(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  // Extract token from Authorization header
  const authHeader = request.headers.authorization;

  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    reply.status(401).send({ 
      error: 'No token provided',
      message: 'Authorization header must be in format: Bearer <token>'
    });
    return;
  }

  const token = authHeader.substring(7); // Remove 'Bearer ' prefix

  try {
    // Verify token signature and expiration
    const decoded = jwt.verify(token, apiConfig.jwtSecret);

    // Attach user info to request for potential use in route handlers
    // Type assertion needed because jwt.verify returns string | JwtPayload
    (request as any).user = decoded;

    // Continue to route handler
    return;
  } catch (error: any) {
    // Handle different JWT error types
    if (error.name === 'TokenExpiredError') {
      reply.status(401).send({ 
        error: 'Token expired',
        message: 'The authentication token has expired. Please log in again.'
      });
      return;
    }

    if (error.name === 'JsonWebTokenError') {
      reply.status(401).send({ 
        error: 'Invalid token',
        message: 'The authentication token is invalid.'
      });
      return;
    }

    if (error.name === 'NotBeforeError') {
      reply.status(401).send({ 
        error: 'Token not active',
        message: 'The authentication token is not yet active.'
      });
      return;
    }

    // Generic error for any other JWT verification failures
    reply.status(401).send({ 
      error: 'Authentication failed',
      message: 'Token validation failed.'
    });
    return;
  }
}
