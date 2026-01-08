import Fastify from 'fastify'
import cors from '@fastify/cors'
import fastifySwagger from '@fastify/swagger'
import scalarPlugin from '@scalar/fastify-api-reference'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import apiConfig from './config/api'
import { readQueuesRoute } from './routes/readQueues'
import { jsonSchemaTransform, serializerCompiler, validatorCompiler, ZodTypeProvider } from 'fastify-type-provider-zod'
import { z } from 'zod'
import { readProcessRoute } from './routes/readProcess'
import { readPipelineRoute } from './routes/readPipeline'
import { validateJWT } from './middleware/auth'

async function startApp() {
  const app = Fastify({logger: true})
  .withTypeProvider<ZodTypeProvider>();
  
  app.setValidatorCompiler(validatorCompiler);
  app.setSerializerCompiler(serializerCompiler);

  app.register(cors, {
    origin: apiConfig.corsAllowOrigins as unknown as string[],
    exposedHeaders: ['etag'],
  })

  await app.register(fastifySwagger, {
    openapi: {
      openapi: '3.1.1',
      info: {
      title: 'Garbo Pipeline Status API',
      description: 'Information about the API to interact with the Garbo pipeline',
      version: JSON.parse(readFileSync(resolve('package.json'), 'utf-8'))
        .version,
      },
      servers: [
        {
          url: '/api',
        },
      ]
    },
    transform: jsonSchemaTransform,
  })

  // Apply JWT authentication to write operations only (POST, PUT, PATCH, DELETE)
  // Read operations (GET) are unprotected - frontend handles auth for UX
  // Public endpoints: /api/health, /api/export/openapi.json, /api (Scalar UI)
  app.addHook('onRequest', async (request, reply) => {
    const url = request.url;
    const method = request.method;
    
    // Public endpoints that never require authentication
    const publicPaths = [
      '/api/health',
      '/api/export/openapi.json',
    ];
    
    // Write methods that require authentication
    const writeMethods = ['POST', 'PUT', 'PATCH', 'DELETE'];
    const isWriteOperation = writeMethods.includes(method);
    
    // Protected route patterns (queues, processes, pipeline)
    const protectedRoutePatterns = [
      '/api/queues',
      '/api/processes',
      '/api/pipeline',
    ];
    
    // Check if this is a public endpoint
    const isPublicPath = publicPaths.some(path => url === path || url.startsWith(path + '/'));
    
    // Check if this is a protected route
    const isProtectedRoute = protectedRoutePatterns.some(pattern => 
      url.startsWith(pattern + '/') || url === pattern
    );
    
    // Apply JWT validation only to write operations on protected routes
    if (isWriteOperation && isProtectedRoute && !isPublicPath) {
      await validateJWT(request, reply);
    }
  });

  app.route({
    url: "/api/health",
    method: "GET",
    schema: {
      requests: {},
      response: {
      200: z.object({
        status: z.string(),
        timestamp: z.string(),
      })
      },
      description: "Health check endpoint",
      tags: ["System"]
    },
    handler: async (request, reply) => {
      return {
      status: "healthy",
      timestamp: new Date().toISOString()
      };
    }
  });

  app.route({
    url: "/api/export/openapi.json",
    method: "GET",
    schema: {
      description: "Export OpenAPI specification as JSON",
      tags: ["System"],
      response: {
        200: z.any()
      }
    },
    handler: async (request, reply) => {
      return app.swagger();
    }
  });

  app.register(readQueuesRoute, {
    prefix: '/api/queues',
  })

  app.register(readProcessRoute, {
    prefix: '/api/processes',
  })

  app.register(readPipelineRoute, {
    prefix: '/api/pipeline',
  })

  await app.register(scalarPlugin, {
	routePrefix: `/api`,
	configuration: {
	  metaData: {
		title: 'Garbo Pipeline Status API',
	  },
	},
  })  
  
  await app.ready();
  return app;
}
export default startApp
