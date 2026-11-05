import Fastify from 'fastify'
import cors from '@fastify/cors'
import multipart from '@fastify/multipart'
import { PDF_MAX_BYTES } from './services/S3UploadService'
import fastifySwagger from '@fastify/swagger'
import scalarPlugin from '@scalar/fastify-api-reference'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import apiConfig from './config/api'
import { readQueuesRoute } from './routes/readQueues'
import { cachePdfRoute } from './routes/cachePdf'
import { jsonSchemaTransform, serializerCompiler, validatorCompiler, ZodTypeProvider } from 'fastify-type-provider-zod'
import { z } from 'zod'
import { readProcessRoute } from './routes/readProcess'
import { readPipelineRoute } from './routes/readPipeline'
import { internalPruneRunsRoute } from './routes/internalPruneRuns'
import { clearRunsRoute } from './routes/clearRuns'
import { normalizeRequestPath, validateJWT } from './middleware/auth'

async function startApp() {
  const app = Fastify({logger: true})
  .withTypeProvider<ZodTypeProvider>();
  
  app.setValidatorCompiler(validatorCompiler);
  app.setSerializerCompiler(serializerCompiler);

  app.register(cors, {
    origin: apiConfig.corsAllowOrigins as unknown as string[],
    exposedHeaders: ['etag'],
  })

  await app.register(multipart, {
    limits: {
      fileSize: PDF_MAX_BYTES, // 400 MB per file (annual reports with images)
      files: 20,
    },
    throwFileSizeLimit: true, // reject with 413 and clear error when file exceeds limit
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

  // JWT on all methods for protected API routes. Public: /api/health, openapi export, Scalar UI (/api).
  // /api/internal uses x-internal-service-token on route preHandlers, not JWT.
  // NODE_ENV=development skips JWT for local DX; staging/production must not use development.
  app.addHook('onRequest', async (request, reply) => {
    const path = normalizeRequestPath(request.url);

    const publicPaths = [
      '/api/health',
      '/api/export/openapi.json',
    ];

    const protectedRoutePatterns = [
      '/api/queues',
      '/api/processes',
      '/api/pipeline',
      '/api/runs',
      '/api/cache-pdf',
      '/api/internal',
    ];

    const isPublicPath = publicPaths.some(
      (publicPath) => path === publicPath || path.startsWith(publicPath + '/')
    );

    const isProtectedRoute = protectedRoutePatterns.some(
      (pattern) => path === pattern || path.startsWith(pattern + '/')
    );

    const isInternalServicePath =
      path === '/api/internal' || path.startsWith('/api/internal/');

    const isDevelopment = process.env.NODE_ENV === 'development';
    if (
      !isDevelopment &&
      isProtectedRoute &&
      !isPublicPath &&
      !isInternalServicePath
    ) {
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

  app.register(cachePdfRoute, {
    prefix: '/api/cache-pdf',
  })
  app.register(readQueuesRoute, {
    prefix: '/api/queues',
  })

  app.register(readProcessRoute, {
    prefix: '/api/processes',
  })

  app.register(readPipelineRoute, {
    prefix: '/api/pipeline',
  })

  app.register(internalPruneRunsRoute, {
    prefix: '/api/internal',
  })

  app.register(clearRunsRoute, {
    prefix: '/api/runs',
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
