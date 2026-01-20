import 'dotenv/config'
import { z } from 'zod'

const nodeEnv = process.env.NODE_ENV

const envSchema = z.object({
  API_BASE_URL: z.string().default('http://localhost:3001'),
  PORT: z.coerce.number().default(3001),
  NODE_ENV: z.enum(['development', 'staging', 'production']).default('production'),
  JWT_SECRET: z.string().optional().describe('Required only for write operations (POST, PUT, PATCH, DELETE). GET requests work without it.'),
})

const env = envSchema.parse(process.env);

const developmentOrigins = [
  'http://localhost:5174',
  'http://localhost:3000',
] as const

const stageOrigins = [
  'https://stage-validation.klimatkollen.se',
] as const

const productionOrigins = [
  'https://validation.klimatkollen.se',
] as const

const apiConfig = {
  corsAllowOrigins:
    nodeEnv === 'staging'
      ? stageOrigins
      : nodeEnv === 'production'
      ? productionOrigins
      : developmentOrigins,

  baseURL: env.API_BASE_URL,
  port: env.PORT,
  jwtSecret: env.JWT_SECRET, // Optional - only needed for write operations
}

export default apiConfig
