import 'dotenv/config'
import { z } from 'zod'

const nodeEnv = process.env.NODE_ENV

const envSchema = z.object({
  API_BASE_URL: z.string().default('http://localhost:3001'),
  PORT: z.coerce.number().default(3001),
  NODE_ENV: z.enum(['development', 'staging', 'production']).default('production'),
  JWT_SECRET: z.string().min(1, 'JWT_SECRET is required for authentication'),
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
  jwtSecret: env.JWT_SECRET,
}

export default apiConfig
