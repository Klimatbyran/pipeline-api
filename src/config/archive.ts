import 'dotenv/config'
import { z } from 'zod'

const envSchema = z.object({
  /** Postgres connection URL for garbo's JobRunArchive (read-only). When set, processes and batches are read from DB for faster response and full history. */
  ARCHIVE_DATABASE_URL: z.string().url().optional(),
})

const parsed = envSchema.safeParse(process.env)
const env = parsed.success ? parsed.data : { ARCHIVE_DATABASE_URL: undefined }

export default {
  /** When set, pipeline-api reads process/batch data from JobRunArchive (Postgres) for speed and history beyond 30 days. */
  databaseUrl: env?.ARCHIVE_DATABASE_URL,
  isConfigured: Boolean(env?.ARCHIVE_DATABASE_URL),
}
