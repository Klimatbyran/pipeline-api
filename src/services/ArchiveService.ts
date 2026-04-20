import { Pool } from 'pg'
import archiveConfig from '../config/archive'
import { DataJob } from '../schemas/types'
import { QUEUE_NAMES } from '../lib/bullmq'

/** Garbo's JobRunArchive table name (Prisma default). */
const TABLE = 'JobRunArchive'

let pool: Pool | null = null

function getPool(): Pool {
  if (!archiveConfig.databaseUrl) {
    throw new Error('Archive is not configured (ARCHIVE_DATABASE_URL)')
  }
  if (!pool) {
    pool = new Pool({
      connectionString: archiveConfig.databaseUrl,
      max: 5,
      idleTimeoutMillis: 30_000,
    })
  }
  return pool
}

export function isArchiveConfigured(): boolean {
  return archiveConfig.isConfigured
}

/**
 * Map a JobRunArchive row to the same DataJob shape that Redis/transformJobtoBaseJob produces,
 * so ProcessService and callers see a single contract.
 */
function rowToDataJob(row: {
  id: string
  queueName: string
  jobName: string | null
  bullJobId: string
  url: string | null
  batchId: string | null
  status: string
  startedAt: Date
  finishedAt: Date
  inputData: unknown
  outputData: unknown
  error: string | null
}): DataJob {
  const inputData = (row.inputData as Record<string, unknown>) ?? {}
  const threadId = typeof inputData.threadId === 'string' ? inputData.threadId : undefined
  return {
    name: row.jobName ?? row.queueName,
    id: row.bullJobId,
    url: row.url ?? undefined,
    autoApprove: inputData.autoApprove === true,
    timestamp: new Date(row.startedAt).getTime(),
    processedOn: new Date(row.startedAt).getTime(),
    processId: threadId,
    threadId,
    queue: row.queueName,
    finishedOn: new Date(row.finishedAt).getTime(),
    attemptsMade: 1,
    failedReason: row.error ?? undefined,
    stacktrace: [],
    status: row.status as 'completed' | 'failed',
    data: row.inputData as Record<string, unknown>,
    returnvalue: row.outputData,
  }
}

/**
 * Fetch completed/failed jobs from JobRunArchive with optional filters.
 * Used for fast process list and batch list without scanning Redis.
 */
export async function getDataJobsFromArchive(options: {
  queueNames?: string[]
  processId?: string
  batchId?: string
  limit?: number
}): Promise<DataJob[]> {
  const pool = getPool()
  const { queueNames, processId, batchId, limit = 50_000 } = options

  const conditions: string[] = ['1 = 1']
  const params: unknown[] = []
  let idx = 0

  if (queueNames?.length) {
    conditions.push(`"queueName" = ANY($${++idx})`)
    params.push(queueNames)
  }
  if (processId) {
    conditions.push(`"inputData"->>'threadId' = $${++idx}`)
    params.push(processId)
  }
  if (batchId != null) {
    conditions.push(`"batchId" = $${++idx}`)
    params.push(batchId)
  }

  const limitClause = limit > 0 ? `LIMIT ${Math.min(limit, 100_000)}` : ''
  const query = `
    SELECT id, "queueName", "jobName", "bullJobId", url, "batchId", status, "startedAt", "finishedAt", "inputData", "outputData", error
    FROM "${TABLE}"
    WHERE ${conditions.join(' AND ')}
    ORDER BY "finishedAt" DESC
    ${limitClause}
  `

  const result = await pool.query(query, params)
  return result.rows.map(rowToDataJob)
}

/**
 * Return distinct batch IDs from the archive (fast, indexed).
 * Use with merge from Redis so very recent batches still show.
 */
export async function getBatchIdsFromArchive(): Promise<string[]> {
  const pool = getPool()
  const result = await pool.query<{ batchId: string }>(
    `SELECT DISTINCT "batchId" FROM "${TABLE}" WHERE "batchId" IS NOT NULL ORDER BY "batchId"`
  )
  return result.rows.map((r) => r.batchId)
}

/**
 * Close the pool (e.g. on shutdown). Safe to call if not configured.
 */
export async function closeArchivePool(): Promise<void> {
  if (pool) {
    await pool.end()
    pool = null
  }
}
