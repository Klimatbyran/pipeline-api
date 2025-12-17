import { JobType, Queue } from "bullmq"
import redis from "../config/redis"

export type STATUS = 'active' | 'waiting' | 'waiting-children' | 'prioritized' | 'completed' | 'failed' | 'delayed' | 'paused';
export const JOB_STATUS = ['active', 'waiting', 'waiting-children', 'prioritized', 'completed', 'failed', 'delayed', 'paused'];

export const QUEUE_NAMES = {
  NLM_PARSE_PDF: 'nlmParsePDF',
  DOCLING_PARSE_PDF: 'doclingParsePDF',
  PARSE_PDF: 'parsePdf',
  NLM_EXTRACT_TABLES: 'nlmExtractTables',
  INDEX_MARKDOWN: 'indexMarkdown',
  PRECHECK: 'precheck',
  GUESS_WIKIDATA: 'guessWikidata',
  // Legacy combined scope queue (kept for backwards compatibility)
  FOLLOW_UP_SCOPE_12: 'followUpScope12',
  // New split scope queues
  FOLLOW_UP_SCOPE_1: 'followUpScope1',
  FOLLOW_UP_SCOPE_2: 'followUpScope2',
  FOLLOW_UP_SCOPE_3: 'followUpScope3',
  FOLLOW_UP_BIOGENIC: 'followUpBiogenic',
  FOLLOW_UP_ECONOMY: 'followUpEconomy',
  FOLLOW_UP_GOALS: 'followUpGoals',
  FOLLOW_UP_INITIATIVES: 'followUpInitiatives',
  FOLLOW_UP_FISCAL_YEAR: 'followUpFiscalYear',
  FOLLOW_UP_COMPANY_TAGS: 'followUpCompanyTags',
  FOLLOW_UP_BASE_YEAR: 'followUpBaseYear',
  FOLLOW_UP_INDUSTRY_GICS: 'followUpIndustryGics',
  EXTRACT_EMISSIONS: 'extractEmissions',
  CHECK_DB: 'checkDB',
  DIFF_BASE_YEAR: 'diffBaseYear',
  DIFF_GOALS: 'diffGoals',
  DIFF_INDUSTRY: 'diffIndustry',
  DIFF_INITIATIVES: 'diffInitiatives',
  DIFF_REPORTING_PERIODS: 'diffReportingPeriods',
  DIFF_TAGS: 'diffTags',
  SAVE_TO_API: 'saveToAPI',
  SEND_COMPANY_LINK: 'sendCompanyLink',
  WIKIPEDIA_UPLOAD: 'wikipediaUpload',
}

async function startQueues() {    
    const queues = Object.values(QUEUE_NAMES).map((name) => new Queue(name, { connection: redis }))
    await Promise.all(queues.map((queue) => queue.waitUntilReady()))
    return queues
}

export default startQueues;