import type { AddJobBody } from '../schemas/types'

export type PipelineCompanyContext = {
  companyId?: string
  companyName?: string
  wikidataId?: string
}

/** Per-URL company identity passed from Validate into parsePdf job data. */
export function companyJobDataForUrl(
  url: string,
  body: Pick<AddJobBody, 'urlContexts' | 'pipelineCompany'>
): Record<string, unknown> {
  const perUrl = body.urlContexts?.find((ctx) => ctx.url === url)
  const ctx = perUrl ?? body.pipelineCompany
  if (!ctx) return {}

  const data: Record<string, unknown> = {}
  if (ctx.companyId?.trim()) {
    data.companyId = ctx.companyId.trim()
  }
  if (ctx.companyName?.trim()) {
    data.companyName = ctx.companyName.trim()
  }
  if (ctx.wikidataId?.trim()) {
    data.wikidata = { node: ctx.wikidataId.trim() }
  }
  return data
}

export function mergeJobDataWithCompanyContext(
  base: Record<string, unknown>,
  url: string,
  body: Pick<AddJobBody, 'urlContexts' | 'pipelineCompany'>
): Record<string, unknown> {
  return { ...base, ...companyJobDataForUrl(url, body) }
}
