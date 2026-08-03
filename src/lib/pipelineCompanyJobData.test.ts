import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import {
  companyJobDataForUrl,
  mergeJobDataWithCompanyContext,
} from './pipelineCompanyJobData.js'

describe('pipelineCompanyJobData', () => {
  it('merges per-url context into job data', () => {
    assert.deepEqual(
      companyJobDataForUrl('https://example.com/a.pdf', {
        urlContexts: [
          {
            url: 'https://example.com/a.pdf',
            companyId: '11111111-1111-4111-8111-111111111111',
            companyName: 'Meta',
            wikidataId: 'Q380',
          },
        ],
      }),
      {
        companyId: '11111111-1111-4111-8111-111111111111',
        companyName: 'Meta',
        wikidata: { node: 'Q380' },
      }
    )
  })

  it('falls back to pipelineCompany for all urls', () => {
    assert.deepEqual(
      mergeJobDataWithCompanyContext(
        { sourceUrl: 'https://example.com/a.pdf' },
        'https://example.com/a.pdf',
        {
          pipelineCompany: { wikidataId: 'Q380' },
        }
      ),
      {
        sourceUrl: 'https://example.com/a.pdf',
        wikidata: { node: 'Q380' },
      }
    )
  })

  it('prefers per-url context over pipelineCompany', () => {
    assert.deepEqual(
      companyJobDataForUrl('https://example.com/a.pdf', {
        pipelineCompany: { wikidataId: 'Q1' },
        urlContexts: [{ url: 'https://example.com/a.pdf', wikidataId: 'Q380' }],
      }),
      {
        wikidata: { node: 'Q380' },
      }
    )
  })
})
