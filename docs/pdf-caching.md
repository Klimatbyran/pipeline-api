# PDF caching & dedupe (S3)

This API supports **full caching** of PDFs into object storage so the pipeline can operate on stable URLs, even when source links change or become unavailable.

## Storage layout

- **Key format**: `uploads/{env}/{sha256}.pdf`
  - `{env}` is derived from `NODE_ENV`:
    - `production` → `prod`
    - `staging` → `stage`
    - otherwise → `dev`
  - `{sha256}` is the SHA-256 hex digest of the PDF bytes.

## Dedupe behavior

- If two PDFs are **byte-identical**, they will produce the same SHA-256 and therefore the same object key.
  - The API will **reuse** the existing object and return the existing URL.
- If two PDFs are “similar” but not identical (e.g. different year, changed numbers, regenerated PDF metadata), they will produce different hashes and will be stored separately.

In practice, SHA-256 collisions are considered negligible for this use case.

## Endpoints

### Upload PDF files (multipart)

`POST /api/queues/parsePdf/upload`

- Uploads one or more PDFs to S3 and enqueues `parsePdf` jobs using the stored URL.
- Response includes per-file metadata:
  - `reusedExisting`: true if the object already existed
  - `uploaded`: true if a new object was stored in this request

### Enqueue from URLs with full cache

`POST /api/queues/parsePdf` with JSON body including `cachePdf=true`

- Fetches each URL server-side, caches the PDF to S3, and enqueues the job using the cached S3 URL.
- The original URL is preserved in job data as `sourceUrl`.

## Redis mapping (“URL table”)

When caching from URLs, the API stores a mapping in Redis so repeated submissions can skip refetching:

- **URL → cache entry**: `pdf:urlmap:{env}:{sha1(sourceUrl)}`
- **SHA → cache entry**: `pdf:shamap:{env}:{sha256}`

The stored value is a JSON blob containing:
`{ env, sourceUrl, sha256, bucket, key, publicUrl, reusedExisting, uploaded, fetchedAt, contentLength? }`

This behaves like a lightweight “table” without requiring a SQL database in `pipeline-api`.

### Retention / TTL

These Redis keys are stored with a **14-day TTL**. After expiry, the next request will refetch the PDF from the source URL and re-cache it.

## Environment variables

- **Required for caching**:
  - `S3_BUCKET`
  - `S3_PUBLIC_BASE_URL` or `S3_ENDPOINT`
  - `NODE_ENV` should be set correctly (`staging` in stage; `production` in prod).

## Operational notes

- Consider S3 lifecycle rules for `uploads/dev/*` and/or `uploads/stage/*` to avoid long-term accumulation during testing.
- Cached PDFs are full copies stored in your bucket.

