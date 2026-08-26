import { createHash } from "crypto";
import { PDF_MAX_BYTES, uploadPdfAndGetUrls, type PdfUploadResult } from "./S3UploadService";
import { getRedisClient } from "../lib/redis-client";
import {
  assertBufferLooksLikePdf,
  assertUrlSafeForServerFetch,
  PDF_FETCH_MAX_REDIRECTS,
  PDF_FETCH_TIMEOUT_MS,
} from "../lib/outbound-pdf-url";

export type PdfCacheEntry = {
  env: string;
  sourceUrl: string;
  sha256: string;
  bucket: string;
  key: string;
  publicUrl: string;
  reusedExisting: boolean;
  uploaded: boolean;
  fetchedAt: string;
  contentLength?: number;
};

function getUploadEnv(): string {
  const nodeEnv = (process.env.NODE_ENV ?? "").trim();
  if (nodeEnv === "production") return "prod";
  if (nodeEnv === "staging") return "stage";
  return "dev";
}

function sha1Hex(input: string): string {
  return createHash("sha1").update(input).digest("hex");
}

async function fetchPdfToBuffer(url: string, maxBytes: number): Promise<{ buffer: Buffer; contentLength?: number }> {
  let currentUrl = url;
  const signal = AbortSignal.timeout(PDF_FETCH_TIMEOUT_MS);

  for (let redirect = 0; redirect <= PDF_FETCH_MAX_REDIRECTS; redirect++) {
    await assertUrlSafeForServerFetch(currentUrl);

    const res = await fetch(currentUrl, {
      redirect: "manual",
      signal,
      headers: {
        Accept: "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
      },
    });

    if (res.status >= 300 && res.status < 400) {
      if (res.body) {
        await res.body.cancel().catch(() => undefined);
      }
      const loc = res.headers.get("location");
      if (!loc) {
        throw new Error(`Redirect ${res.status} without Location header`);
      }
      currentUrl = new URL(loc, currentUrl).href;
      continue;
    }

    if (!res.ok) {
      throw new Error(`Failed to fetch PDF (${res.status})`);
    }

    const contentLengthHeader = res.headers.get("content-length");
    const contentLength = contentLengthHeader ? Number(contentLengthHeader) : undefined;
    if (typeof contentLength === "number" && Number.isFinite(contentLength) && contentLength > maxBytes) {
      throw new Error(`PDF too large (max ${maxBytes / 1024 / 1024} MB)`);
    }

    if (!res.body) {
      const arr = new Uint8Array(await res.arrayBuffer());
      if (arr.byteLength > maxBytes) throw new Error(`PDF too large (max ${maxBytes / 1024 / 1024} MB)`);
      const buffer = Buffer.from(arr);
      assertBufferLooksLikePdf(buffer);
      return { buffer, contentLength: arr.byteLength };
    }

    const chunks: Buffer[] = [];
    let total = 0;

    for await (const chunk of res.body as AsyncIterable<Uint8Array | Buffer>) {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      total += buf.length;
      if (total > maxBytes) {
        throw new Error(`PDF too large (max ${maxBytes / 1024 / 1024} MB)`);
      }
      chunks.push(buf);
    }

    const buffer = Buffer.concat(chunks, total);
    assertBufferLooksLikePdf(buffer);
    return { buffer, contentLength: contentLength ?? total };
  }

  throw new Error(`Too many redirects (max ${PDF_FETCH_MAX_REDIRECTS})`);
}

function buildUrlMapKey(env: string, sourceUrl: string): string {
  return `pdf:urlmap:${env}:${sha1Hex(sourceUrl)}`;
}

export async function cachePdfFromUrl(sourceUrl: string): Promise<PdfCacheEntry> {
  const env = getUploadEnv();
  const redis = getRedisClient();
  const urlKey = buildUrlMapKey(env, sourceUrl);
  const TTL_SECONDS = 14 * 24 * 60 * 60; // 14 days

  const cachedJson = await redis.get(urlKey);
  if (cachedJson) {
    try {
      const parsed = JSON.parse(cachedJson) as PdfCacheEntry;
      if (parsed?.publicUrl && parsed?.key && parsed?.sha256) return parsed;
    } catch {
      /* ignore and recompute */
    }
  }

  const { buffer, contentLength } = await fetchPdfToBuffer(sourceUrl, PDF_MAX_BYTES);
  const upload: PdfUploadResult = await uploadPdfAndGetUrls(buffer, sourceUrl);

  const entry: PdfCacheEntry = {
    env,
    sourceUrl,
    sha256: upload.sha256,
    bucket: upload.bucket,
    key: upload.key,
    publicUrl: upload.publicUrl,
    reusedExisting: upload.reusedExisting,
    uploaded: upload.uploaded,
    fetchedAt: new Date().toISOString(),
    ...(typeof contentLength === "number" ? { contentLength } : {}),
  };

  const payload = JSON.stringify(entry);
  await redis.set(urlKey, payload, "EX", TTL_SECONDS);

  return entry;
}
