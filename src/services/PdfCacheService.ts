import { createHash } from "crypto";
import { PDF_MAX_BYTES, uploadPdfAndGetUrls, type PdfUploadResult } from "./S3UploadService";
import { getRedisClient } from "../lib/redis-client";

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
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      // Some servers behave better with an explicit accept.
      Accept: "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    },
  });

  if (!res.ok) {
    throw new Error(`Failed to fetch PDF (${res.status})`);
  }

  const contentLengthHeader = res.headers.get("content-length");
  const contentLength = contentLengthHeader ? Number(contentLengthHeader) : undefined;
  if (typeof contentLength === "number" && Number.isFinite(contentLength) && contentLength > maxBytes) {
    throw new Error(`PDF too large (max ${maxBytes / 1024 / 1024} MB)`);
  }

  if (!res.body) {
    // Node fetch should always have a body for OK responses, but be defensive.
    const arr = new Uint8Array(await res.arrayBuffer());
    if (arr.byteLength > maxBytes) throw new Error(`PDF too large (max ${maxBytes / 1024 / 1024} MB)`);
    return { buffer: Buffer.from(arr), contentLength: arr.byteLength };
  }

  const chunks: Buffer[] = [];
  let total = 0;

  // Node 18+ uses a web ReadableStream; it supports async iteration.
  for await (const chunk of res.body as any) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buf.length;
    if (total > maxBytes) {
      throw new Error(`PDF too large (max ${maxBytes / 1024 / 1024} MB)`);
    }
    chunks.push(buf);
  }

  return { buffer: Buffer.concat(chunks, total), contentLength: contentLength ?? total };
}

function buildUrlMapKey(env: string, sourceUrl: string): string {
  // Keep Redis keys short and safe; store the raw URL in the value.
  return `pdf:urlmap:${env}:${sha1Hex(sourceUrl)}`;
}

function buildShaMapKey(env: string, sha256: string): string {
  return `pdf:shamap:${env}:${sha256}`;
}

export async function cachePdfFromUrl(sourceUrl: string): Promise<PdfCacheEntry> {
  const env = getUploadEnv();
  const redis = getRedisClient();
  const urlKey = buildUrlMapKey(env, sourceUrl);
  const TTL_SECONDS = 14 * 24 * 60 * 60; // 14 days

  // Fast path: if we already cached this URL, reuse.
  const cachedJson = await redis.get(urlKey);
  if (cachedJson) {
    try {
      const parsed = JSON.parse(cachedJson) as PdfCacheEntry;
      if (parsed?.publicUrl && parsed?.key && parsed?.sha256) return parsed;
    } catch {
      // ignore and recompute
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

  // Store both mappings with a bounded TTL so Redis stays a cache (not long-term storage).
  const shaKey = buildShaMapKey(env, upload.sha256);
  const payload = JSON.stringify(entry);
  await redis
    .multi()
    .set(urlKey, payload, "EX", TTL_SECONDS)
    .set(shaKey, payload, "EX", TTL_SECONDS)
    .exec();

  return entry;
}

