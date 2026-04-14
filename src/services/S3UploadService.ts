import { HeadObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { S3Client } from '@aws-sdk/client-s3';
import { getS3Config } from '../config/s3';
import { createHash } from 'crypto';

/** Max size per PDF (e.g. annual reports with images). Kept in sync with multipart limit in app.ts. */
export const PDF_MAX_BYTES = 400 * 1024 * 1024; // 400 MB per file
const PDF_MIME = 'application/pdf';

let client: S3Client | null = null;

function getTrimmedEnv(name: string): string | undefined {
  const value = process.env[name];
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function getS3Endpoint(): string | undefined {
  return getTrimmedEnv('S3_ENDPOINT');
}

function getClient(): S3Client {
  if (!client) {
    const { region } = getS3Config();
    const endpoint = getS3Endpoint();
    const accessKeyId = process.env.S3_ACCESS_KEY_ID;
    const secretAccessKey = process.env.S3_SECRET_ACCESS_KEY;
    const sessionToken = process.env.S3_SESSION_TOKEN;

    client = new S3Client({
      region,
      ...(endpoint ? { endpoint, forcePathStyle: true } : {}),
      ...(accessKeyId && secretAccessKey
        ? {
            credentials: {
              accessKeyId,
              secretAccessKey,
              ...(sessionToken ? { sessionToken } : {}),
            },
          }
        : {}),
    });
  }
  return client;
}

export type PdfUploadResult = {
  publicUrl: string;
  bucket: string;
  key: string;
  sha256: string;
  /** True if the object already existed and we reused it. */
  reusedExisting: boolean;
  /** True if we performed a PutObject in this request. */
  uploaded: boolean;
};

function getUploadEnv(): string {
  const nodeEnv = getTrimmedEnv('NODE_ENV');
  if (nodeEnv === 'production') return 'prod';
  if (nodeEnv === 'staging') return 'stage';
  return 'dev';
}

function getPublicUrlForKey(params: { bucket: string; key: string }): string {
  const { bucket, key } = params;
  const publicBase = getTrimmedEnv('S3_PUBLIC_BASE_URL');
  const endpoint = getS3Endpoint();

  if (publicBase) {
    return `${publicBase.replace(/\/$/, '')}/${key}`;
  }
  if (endpoint) {
    const base = endpoint.replace(/\/$/, '');
    return `${base}/${bucket}/${key}`;
  }
  throw new Error(
    'Public URL is not configured. Set S3_PUBLIC_BASE_URL (e.g. https://storage.googleapis.com/<bucket>) or S3_ENDPOINT (e.g. https://storage.googleapis.com) so uploaded PDFs can be accessed later.'
  );
}

function sha256Hex(buffer: Buffer): string {
  return createHash('sha256').update(buffer).digest('hex');
}

async function objectExists(params: { bucket: string; key: string }): Promise<boolean> {
  const s3 = getClient();
  try {
    await s3.send(
      new HeadObjectCommand({
        Bucket: params.bucket,
        Key: params.key,
      })
    );
    return true;
  } catch (err: any) {
    // AWS SDK v3 throws for 404/NotFound; treat those as "does not exist"
    const httpStatus = err?.$metadata?.httpStatusCode;
    const name = err?.name;
    if (httpStatus === 404 || name === 'NotFound' || name === 'NoSuchKey') return false;
    throw err;
  }
}

/**
 * Upload PDF buffer to object storage and return a stable public URL.
 * Key format: uploads/{env}/{sha256}.pdf
 *
 * Semantics:
 * - If an object with the same sha256 already exists in this env, we reuse it and return its URL.
 * - Otherwise we upload and return the new URL.
 */
export async function uploadPdfAndGetUrls(
  buffer: Buffer,
  filename?: string
): Promise<PdfUploadResult> {
  if (buffer.length > PDF_MAX_BYTES) {
    throw new Error(`PDF too large (max ${PDF_MAX_BYTES / 1024 / 1024} MB)`);
  }

  const { bucket } = getS3Config();
  const env = getUploadEnv();
  const sha256 = sha256Hex(buffer);
  const key = `uploads/${env}/${sha256}.pdf`;

  const existed = await objectExists({ bucket, key });
  if (!existed) {
    const s3 = getClient();
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: buffer,
        ContentType: PDF_MIME,
        // Keep a small trace for humans; not used for dedupe logic.
        Metadata: filename ? { originalFilename: filename } : undefined,
      })
    );
  }

  const publicUrl = getPublicUrlForKey({ bucket, key });
  return {
    publicUrl,
    bucket,
    key,
    sha256,
    reusedExisting: existed,
    uploaded: !existed,
  };
}

/**
 * Back-compat helper: existing callers expect a single URL string.
 * Returns the stable public URL.
 */
export async function uploadPdfAndGetUrl(buffer: Buffer, filename?: string): Promise<string> {
  const { publicUrl } = await uploadPdfAndGetUrls(buffer, filename);
  return publicUrl;
}
