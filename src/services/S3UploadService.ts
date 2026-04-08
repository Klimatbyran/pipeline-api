import { PutObjectCommand } from '@aws-sdk/client-s3';
import { S3Client } from '@aws-sdk/client-s3';
import { getS3Config } from '../config/s3';
import { randomUUID } from 'crypto';

/** Max size per PDF (e.g. annual reports with images). Kept in sync with multipart limit in app.ts. */
export const PDF_MAX_BYTES = 400 * 1024 * 1024; // 400 MB per file
const PDF_MIME = 'application/pdf';

let client: S3Client | null = null;

function getClient(): S3Client {
  if (!client) {
    const { region } = getS3Config();
    const endpoint = process.env.S3_ENDPOINT;
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

/**
 * Upload PDF buffer to object storage and return a stable public URL.
 * Key format: uploads/YYYY-MM-DD/{uuid}.pdf
 */
export async function uploadPdfAndGetUrls(
  buffer: Buffer,
  filename?: string
): Promise<{ publicUrl: string; bucket: string; key: string }> {
  if (buffer.length > PDF_MAX_BYTES) {
    throw new Error(`PDF too large (max ${PDF_MAX_BYTES / 1024 / 1024} MB)`);
  }

  const { bucket } = getS3Config();
  const date = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const ext = filename?.toLowerCase().endsWith('.pdf') ? '' : '.pdf';
  const key = `uploads/${date}/${randomUUID()}${ext}`;

  const s3 = getClient();
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: buffer,
      ContentType: PDF_MIME,
    })
  );

  const publicBase = process.env.S3_PUBLIC_BASE_URL?.trim();
  const endpoint = process.env.S3_ENDPOINT?.trim();

  let publicUrl: string | undefined;
  if (publicBase) {
    publicUrl = `${publicBase.replace(/\/$/, '')}/${key}`;
  } else if (endpoint && endpoint.includes('storage.googleapis.com')) {
    publicUrl = `https://storage.googleapis.com/${bucket}/${key}`;
  }

  if (!publicUrl) {
    throw new Error(
      'Public URL is not configured. Set S3_PUBLIC_BASE_URL (e.g. https://storage.googleapis.com/<bucket>) so uploaded PDFs can be accessed later.'
    );
  }

  return { publicUrl, bucket, key };
}

/**
 * Back-compat helper: existing callers expect a single URL string.
 * Returns the stable public URL.
 */
export async function uploadPdfAndGetUrl(buffer: Buffer, filename?: string): Promise<string> {
  const { publicUrl } = await uploadPdfAndGetUrls(buffer, filename);
  return publicUrl;
}
