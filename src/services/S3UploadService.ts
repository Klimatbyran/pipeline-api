import { PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
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
    client = new S3Client({
      region,
      ...(endpoint ? { endpoint, forcePathStyle: true } : {}),
    });
  }
  return client;
}

/**
 * Upload PDF buffer to S3 and return a presigned GET URL so the worker can fetch it.
 * Key format: uploads/YYYY-MM-DD/{uuid}.pdf
 */
export async function uploadPdfAndGetUrl(buffer: Buffer, filename?: string): Promise<string> {
  if (buffer.length > PDF_MAX_BYTES) {
    throw new Error(`PDF too large (max ${PDF_MAX_BYTES / 1024 / 1024} MB)`);
  }

  const { bucket, presignedExpirySeconds } = getS3Config();
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

  const getCmd = new GetObjectCommand({ Bucket: bucket, Key: key });
  const url = await getSignedUrl(s3, getCmd, { expiresIn: presignedExpirySeconds });
  return url;
}
