/**
 * S3 configuration for PDF upload (parsePdf/upload endpoint).
 * Only required when using POST /api/queues/parsePdf/upload.
 * Credentials: set S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY,
 */
export function getS3Config(): {
  bucket: string;
  region: string;
} {
  const bucket = process.env.S3_BUCKET;
  // Many S3-compatible providers ignore region, but the AWS SDK still requires one.
  // Default matches our current AWS region convention.
  const region = process.env.S3_REGION ?? 'eu-north-1';

  if (!bucket?.trim()) {
    throw new Error(
      'S3 upload is not configured: set S3_BUCKET (and optionally S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY) in the environment.'
    );
  }

  return { bucket: bucket.trim(), region };
}

export function isS3Configured(): boolean {
  return Boolean(process.env.S3_BUCKET?.trim());
}
