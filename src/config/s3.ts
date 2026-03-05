/**
 * S3 configuration for PDF upload (parsePdf/upload endpoint).
 * Only required when using POST /api/queues/parsePdf/upload.
 * Credentials: set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or use IAM role (e.g. in Kubernetes).
 */
export function getS3Config(): {
  bucket: string;
  region: string;
  presignedExpirySeconds: number;
} {
  const bucket = process.env.S3_BUCKET;
  const region = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? 'eu-north-1';
  const presignedExpirySeconds = Math.min(
    Math.max(parseInt(process.env.S3_PRESIGNED_EXPIRY_SECONDS ?? '86400', 10) || 86400, 60),
    604800
  ); // 1 min to 7 days, default 24h

  if (!bucket?.trim()) {
    throw new Error(
      'S3 upload is not configured: set S3_BUCKET (and optionally AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) in the environment.'
    );
  }

  return { bucket: bucket.trim(), region, presignedExpirySeconds };
}

export function isS3Configured(): boolean {
  return Boolean(process.env.S3_BUCKET?.trim());
}
