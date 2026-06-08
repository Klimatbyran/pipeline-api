/** Parse a 4-digit catalog year from a report URL path or filename (fallback only). */
export function parseReportYearFromUrl(
  raw: string | null | undefined,
): string | null {
  if (typeof raw !== "string" || !raw.trim()) return null;
  const matches = raw.match(/(?:19|20)\d{2}/g);
  if (!matches?.length) return null;
  const years = matches
    .map((token) => Number(token))
    .filter((year) => year >= 2000 && year <= 2030);
  if (!years.length) return null;
  return String(Math.max(...years));
}

/**
 * URL-derived year for queue UI only. Garbo save uses job.data.documentReportYear
 * as a trusted pipeline field (parse/prompt/operator), so we must not set that
 * from the URL at enqueue — save falls back to URL only when periods lack years.
 */
export function withUrlReportYearForDisplay(
  data: Record<string, unknown>,
  sourceUrl: string | null | undefined,
): Record<string, unknown> {
  const yearFromUrl = parseReportYearFromUrl(sourceUrl);
  if (!yearFromUrl) return data;
  return {
    ...data,
    reportYear: Number(yearFromUrl),
  };
}
