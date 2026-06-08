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

/** PDF year to attach to parsePdf job data when enqueueing from a source URL. */
export function documentReportYearFromSourceUrl(
  sourceUrl: string | null | undefined,
): string | undefined {
  const year = parseReportYearFromUrl(sourceUrl);
  return year ?? undefined;
}

export function withDocumentReportYearFields(
  data: Record<string, unknown>,
  sourceUrl: string | null | undefined,
): Record<string, unknown> {
  const documentReportYear = documentReportYearFromSourceUrl(sourceUrl);
  if (!documentReportYear) return data;
  return {
    ...data,
    documentReportYear,
    reportYear: Number(documentReportYear),
  };
}
