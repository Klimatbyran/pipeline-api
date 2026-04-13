import dns from "node:dns/promises";
import net from "node:net";

/** Total time budget for the entire redirect chain + body download. */
export const PDF_FETCH_TIMEOUT_MS = 120_000;
export const PDF_FETCH_MAX_REDIRECTS = 5;

const BLOCKED_HOSTNAMES = new Set(
  [
    "localhost",
    "metadata.google.internal",
    "metadata",
    "metadata.gke.internal",
    "kubernetes.default",
    "kubernetes.default.svc",
  ].map((h) => h.toLowerCase()),
);

function isPrivateOrReservedIpv4(ip: string): boolean {
  const parts = ip.split(".").map((p) => Number(p));
  if (parts.length !== 4 || parts.some((p) => !Number.isInteger(p) || p < 0 || p > 255)) {
    return true;
  }
  const [a, b] = parts;
  if (a === 10) return true;
  if (a === 127) return true;
  if (a === 0) return true;
  if (a === 169 && parts[1] === 254) return true;
  if (a === 172 && b >= 16 && b <= 31) return true;
  if (a === 192 && b === 168) return true;
  if (a === 100 && b >= 64 && b <= 127) return true;
  if (a === 192 && b === 0 && parts[2] === 0) return true;
  return false;
}

function isBlockedIpv6(ip: string): boolean {
  const n = ip.toLowerCase();
  if (n === "::1") return true;
  if (n.startsWith("fe80:")) return true;
  if (n.startsWith("fc") || n.startsWith("fd")) return true;
  return false;
}

/**
 * Reject URLs that would let server-side fetch reach private networks or obvious SSRF targets.
 * Call again after each redirect target is resolved.
 */
export async function assertUrlSafeForServerFetch(urlString: string): Promise<void> {
  let url: URL;
  try {
    url = new URL(urlString);
  } catch {
    throw new Error("Invalid URL");
  }

  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("Only http(s) URLs are allowed");
  }

  if (url.username || url.password) {
    throw new Error("URL must not contain credentials");
  }

  const hostname = url.hostname;
  if (!hostname) {
    throw new Error("Missing hostname");
  }

  if (BLOCKED_HOSTNAMES.has(hostname.toLowerCase())) {
    throw new Error("Host is not allowed for server-side fetch");
  }

  if (net.isIPv4(hostname)) {
    if (isPrivateOrReservedIpv4(hostname)) {
      throw new Error("Private or reserved IPv4 addresses are not allowed");
    }
    return;
  }

  if (net.isIPv6(hostname)) {
    if (isBlockedIpv6(hostname)) {
      throw new Error("Private or reserved IPv6 addresses are not allowed");
    }
    return;
  }

  let records: import("node:dns").LookupAddress[];
  try {
    records = (await dns.lookup(hostname, { all: true })) as import("node:dns").LookupAddress[];
  } catch (e: any) {
    throw new Error(`DNS lookup failed: ${e?.message ?? "unknown error"}`);
  }

  if (!records.length) {
    throw new Error("DNS lookup returned no addresses");
  }

  for (const { address, family } of records) {
    if (family === 4) {
      if (isPrivateOrReservedIpv4(address)) {
        throw new Error("Resolved address is in a private or reserved IPv4 range");
      }
    } else if (family === 6) {
      if (isBlockedIpv6(address)) {
        throw new Error("Resolved address is in a private or reserved IPv6 range");
      }
    }
  }
}

export function assertBufferLooksLikePdf(buffer: Buffer): void {
  if (buffer.length < 4) {
    throw new Error("Response is too small to be a PDF");
  }
  const head = buffer.subarray(0, 5).toString("latin1");
  if (!head.startsWith("%PDF")) {
    throw new Error("Response is not a PDF document (missing %PDF header)");
  }
}
