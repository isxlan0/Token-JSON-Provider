export const CLAIM_DEBUG_QUERY_PARAM = "claim_debug";
export const CLAIM_DEBUG_STORAGE_KEY = "token_atlas_claim_debug";

const DEBUG_TRUE_VALUES = new Set(["1", "true", "on", "yes", "debug"]);
const DEBUG_FALSE_VALUES = new Set(["0", "false", "off", "no"]);

function normalizeText(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeOptionalNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function normalizeClaimDebugFlag(value) {
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = normalizeText(String(value ?? "")).toLowerCase();
  if (!normalized) {
    return null;
  }
  if (DEBUG_TRUE_VALUES.has(normalized)) {
    return true;
  }
  if (DEBUG_FALSE_VALUES.has(normalized)) {
    return false;
  }
  return null;
}

export function readClaimDebugFlagFromSearch(search = "") {
  const normalizedSearch = typeof search === "string" ? search : "";
  try {
    return normalizeClaimDebugFlag(new URLSearchParams(normalizedSearch).get(CLAIM_DEBUG_QUERY_PARAM));
  } catch (error) {
    return null;
  }
}

export function resolveServerClaimDebug(debugPayload) {
  if (!debugPayload || typeof debugPayload !== "object") {
    return null;
  }
  const traceEnabled = normalizeClaimDebugFlag(debugPayload.claim_trace);
  if (typeof traceEnabled === "boolean") {
    return traceEnabled;
  }
  const level = normalizeText(debugPayload.log_level).toLowerCase();
  if (!level) {
    return null;
  }
  return level === "debug";
}

export function resolveClaimDebugMode({
  search = "",
  storageValue = null,
  serverDebug = null,
  currentEnabled = false,
  currentSource = "",
} = {}) {
  const queryFlag = readClaimDebugFlagFromSearch(search);
  if (typeof queryFlag === "boolean") {
    return { enabled: queryFlag, source: "query" };
  }

  const storageFlag = normalizeClaimDebugFlag(storageValue);
  if (typeof storageFlag === "boolean") {
    return { enabled: storageFlag, source: "storage" };
  }

  if (typeof serverDebug === "boolean") {
    return { enabled: serverDebug, source: "server" };
  }

  if (normalizeText(currentSource)) {
    return { enabled: Boolean(currentEnabled), source: currentSource };
  }

  return { enabled: Boolean(currentEnabled), source: currentEnabled ? "current" : "default" };
}

export function summarizeClaimAcceptedPayload(payload = {}) {
  return {
    request_id: normalizeText(payload.request_id),
    status: normalizeText(payload.status).toLowerCase(),
    queued: Boolean(payload.queued),
    requested: normalizeOptionalNumber(payload.requested),
    granted: normalizeOptionalNumber(payload.granted),
    remaining: normalizeOptionalNumber(payload.remaining ?? payload.queue_remaining),
    queue_id: normalizeOptionalNumber(payload.queue_id),
    queue_position: normalizeOptionalNumber(payload.queue_position),
    queue_total: normalizeOptionalNumber(payload.queue_total),
    block_reason: normalizeText(payload.block_reason),
    terminal: Boolean(payload.terminal),
  };
}

export function summarizeClaimRealtimePayload(payload = {}) {
  const requests = Array.isArray(payload?.requests) ? payload.requests : [];
  return {
    request_count: requests.length,
    stream_required: Boolean(payload?.stream_required),
    transport: normalizeText(payload?.transport),
    requests: requests.slice(0, 5).map((request) => ({
      request_id: normalizeText(request?.request_id),
      status: normalizeText(request?.status).toLowerCase(),
      queued: Boolean(request?.queued),
      terminal: Boolean(request?.terminal),
      requested: normalizeOptionalNumber(request?.requested),
      granted: normalizeOptionalNumber(request?.granted),
      remaining: normalizeOptionalNumber(request?.remaining),
      queue_position: normalizeOptionalNumber(request?.queue_position),
      queue_total: normalizeOptionalNumber(request?.queue_total),
      block_reason: normalizeText(request?.block_reason),
      origin_tab_id: normalizeText(request?.origin_tab_id),
    })),
  };
}

export function summarizeQueueStatusPayload(payload = {}) {
  return {
    queued: Boolean(payload?.queued),
    status: normalizeText(payload?.status).toLowerCase(),
    request_id: normalizeText(payload?.request_id),
    queue_id: normalizeOptionalNumber(payload?.queue_id),
    position: normalizeOptionalNumber(payload?.position),
    total_queued: normalizeOptionalNumber(payload?.total_queued),
    requested: normalizeOptionalNumber(payload?.requested),
    remaining: normalizeOptionalNumber(payload?.remaining),
    block_reason: normalizeText(payload?.block_reason),
    front_blocked: Boolean(payload?.front_blocked),
    front_block_reason: normalizeText(payload?.front_block_reason),
    claimable_now_state: normalizeText(payload?.claimable_now_state),
  };
}
