const TERMINAL_STATUSES = new Set(["succeeded", "partial", "failed", "cancelled", "expired"]);

function normalizeNumber(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return parsed;
}

function normalizeRequest(request = {}) {
  const requestId = typeof request.request_id === "string" ? request.request_id.trim() : "";
  if (!requestId) {
    return null;
  }
  const status = typeof request.status === "string" && request.status
    ? request.status.trim().toLowerCase()
    : "processing";
  const requested = Math.max(0, normalizeNumber(request.requested, 0));
  const granted = Math.max(0, normalizeNumber(request.granted, 0));
  const remaining = Math.max(0, normalizeNumber(request.remaining, Math.max(0, requested - granted)));
  const terminal = Boolean(request.terminal || TERMINAL_STATUSES.has(status));
  const queued = Boolean(request.queued || status === "queued");
  return {
    request_id: requestId,
    status,
    requested,
    granted,
    remaining,
    queued,
    queue_id: normalizeNumber(request.queue_id, 0),
    queue_position: normalizeNumber(request.queue_position, 0),
    queue_total: normalizeNumber(request.queue_total, 0),
    items: Array.isArray(request.items) ? request.items.slice() : [],
    source: typeof request.source === "string" && request.source ? request.source.trim() : "self",
    origin_tab_id: typeof request.origin_tab_id === "string" ? request.origin_tab_id.trim() : "",
    reason_code: typeof request.reason_code === "string" ? request.reason_code.trim() : "",
    reason_message: typeof request.reason_message === "string" ? request.reason_message.trim() : "",
    updated_at: typeof request.updated_at === "string" ? request.updated_at : "",
    updated_at_ts: normalizeNumber(request.updated_at_ts, 0),
    terminal,
  };
}

function sortRequests(requests = []) {
  return requests.slice().sort((left, right) => {
    if (left.terminal !== right.terminal) {
      return left.terminal ? 1 : -1;
    }
    if (left.updated_at_ts !== right.updated_at_ts) {
      return right.updated_at_ts - left.updated_at_ts;
    }
    return right.request_id.localeCompare(left.request_id);
  });
}

function selectActiveRequestId(requests, preferredRequestId, context = {}) {
  const preferred = typeof preferredRequestId === "string" ? preferredRequestId.trim() : "";
  if (preferred && requests.some((request) => request.request_id === preferred)) {
    return preferred;
  }

  const ownActive = requests.find((request) => {
    if (request.terminal) {
      return false;
    }
    if (request.origin_tab_id && context.tabId) {
      return request.origin_tab_id === context.tabId;
    }
    return request.source !== "other_session";
  });
  if (ownActive) {
    return ownActive.request_id;
  }

  if (context.isSubmitting) {
    const ownRecent = requests.find((request) => {
      if (request.origin_tab_id && context.tabId) {
        return request.origin_tab_id === context.tabId;
      }
      return request.source !== "other_session";
    });
    if (ownRecent) {
      return ownRecent.request_id;
    }
  }

  return "";
}

function buildToastEffect(request) {
  const granted = Math.max(0, Number(request.granted) || 0);
  switch (request.status) {
    case "succeeded":
      if (request.source === "other_session") {
        return {
          type: "toast",
          requestId: request.request_id,
          title: "账号已到账",
          message: `共 ${granted} 个账号，来自其他会话。`,
        };
      }
      return {
        type: "toast",
        requestId: request.request_id,
        title: "申请成功",
        message: `共 ${granted} 个账号`,
      };
    case "partial":
      return {
        type: "toast",
        requestId: request.request_id,
        title: request.source === "other_session" ? "其他会话部分到账" : "申请部分完成",
        message: request.reason_message || `已到账 ${granted} 个账号，仍有 ${request.remaining} 个未完成。`,
      };
    case "failed":
      return {
        type: "toast",
        requestId: request.request_id,
        title: "申请失败",
        message: request.reason_message || "领取请求失败。",
      };
    case "cancelled":
      return {
        type: "toast",
        requestId: request.request_id,
        title: "排队已取消",
        message: request.reason_message || "领取请求已取消。",
      };
    case "expired":
      return {
        type: "toast",
        requestId: request.request_id,
        title: "排队已过期",
        message: request.reason_message || "领取请求已过期。",
      };
    default:
      return null;
  }
}

function shouldEmitToast(request, context = {}) {
  if (!context.emitToasts || !request.terminal) {
    return false;
  }
  if (request.source === "other_session") {
    return true;
  }
  if (request.origin_tab_id && context.tabId) {
    return request.origin_tab_id === context.tabId;
  }
  return request.request_id === context.activeRequestId;
}

export function createInitialClaimRealtimeState() {
  return {
    requestsById: {},
    toastKeys: {},
    activeRequestId: "",
  };
}

export function buildQueueStatusFromRequest(request) {
  if (!request || request.status !== "queued") {
    return { queued: false };
  }
  return {
    queued: true,
    queue_id: request.queue_id || 0,
    position: request.queue_position || 0,
    total_queued: request.queue_total || 0,
    requested: request.requested || 0,
    remaining: request.remaining || 0,
    request_id: request.request_id,
  };
}

export function applyClaimAcceptedState(previousState, accepted = {}, context = {}) {
  const requestId = typeof accepted.request_id === "string" ? accepted.request_id.trim() : "";
  if (!requestId) {
    return {
      state: previousState || createInitialClaimRealtimeState(),
      requests: [],
      activeRequest: null,
      queueRequest: null,
      effects: [],
    };
  }

  const prior = previousState || createInitialClaimRealtimeState();
  const requestsById = { ...(prior.requestsById || {}) };
  requestsById[requestId] = normalizeRequest({
    request_id: requestId,
    status: accepted.status || (accepted.queued ? "queued" : "processing"),
    requested: accepted.requested,
    remaining: accepted.queue_remaining ?? accepted.requested,
    queued: accepted.queued,
    queue_id: accepted.queue_id,
    queue_position: accepted.queue_position,
    queue_total: accepted.queue_position,
    source: "self",
    origin_tab_id: context.tabId || "",
    terminal: false,
  });
  const requests = sortRequests(Object.values(requestsById).filter(Boolean));
  const activeRequest = requests.find((request) => request.request_id === requestId) || null;
  const queueRequest = activeRequest?.status === "queued"
    ? activeRequest
    : requests.find((request) => request.status === "queued") || null;
  return {
    state: {
      requestsById,
      toastKeys: { ...(prior.toastKeys || {}) },
      activeRequestId: requestId,
    },
    requests,
    activeRequest,
    queueRequest,
    effects: [],
  };
}

export function applyClaimSnapshotState(previousState, snapshot = {}, context = {}) {
  const prior = previousState || createInitialClaimRealtimeState();
  const normalizedRequests = sortRequests(
    (Array.isArray(snapshot?.requests) ? snapshot.requests : [])
      .map((request) => normalizeRequest(request))
      .filter(Boolean)
  );

  const requestsById = {};
  normalizedRequests.forEach((request) => {
    requestsById[request.request_id] = request;
  });

  const nextActiveRequestId = selectActiveRequestId(
    normalizedRequests,
    context.activeRequestId || prior.activeRequestId,
    context
  );
  const activeRequest = nextActiveRequestId
    ? normalizedRequests.find((request) => request.request_id === nextActiveRequestId) || null
    : null;
  const queueRequest = activeRequest?.status === "queued"
    ? activeRequest
    : normalizedRequests.find((request) => request.status === "queued") || null;

  const toastKeys = { ...(prior.toastKeys || {}) };
  const effects = [];
  normalizedRequests.forEach((request) => {
    const previous = prior.requestsById?.[request.request_id];
    const justReachedTerminal = request.terminal && (!previous || !previous.terminal || previous.status !== request.status);
    if (!justReachedTerminal) {
      return;
    }
    effects.push({
      type: "terminal",
      requestId: request.request_id,
      status: request.status,
    });
    const toastKey = `${request.request_id}:${request.status}`;
    if (toastKeys[toastKey] || !shouldEmitToast(request, { ...context, activeRequestId: nextActiveRequestId })) {
      return;
    }
    const toast = buildToastEffect(request);
    if (!toast) {
      return;
    }
    toastKeys[toastKey] = true;
    effects.push(toast);
  });

  return {
    state: {
      requestsById,
      toastKeys,
      activeRequestId: nextActiveRequestId,
    },
    requests: normalizedRequests,
    activeRequest,
    queueRequest,
    effects,
  };
}
