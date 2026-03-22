export function createSummarySyncController(deps) {
  const state = {
    summaryVersionTermTs: null,
    runtimeBroadcastSeq: 0,
    dashboardBroadcastSeq: 0,
    summaryRequestSeq: 0,
    localRequests: new Map(),
    inboundRequests: new Map(),
    summaryFlushPromise: null,
    lastAppliedRuntimeVersion: null,
    lastAppliedDashboardVersion: null,
  };

  function hasCrossTabCoordination() {
    return Boolean(deps.hasCrossTabCoordination?.());
  }

  function isLeaderTab() {
    return Boolean(deps.isLeaderTab?.());
  }

  function canPerformPrimaryRefreshWork() {
    return Boolean(deps.canPerformPrimaryRefreshWork?.());
  }

  function getTabId() {
    const tabId = deps.getTabId?.();
    return typeof tabId === "string" ? tabId : "";
  }

  function getCurrentLeader() {
    return deps.getCurrentLeader?.() || null;
  }

  function normalizeSummaryVersion(version) {
    const termTs = Number(version?.term_ts ?? version?.termTs);
    const seq = Number(version?.seq);
    if (!Number.isFinite(termTs) || !Number.isFinite(seq)) {
      return null;
    }
    return { termTs, seq };
  }

  function cloneSummaryVersion(version) {
    const normalized = normalizeSummaryVersion(version);
    return normalized ? { termTs: normalized.termTs, seq: normalized.seq } : null;
  }

  function compareSummaryVersions(left, right) {
    const normalizedLeft = normalizeSummaryVersion(left);
    const normalizedRight = normalizeSummaryVersion(right);
    if (!normalizedLeft && !normalizedRight) {
      return 0;
    }
    if (!normalizedLeft) {
      return -1;
    }
    if (!normalizedRight) {
      return 1;
    }
    if (normalizedLeft.termTs !== normalizedRight.termTs) {
      return normalizedLeft.termTs - normalizedRight.termTs;
    }
    return normalizedLeft.seq - normalizedRight.seq;
  }

  function buildSummaryVersionPayload(version) {
    const normalized = normalizeSummaryVersion(version);
    if (!normalized) {
      return null;
    }
    return {
      term_ts: normalized.termTs,
      seq: normalized.seq,
    };
  }

  function createSummaryRequestId() {
    state.summaryRequestSeq += 1;
    return `${getTabId()}:summary:${Date.now()}:${state.summaryRequestSeq}`;
  }

  function createSummaryRefreshError(reason = "reset", cause = null) {
    const error = new Error(`摘要刷新未完成：${reason}`);
    error.code = "SUMMARY_REFRESH_ABORTED";
    if (cause !== null) {
      error.cause = cause;
    }
    return error;
  }

  function normalizeSummaryRefreshRequest(request = {}) {
    const requestId = typeof request?.requestId === "string" && request.requestId
      ? request.requestId
      : (typeof request?.request_id === "string" && request.request_id
          ? request.request_id
          : createSummaryRequestId());
    const requestedAt = Number(request?.requestedAt ?? request?.requested_at);
    const originTabId = typeof request?.originTabId === "string" && request.originTabId
      ? request.originTabId
      : (typeof request?.origin_tab_id === "string" && request.origin_tab_id
          ? request.origin_tab_id
          : getTabId());
    const includeDashboard = request?.needDashboard ?? request?.need_dashboard ?? request?.include_dashboard;
    const needRuntime = request?.needRuntime ?? request?.need_runtime;
    return {
      requestId,
      requestedAt: Number.isFinite(requestedAt) ? requestedAt : Date.now(),
      originTabId,
      reason: typeof request?.reason === "string" && request.reason ? request.reason : "summary-sync",
      needRuntime: needRuntime !== false,
      needDashboard: includeDashboard !== false,
    };
  }

  function compareSummaryRefreshRequests(left, right) {
    const normalizedLeft = normalizeSummaryRefreshRequest(left);
    const normalizedRight = normalizeSummaryRefreshRequest(right);
    if (normalizedLeft.requestedAt !== normalizedRight.requestedAt) {
      return normalizedLeft.requestedAt - normalizedRight.requestedAt;
    }
    return String(normalizedLeft.requestId || "").localeCompare(String(normalizedRight.requestId || ""));
  }

  function normalizeRequestIds(requestIds) {
    const source = Array.isArray(requestIds)
      ? requestIds
      : (typeof requestIds === "string" && requestIds ? [requestIds] : []);
    const seen = new Set();
    const normalized = [];
    source.forEach((requestId) => {
      if (typeof requestId !== "string" || !requestId || seen.has(requestId)) {
        return;
      }
      seen.add(requestId);
      normalized.push(requestId);
    });
    return normalized;
  }

  function buildRequestMetadata(requestIds) {
    const normalized = normalizeRequestIds(requestIds);
    if (!normalized.length) {
      return {};
    }
    return {
      request_id: normalized[0],
      request_ids: normalized,
    };
  }

  function extractRequestIds(payload = {}) {
    const requestIds = normalizeRequestIds(payload?.request_ids);
    if (requestIds.length) {
      return requestIds;
    }
    return normalizeRequestIds(payload?.request_id);
  }

  function getCurrentLeaderTermTs() {
    const leaderTermTs = Number(getCurrentLeader()?.termTs);
    return Number.isFinite(leaderTermTs) ? leaderTermTs : null;
  }

  function ensureSummaryBroadcastState() {
    const leaderTermTs = getCurrentLeaderTermTs();
    if (!leaderTermTs) {
      return null;
    }
    if (state.summaryVersionTermTs !== leaderTermTs) {
      state.summaryVersionTermTs = leaderTermTs;
      state.runtimeBroadcastSeq = 0;
      state.dashboardBroadcastSeq = 0;
    }
    return leaderTermTs;
  }

  function nextSummaryBroadcastVersion(kind) {
    if (!hasCrossTabCoordination() || !isLeaderTab()) {
      return null;
    }
    const leaderTermTs = ensureSummaryBroadcastState();
    if (!leaderTermTs) {
      return null;
    }
    if (kind === "runtime") {
      state.runtimeBroadcastSeq += 1;
      return { termTs: leaderTermTs, seq: state.runtimeBroadcastSeq };
    }
    state.dashboardBroadcastSeq += 1;
    return { termTs: leaderTermTs, seq: state.dashboardBroadcastSeq };
  }

  function getRequestMap(local) {
    return local ? state.localRequests : state.inboundRequests;
  }

  function createSummaryRequestEntry(request = {}, options = {}) {
    const normalized = normalizeSummaryRefreshRequest(request);
    const local = Boolean(options.local);
    const entry = {
      local,
      requestId: normalized.requestId,
      requestedAt: normalized.requestedAt,
      originTabId: normalized.originTabId,
      reason: normalized.reason,
      status: "pending",
      needRuntime: normalized.needRuntime,
      needDashboard: normalized.needDashboard,
      runtimeDone: normalized.needRuntime === false,
      dashboardDone: normalized.needDashboard === false,
      promise: null,
      resolve: null,
      reject: null,
    };
    if (local) {
      let resolvePromise = null;
      let rejectPromise = null;
      entry.promise = new Promise((resolve, reject) => {
        resolvePromise = resolve;
        rejectPromise = reject;
      });
      entry.promise.catch(() => {});
      entry.resolve = resolvePromise;
      entry.reject = rejectPromise;
    }
    return entry;
  }

  function isPendingSummaryRequestEntry(entry) {
    return Boolean(entry && entry.status === "pending");
  }

  function deleteSummaryRequestEntry(entry) {
    if (!entry?.requestId) {
      return;
    }
    getRequestMap(entry.local).delete(entry.requestId);
  }

  function maybeResolveSummaryRequestEntry(entry) {
    if (!isPendingSummaryRequestEntry(entry) || !entry.runtimeDone || !entry.dashboardDone) {
      return false;
    }
    entry.status = "resolved";
    deleteSummaryRequestEntry(entry);
    if (entry.local) {
      try {
        entry.resolve?.({
          requestId: entry.requestId,
          requestedAt: entry.requestedAt,
        });
      } catch (resolveError) {
        // Ignore deferred resolution failures.
      }
    }
    return true;
  }

  function rejectSummaryRequestEntry(entry, error) {
    if (!isPendingSummaryRequestEntry(entry)) {
      return false;
    }
    entry.status = "rejected";
    deleteSummaryRequestEntry(entry);
    if (entry.local) {
      try {
        entry.reject?.(error);
      } catch (rejectError) {
        // Ignore deferred rejection failures.
      }
    }
    return true;
  }

  function mergeSummaryRequestEntry(entry, request = {}) {
    const normalized = normalizeSummaryRefreshRequest(request);
    entry.requestedAt = Math.max(entry.requestedAt || 0, normalized.requestedAt || 0);
    entry.originTabId = normalized.originTabId || entry.originTabId;
    entry.reason = normalized.reason || entry.reason;
    if (normalized.needRuntime && !entry.needRuntime) {
      entry.needRuntime = true;
      entry.runtimeDone = false;
    }
    if (normalized.needDashboard && !entry.needDashboard) {
      entry.needDashboard = true;
      entry.dashboardDone = false;
    }
    maybeResolveSummaryRequestEntry(entry);
    return entry;
  }

  function upsertSummaryRequestEntry(request = {}, options = {}) {
    const normalized = normalizeSummaryRefreshRequest(request);
    const local = Boolean(options.local);
    const requestMap = getRequestMap(local);
    const existing = requestMap.get(normalized.requestId);
    if (!existing) {
      const entry = createSummaryRequestEntry(normalized, { local });
      requestMap.set(entry.requestId, entry);
      maybeResolveSummaryRequestEntry(entry);
      return entry;
    }
    return mergeSummaryRequestEntry(existing, normalized);
  }

  function buildSummaryRefreshRequest(reason = "summary-sync", includeDashboard = true) {
    return {
      requestId: createSummaryRequestId(),
      requestedAt: Date.now(),
      originTabId: getTabId(),
      reason,
      needRuntime: true,
      needDashboard: Boolean(includeDashboard),
    };
  }

  function buildSummaryRefreshRequestPayload(request = {}) {
    const normalized = normalizeSummaryRefreshRequest(request);
    return {
      request_id: normalized.requestId,
      requested_at: normalized.requestedAt,
      origin_tab_id: normalized.originTabId,
      reason: normalized.reason,
      need_runtime: normalized.needRuntime,
      include_dashboard: normalized.needDashboard,
    };
  }

  function collectPendingRequestEntries(options = {}) {
    const entries = Array.from(state.localRequests.values());
    if (options.includeInbound) {
      entries.push(...Array.from(state.inboundRequests.values()));
    }
    return entries
      .filter((entry) => isPendingSummaryRequestEntry(entry))
      .sort((left, right) => compareSummaryRefreshRequests(left, right));
  }

  function collectPendingRequestIds(kind, options = {}) {
    const seen = new Set();
    const requestIds = [];
    collectPendingRequestEntries(options).forEach((entry) => {
      const needed = kind === "runtime"
        ? entry.needRuntime && !entry.runtimeDone
        : entry.needDashboard && !entry.dashboardDone;
      if (!needed || seen.has(entry.requestId)) {
        return;
      }
      seen.add(entry.requestId);
      requestIds.push(entry.requestId);
    });
    return requestIds;
  }

  function markRequestIdsSettled(kind, requestIds) {
    const normalizedRequestIds = normalizeRequestIds(requestIds);
    if (!normalizedRequestIds.length) {
      return;
    }
    normalizedRequestIds.forEach((requestId) => {
      const localEntry = state.localRequests.get(requestId);
      const inboundEntry = state.inboundRequests.get(requestId);
      [localEntry, inboundEntry].forEach((entry) => {
        if (!isPendingSummaryRequestEntry(entry)) {
          return;
        }
        if (kind === "runtime" && entry.needRuntime) {
          entry.runtimeDone = true;
        }
        if (kind === "dashboard" && entry.needDashboard) {
          entry.dashboardDone = true;
        }
        maybeResolveSummaryRequestEntry(entry);
      });
    });
  }

  function rejectSummaryRequestsByIds(requestIds, reason, cause = null) {
    const normalizedRequestIds = normalizeRequestIds(requestIds);
    if (!normalizedRequestIds.length) {
      return;
    }
    const error = createSummaryRefreshError(reason, cause);
    normalizedRequestIds.forEach((requestId) => {
      const localEntry = state.localRequests.get(requestId);
      const inboundEntry = state.inboundRequests.get(requestId);
      if (localEntry) {
        rejectSummaryRequestEntry(localEntry, error);
      }
      if (inboundEntry) {
        deleteSummaryRequestEntry(inboundEntry);
      }
    });
  }

  function dropRemoteSummaryRequestEntries() {
    state.inboundRequests.clear();
  }

  function replayPendingLocalSummaryRequests() {
    if (!hasCrossTabCoordination()) {
      return;
    }
    collectPendingRequestEntries({ includeInbound: false }).forEach((entry) => {
      deps.broadcastMessage?.("summary_refresh_requested", buildSummaryRefreshRequestPayload(entry));
    });
  }

  function applyRuntimeSnapshotEnvelope(payload, options = {}) {
    const envelope = payload && (payload.snapshot || payload.version || payload.request_id || payload.request_ids)
      ? payload
      : { snapshot: payload || {} };
    const version = normalizeSummaryVersion(envelope?.version);
    if (options.requireVersion && !version) {
      return false;
    }
    const currentLeaderTermTs = getCurrentLeaderTermTs();
    if (version && currentLeaderTermTs != null && version.termTs < currentLeaderTermTs) {
      return false;
    }
    if (version && compareSummaryVersions(version, state.lastAppliedRuntimeVersion) <= 0) {
      return false;
    }
    deps.applyRuntimeSnapshot?.(envelope?.snapshot || {});
    if (version) {
      state.lastAppliedRuntimeVersion = cloneSummaryVersion(version);
    }
    markRequestIdsSettled("runtime", extractRequestIds(envelope));
    return true;
  }

  function applyDashboardSummaryEnvelope(payload, options = {}) {
    const envelope = payload && (payload.summary || payload.version || payload.request_id || payload.request_ids)
      ? payload
      : { summary: payload || {} };
    const version = normalizeSummaryVersion(envelope?.version);
    if (options.requireVersion && !version) {
      return false;
    }
    const currentLeaderTermTs = getCurrentLeaderTermTs();
    if (version && currentLeaderTermTs != null && version.termTs < currentLeaderTermTs) {
      return false;
    }
    if (version && compareSummaryVersions(version, state.lastAppliedDashboardVersion) <= 0) {
      return false;
    }
    deps.applyDashboardSummary?.(envelope?.summary || {});
    if (version) {
      state.lastAppliedDashboardVersion = cloneSummaryVersion(version);
    }
    markRequestIdsSettled("dashboard", extractRequestIds(envelope));
    return true;
  }

  function resolveEnvelopeRequestIds(kind, options = {}) {
    const explicitRequestIds = normalizeRequestIds(options.requestIds ?? options.request_ids);
    if (explicitRequestIds.length) {
      return explicitRequestIds;
    }
    if (options.attachPendingRequestIds === false) {
      return [];
    }
    return collectPendingRequestIds(kind, {
      includeInbound: hasCrossTabCoordination() && isLeaderTab(),
    });
  }

  function buildRuntimeSnapshotFromBootstrap(payload = {}) {
    const profile = payload?.profile || {};
    return {
      quota: profile?.quota || null,
      claims: profile?.claims || {},
      api_keys: {
        summary: profile?.api_keys || {},
      },
      upload_results: payload?.upload_results || {},
    };
  }

  async function loadRuntimeSnapshot(options = {}) {
    const shouldBroadcast = Boolean(
      options.broadcast &&
      hasCrossTabCoordination() &&
      isLeaderTab()
    );
    const requestIds = resolveEnvelopeRequestIds("runtime", options);
    const version = shouldBroadcast
      ? buildSummaryVersionPayload(options.version ?? nextSummaryBroadcastVersion("runtime"))
      : null;
    const payload = await deps.fetchJson?.("/me/runtime-snapshot");
    if (shouldBroadcast || requestIds.length || version) {
      const envelope = {
        snapshot: payload || {},
        ...buildRequestMetadata(requestIds),
      };
      if (version) {
        envelope.version = version;
      }
      applyRuntimeSnapshotEnvelope(envelope);
      if (shouldBroadcast && isLeaderTab()) {
        deps.broadcastMessage?.("runtime_snapshot", envelope);
      }
    } else {
      deps.applyRuntimeSnapshot?.(payload || {});
    }
    return payload;
  }

  async function loadDashboardSummary(options = {}) {
    const shouldBroadcast = Boolean(
      options.broadcast &&
      hasCrossTabCoordination() &&
      isLeaderTab()
    );
    const requestIds = resolveEnvelopeRequestIds("dashboard", options);
    const version = shouldBroadcast
      ? buildSummaryVersionPayload(options.version ?? nextSummaryBroadcastVersion("dashboard"))
      : null;
    const summary = await deps.fetchJson?.(
      "/dashboard/summary?window=7d&bucket=1h&leaderboard_window=24h&leaderboard_limit=10&recent_limit=10&contributor_limit=10&recent_contributor_limit=10"
    );
    if (shouldBroadcast || requestIds.length || version) {
      const envelope = {
        summary: summary || {},
        ...buildRequestMetadata(requestIds),
      };
      if (version) {
        envelope.version = version;
      }
      applyDashboardSummaryEnvelope(envelope);
      if (shouldBroadcast && isLeaderTab()) {
        deps.broadcastMessage?.("dashboard_summary", envelope);
      }
    } else {
      deps.applyDashboardSummary?.(summary || {});
    }
    return summary;
  }

  async function loadBootstrapBundle(options = {}) {
    const shouldBroadcast = Boolean(
      options.broadcast &&
      hasCrossTabCoordination() &&
      isLeaderTab()
    );
    const runtimeRequestIds = resolveEnvelopeRequestIds("runtime", {
      requestIds: options.runtimeRequestIds ?? options.requestIds,
      request_ids: options.runtime_request_ids,
      attachPendingRequestIds: options.attachPendingRequestIds,
    });
    const dashboardRequestIds = resolveEnvelopeRequestIds("dashboard", {
      requestIds: options.dashboardRequestIds ?? options.requestIds,
      request_ids: options.dashboard_request_ids,
      attachPendingRequestIds: options.attachPendingRequestIds,
    });
    const runtimeVersion = shouldBroadcast
      ? buildSummaryVersionPayload(options.runtimeVersion ?? options.version ?? nextSummaryBroadcastVersion("runtime"))
      : null;
    const dashboardVersion = shouldBroadcast
      ? buildSummaryVersionPayload(options.dashboardVersion ?? options.version ?? nextSummaryBroadcastVersion("dashboard"))
      : null;
    const bootstrap = await deps.fetchJson?.("/me/bootstrap");
    const runtimeSnapshot = buildRuntimeSnapshotFromBootstrap(bootstrap || {});
    const dashboardSummary = bootstrap?.dashboard || {};

    if (shouldBroadcast || runtimeRequestIds.length || runtimeVersion) {
      const runtimeEnvelope = {
        snapshot: runtimeSnapshot,
        ...buildRequestMetadata(runtimeRequestIds),
      };
      if (runtimeVersion) {
        runtimeEnvelope.version = runtimeVersion;
      }
      applyRuntimeSnapshotEnvelope(runtimeEnvelope);
      if (shouldBroadcast && isLeaderTab()) {
        deps.broadcastMessage?.("runtime_snapshot", runtimeEnvelope);
      }
    } else {
      deps.applyRuntimeSnapshot?.(runtimeSnapshot);
    }

    if (shouldBroadcast || dashboardRequestIds.length || dashboardVersion) {
      const dashboardEnvelope = {
        summary: dashboardSummary,
        ...buildRequestMetadata(dashboardRequestIds),
      };
      if (dashboardVersion) {
        dashboardEnvelope.version = dashboardVersion;
      }
      applyDashboardSummaryEnvelope(dashboardEnvelope);
      if (shouldBroadcast && isLeaderTab()) {
        deps.broadcastMessage?.("dashboard_summary", dashboardEnvelope);
      }
    } else {
      deps.applyDashboardSummary?.(dashboardSummary);
    }

    return bootstrap || {};
  }

  function buildSummaryRefreshBatch() {
    if (!canPerformPrimaryRefreshWork()) {
      return null;
    }
    const shouldBroadcast = hasCrossTabCoordination() && isLeaderTab();
    const runtimeRequestIds = collectPendingRequestIds("runtime", { includeInbound: shouldBroadcast });
    const dashboardRequestIds = collectPendingRequestIds("dashboard", { includeInbound: shouldBroadcast });
    if (!runtimeRequestIds.length && !dashboardRequestIds.length) {
      return null;
    }
    return {
      shouldBroadcast,
      runtimeRequestIds,
      dashboardRequestIds,
      runtimeVersion: shouldBroadcast && runtimeRequestIds.length
        ? nextSummaryBroadcastVersion("runtime")
        : null,
      dashboardVersion: shouldBroadcast && dashboardRequestIds.length
        ? nextSummaryBroadcastVersion("dashboard")
        : null,
    };
  }

  function scheduleNextFlush() {
    Promise.resolve().then(() => {
      if (!canPerformPrimaryRefreshWork()) {
        return;
      }
      flushSummaryRefreshRequests().catch((error) => {
        if (!deps.handleAccessError?.(error)) {
          console.error("刷新摘要失败", error);
        }
      });
    });
  }

  async function flushSummaryRefreshRequests() {
    if (!canPerformPrimaryRefreshWork()) {
      return null;
    }
    if (state.summaryFlushPromise) {
      return state.summaryFlushPromise;
    }
    const batch = buildSummaryRefreshBatch();
    if (!batch) {
      return null;
    }
    state.summaryFlushPromise = (async () => {
      const tasks = [];
      if (batch.runtimeRequestIds.length && batch.dashboardRequestIds.length) {
        tasks.push({
          kind: "bootstrap",
          requestIds: Array.from(new Set([
            ...batch.runtimeRequestIds,
            ...batch.dashboardRequestIds,
          ])),
          promise: loadBootstrapBundle({
            broadcast: batch.shouldBroadcast,
            runtimeVersion: batch.runtimeVersion,
            dashboardVersion: batch.dashboardVersion,
            runtimeRequestIds: batch.runtimeRequestIds,
            dashboardRequestIds: batch.dashboardRequestIds,
            attachPendingRequestIds: false,
          }),
        });
      } else if (batch.runtimeRequestIds.length) {
        tasks.push({
          kind: "runtime",
          requestIds: batch.runtimeRequestIds,
          promise: loadRuntimeSnapshot({
            broadcast: batch.shouldBroadcast,
            version: batch.runtimeVersion,
            requestIds: batch.runtimeRequestIds,
            attachPendingRequestIds: false,
          }),
        });
      }
      if (batch.dashboardRequestIds.length) {
        tasks.push({
          kind: "dashboard",
          requestIds: batch.dashboardRequestIds,
          promise: loadDashboardSummary({
            broadcast: batch.shouldBroadcast,
            version: batch.dashboardVersion,
            requestIds: batch.dashboardRequestIds,
            attachPendingRequestIds: false,
          }),
        });
      }
      if (!tasks.length) {
        return [];
      }
      const results = await Promise.allSettled(tasks.map((task) => task.promise));
      results.forEach((result, index) => {
        if (result.status !== "rejected") {
          return;
        }
        const task = tasks[index];
        rejectSummaryRequestsByIds(
          task.requestIds,
          `${task.kind}-refresh-failed`,
          result.reason
        );
      });
      const firstFailure = results.find((result) => result.status === "rejected");
      if (firstFailure) {
        throw firstFailure.reason;
      }
      return results;
    })();
    try {
      return await state.summaryFlushPromise;
    } finally {
      state.summaryFlushPromise = null;
      if (canPerformPrimaryRefreshWork()) {
        scheduleNextFlush();
      }
    }
  }

  function triggerSummaryRefreshFlush() {
    if (!canPerformPrimaryRefreshWork()) {
      return;
    }
    scheduleNextFlush();
  }

  function refreshSummariesByLeader(reason = "summary-sync", options = {}) {
    const includeDashboard = options.includeDashboard !== false;
    const requestLeader = options.requestLeader !== false;
    const entry = upsertSummaryRequestEntry(
      buildSummaryRefreshRequest(reason, includeDashboard),
      { local: true }
    );
    if (hasCrossTabCoordination() && !isLeaderTab()) {
      deps.broadcastMessage?.("summary_refresh_requested", buildSummaryRefreshRequestPayload(entry));
    }
    if (canPerformPrimaryRefreshWork()) {
      triggerSummaryRefreshFlush();
    } else if (requestLeader && hasCrossTabCoordination()) {
      deps.requestLeadership?.(false, { reason });
    }
    return entry.promise;
  }

  function handleSummaryRefreshRequested(payload = {}, message = {}) {
    const request = normalizeSummaryRefreshRequest({
      request_id: payload?.request_id,
      requested_at: payload?.requested_at,
      origin_tab_id: payload?.origin_tab_id || message.tabId,
      reason: typeof payload?.reason === "string" && payload.reason ? payload.reason : "summary-sync",
      need_runtime: payload?.need_runtime !== false,
      include_dashboard: payload?.include_dashboard !== false,
    });
    if (request.originTabId === getTabId()) {
      upsertSummaryRequestEntry(request, { local: true });
      return request.requestId;
    }
    if (!(hasCrossTabCoordination() && isLeaderTab())) {
      return null;
    }
    upsertSummaryRequestEntry(request, { local: false });
    triggerSummaryRefreshFlush();
    return request.requestId;
  }

  function handleLeaderChanged(change = {}) {
    const currentLeader = change.currentLeader || null;
    if (!currentLeader || currentLeader.tabId === getTabId()) {
      return;
    }
    dropRemoteSummaryRequestEntries();
    if (change.leaderChanged) {
      replayPendingLocalSummaryRequests();
    }
  }

  function handleLeaderStatusChange(change = {}) {
    if (change.isLeader) {
      triggerSummaryRefreshFlush();
      return;
    }
    dropRemoteSummaryRequestEntries();
  }

  function resetSessionState(reason = "reset") {
    const pendingEntries = Array.from(state.localRequests.values());
    if (pendingEntries.length) {
      const error = createSummaryRefreshError(reason);
      pendingEntries.forEach((entry) => {
        rejectSummaryRequestEntry(entry, error);
      });
    }
    state.inboundRequests.clear();
    state.summaryFlushPromise = null;
    state.summaryVersionTermTs = null;
    state.runtimeBroadcastSeq = 0;
    state.dashboardBroadcastSeq = 0;
    state.summaryRequestSeq = 0;
    state.lastAppliedRuntimeVersion = null;
    state.lastAppliedDashboardVersion = null;
  }

  function resetTransportState() {
    dropRemoteSummaryRequestEntries();
  }

  return {
    loadBootstrapBundle,
    loadRuntimeSnapshot,
    loadDashboardSummary,
    applyRuntimeSnapshotEnvelope,
    applyDashboardSummaryEnvelope,
    refreshSummariesByLeader,
    flushSummaryRefreshRequests,
    handleSummaryRefreshRequested,
    handleLeaderChanged,
    handleLeaderStatusChange,
    replayPendingLocalSummaryRequests,
    resetSessionState,
    resetTransportState,
  };
}
