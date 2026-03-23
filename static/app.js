import { createLeaderSyncController } from "./app/leader-sync.js?v=20260322f";
import {
  applyClaimAcceptedState,
  applyClaimSnapshotState,
  buildQueueStatusFromRequest,
  createInitialClaimRealtimeState,
} from "./app/claim-realtime.js?v=20260323b";
import { createSummarySyncController } from "./app/summary-sync.js?v=20260323i";

const APP_BUILD = (() => {
  try {
    return new URL(import.meta.url).searchParams.get("v") || "dev";
  } catch (error) {
    return "unknown";
  }
})();
const APP_ENTRY = "web-spa";

function createClientTabId() {
  const storageKey = "token_atlas_tab_id";
  try {
    if (typeof window !== "undefined" && window.sessionStorage) {
      const existing = window.sessionStorage.getItem(storageKey);
      if (existing && existing.trim()) {
        return existing.trim();
      }
      const created = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      window.sessionStorage.setItem(storageKey, created);
      return created;
    }
  } catch (error) {
    console.warn("读取标签页标识失败，改用临时标识", error);
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

const state = {
  user: null,
  authErrorParam: "",
  quota: null,
  stats: null,
  apiKeysState: {
    items: [],
    limit: 0,
    active: 0,
    loaded: false,
  },
  claimResults: [],
  claimSelected: new Set(),
  claimMultiMode: false,
  refreshing: false,
  queueStatus: null,
  queueSticky: false,
  leaderboard: [],
  recentClaims: [],
  contributorLeaderboard: [],
  recentContributors: [],
  trends: [],
  trendsMeta: null,
  systemStatus: null,
  lastClaimTotal: null,
  claimsInitialized: false,
  skipNextClaimModal: false,
  pendingHiddenClaimIds: new Set(),
  isClaimSubmitting: false,
  isClaimQueued: false,
  uploadPolicy: null,
  uploadQueue: [],
  uploadResults: [],
  uploadResultsSummary: null,
  uploadResultsLoaded: false,
  uploadHistory: [],
  isUploading: false,
  activeTab: "data",
  queueStream: null,
  claimRealtimeState: createInitialClaimRealtimeState(),
  activeClaimRequestId: "",
  claimStreamConnected: false,
  claimStreamRetryTimer: null,
  claimStreamRetryCount: 0,
  claimStreamError: "",
  uploadStream: null,
  uploadPollTimer: null,
  uploadPollFailureCount: 0,
  uploadResultsDirty: false,
  lowFrequencyTimer: null,
  claimsRequestTimer: null,
  claimsDirty: false,
  claimsLoadPromise: null,
  claimsLoadShouldBroadcast: false,
  visibilityHidden: document.visibilityState !== "visible",
  tabId: createClientTabId(),
  broadcastChannel: null,
  crossTabBusMode: "none",
  lastCrossTabMessageId: null,
  currentLeader: null,
  isPollingLeader: false,
  leaderHeartbeatTimer: null,
  leaderMonitorTimer: null,
  leaderProbeTimer: null,
  leaderRetryTimer: null,
  leaderCandidate: null,
  leaderCandidatePeers: new Map(),
  lastLowFrequencyAt: 0,
  bootstrapPromise: null,
  runtimeSnapshotMeta: null,
  dashboardMeta: null,
  dashboardSectionMeta: {},
};

const LOW_FREQUENCY_REFRESH_MS = 60000;
const CLAIM_STREAM_RETRY_BASE_MS = 1000;
const CLAIM_STREAM_RETRY_MAX_MS = 30000;
const QUEUE_POLL_MAX_BACKOFF_MS = 120000;
const LEADER_HEARTBEAT_MS = 10000;
const LEADER_STALE_MS = 30000;
const LEADER_PROBE_WAIT_MS = 400;
const CLAIMS_REFRESH_DEBOUNCE_MS = 100;
const LEADERSHIP_REASON_PRIORITY = Object.freeze({
  background: 0,
  visible: 0,
  manual: 1,
  "claim-view": 1,
  "claim-queue": 1,
  upload: 2,
});

const elements = {
  loadingScreen: document.getElementById("loading-screen"),
  loadingSpinner: document.getElementById("loading-spinner"),
  loadingTitle: document.getElementById("loading-title"),
  loadingSubtitle: document.getElementById("loading-subtitle"),
  loadingMessage: document.getElementById("loading-message"),
  loadingRetryBtn: document.getElementById("loading-retry-btn"),
  loginScreen: document.getElementById("login-screen"),
  appScreen: document.getElementById("app-screen"),
  loginBtn: document.getElementById("linuxdo-login-btn"),
  loginMessage: document.getElementById("login-message"),
  bannedScreen: document.getElementById("banned-screen"),
  bannedReason: document.getElementById("banned-reason"),
  bannedExpires: document.getElementById("banned-expires"),
  bannedAdminLink: document.getElementById("banned-admin-link"),
  bannedLogoutBtn: document.getElementById("banned-logout-btn"),
  logoutBtn: document.getElementById("logout-btn"),
  summaryOrigin: document.getElementById("summary-origin"),
  authMethod: document.getElementById("auth-method"),
  authUsername: document.getElementById("auth-username"),
  tabData: document.getElementById("tab-data"),
  tabKeys: document.getElementById("tab-keys"),
  tabClaim: document.getElementById("tab-claim"),
  tabUpload: document.getElementById("tab-upload"),
  tabDocs: document.getElementById("tab-docs"),
  viewData: document.getElementById("view-data"),
  viewKeys: document.getElementById("view-keys"),
  viewClaim: document.getElementById("view-claim"),
  viewUpload: document.getElementById("view-upload"),
  viewDocs: document.getElementById("view-docs"),
  userName: document.getElementById("user-name"),
  userUsername: document.getElementById("user-username"),
  userId: document.getElementById("user-id"),
  userTrust: document.getElementById("user-trust"),
  quotaUsed: document.getElementById("quota-used"),
  quotaRemaining: document.getElementById("quota-remaining"),
  quotaLimit: document.getElementById("quota-limit"),
  inventoryAvailable: document.getElementById("inventory-available"),
  inventoryTotal: document.getElementById("inventory-total"),
  claimsOthersTotal: document.getElementById("claims-others-total"),
  claimsOthersUnique: document.getElementById("claims-others-unique"),
  claimsTotal: document.getElementById("claims-total"),
  claimsUnique: document.getElementById("claims-unique"),
  myClaimsTotal: document.getElementById("my-claims-total"),
  inventoryHealthValue: document.getElementById("inventory-health-value"),
  inventoryHealthUnclaimed: document.getElementById("inventory-health-unclaimed"),
  inventoryHealthHourly: document.getElementById("inventory-health-hourly"),
  systemStatusValue: document.getElementById("system-status-value"),
  systemStatusQueue: document.getElementById("system-status-queue"),
  systemStatusIndex: document.getElementById("system-status-index"),
  leaderboardList: document.getElementById("leaderboard-list"),
  recentClaimsList: document.getElementById("recent-claims-list"),
  contributorLeaderboardList: document.getElementById("contributor-leaderboard-list"),
  recentContributorsList: document.getElementById("recent-contributors-list"),
  inventoryStatus: document.getElementById("inventory-status"),
  inventoryUnclaimed: document.getElementById("inventory-unclaimed"),
  inventoryAvailableDetail: document.getElementById("inventory-available-detail"),
  inventoryTotalDetail: document.getElementById("inventory-total-detail"),
  inventoryProgress: document.getElementById("inventory-progress"),
  inventoryPolicy: document.getElementById("inventory-policy"),
  queueTotal: document.getElementById("queue-total"),
  indexUpdated: document.getElementById("index-updated"),
  hourlyLimit: document.getElementById("hourly-limit"),
  maxClaims: document.getElementById("max-claims"),
  trendChart: document.getElementById("trend-chart"),
  trendSummary: document.getElementById("trend-summary"),
  apiKeyName: document.getElementById("api-key-name"),
  apiKeyCreateBtn: document.getElementById("api-key-create-btn"),
  apiKeyCreated: document.getElementById("api-key-created"),
  apiKeyLimit: document.getElementById("api-key-limit"),
  apiKeyList: document.getElementById("api-key-list"),
  claimCount: document.getElementById("claim-count"),
  claimBtn: document.getElementById("claim-btn"),
  claimMultiMode: document.getElementById("claim-multi-mode"),
  claimSelectAll: document.getElementById("claim-select-all"),
  claimDownloadAll: document.getElementById("claim-download-all"),
  claimRemoveSelected: document.getElementById("claim-remove-selected"),
  claimClear: document.getElementById("claim-clear"),
  claimSummary: document.getElementById("claim-summary"),
  claimError: document.getElementById("claim-error"),
  claimResults: document.getElementById("claim-results"),
  uploadPolicy: document.getElementById("upload-policy"),
  uploadSummary: document.getElementById("upload-summary"),
  uploadError: document.getElementById("upload-error"),
  uploadDropzone: document.getElementById("upload-dropzone"),
  uploadInput: document.getElementById("upload-input"),
  uploadSelectBtn: document.getElementById("upload-select-btn"),
  uploadSubmitBtn: document.getElementById("upload-submit-btn"),
  uploadSelectedList: document.getElementById("upload-selected-list"),
  uploadResults: document.getElementById("upload-results"),
};

function normalizeDataSource(value) {
  const normalized = typeof value === "string" ? value.trim() : "";
  switch (normalized) {
    case "live":
    case "cache":
    case "stale":
    case "unavailable":
      return normalized;
    default:
      return "";
  }
}

function normalizeOptionalNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function extractPayloadMeta(payload = {}) {
  return {
    data_source: normalizeDataSource(payload?.data_source),
    generated_at: typeof payload?.generated_at === "string" ? payload.generated_at : "",
    stale_at: typeof payload?.stale_at === "string" ? payload.stale_at : "",
    degraded: Boolean(payload?.degraded),
    degraded_reason: typeof payload?.degraded_reason === "string" ? payload.degraded_reason : "",
  };
}

function isUnavailableMeta(meta = null) {
  return normalizeDataSource(meta?.data_source) === "unavailable";
}

function isStaleMeta(meta = null) {
  return normalizeDataSource(meta?.data_source) === "stale";
}

function formatMetricValue(value, meta = null) {
  if (isUnavailableMeta(meta)) {
    return "暂不可用";
  }
  const numeric = normalizeOptionalNumber(value);
  if (numeric === null) {
    return "-";
  }
  return String(numeric);
}

function listEmptyText(baseText, meta = null) {
  if (isUnavailableMeta(meta)) {
    return "数据暂不可用。";
  }
  if (isStaleMeta(meta)) {
    return `${baseText}（旧快照）`;
  }
  return baseText;
}

function describeMetaStatus(label, meta = null) {
  if (isUnavailableMeta(meta)) {
    return `${label}暂不可用`;
  }
  if (isStaleMeta(meta)) {
    return `${label}使用旧快照`;
  }
  if (meta?.degraded) {
    return `${label}已降级`;
  }
  return "";
}

function updateSummaryOriginBadge() {
  if (!elements.summaryOrigin) {
    return;
  }
  const statusParts = [
    describeMetaStatus("运行时", state.runtimeSnapshotMeta),
    describeMetaStatus("排队", state.queueStatus),
    describeMetaStatus("面板", state.dashboardMeta),
  ].filter(Boolean);
  const originText = statusParts.length
    ? `API：${window.location.origin} · 构建：${APP_BUILD} · ${statusParts.join(" / ")}`
    : `API：${window.location.origin} · 构建：${APP_BUILD}`;
  elements.summaryOrigin.textContent = originText;
  elements.summaryOrigin.title = originText;
}

function describeQueueStatusNotice(status = {}) {
  const meta = extractPayloadMeta(status);
  if (isUnavailableMeta(meta)) {
    return "排队状态暂不可用，稍后会自动重试。";
  }
  if (isStaleMeta(meta)) {
    const updatedAt = formatDateTime(status?.stale_at || status?.generated_at || status?.claimable_now_updated_at);
    return updatedAt && updatedAt !== "-"
      ? `排队状态当前使用旧快照（更新时间 ${updatedAt}）。`
      : "排队状态当前使用旧快照。";
  }
  if (meta.degraded && meta.degraded_reason) {
    return "排队状态已降级返回。";
  }
  return "";
}

function showScreen(name) {
  const loadingVisible = name === "loading";
  const loginVisible = name === "login";
  const appVisible = name === "app";
  const bannedVisible = name === "banned";
  if (elements.loadingScreen) {
    elements.loadingScreen.classList.toggle("hidden", !loadingVisible);
  }
  elements.loginScreen.classList.toggle("hidden", !loginVisible);
  elements.appScreen.classList.toggle("hidden", !appVisible);
  if (elements.bannedScreen) {
    elements.bannedScreen.classList.toggle("hidden", !bannedVisible);
  }
}

function switchTab(name) {
  state.activeTab = name;
  const views = [
    { key: "data", tab: elements.tabData, view: elements.viewData },
    { key: "keys", tab: elements.tabKeys, view: elements.viewKeys },
    { key: "claim", tab: elements.tabClaim, view: elements.viewClaim },
    { key: "upload", tab: elements.tabUpload, view: elements.viewUpload },
    { key: "docs", tab: elements.tabDocs, view: elements.viewDocs },
  ];

  views.forEach((item) => {
    if (!item.tab || !item.view) {
      return;
    }
    const active = item.key === name;
    item.tab.classList.toggle("active", active);
    item.view.classList.toggle("active", active);
  });

  const headerTitle = document.getElementById("header-title");
  if (headerTitle) {
    const map = {
      data: "数据面板",
      keys: "API Key 管理",
      claim: "领取账号",
      upload: "上传账号",
      docs: "API 使用指南",
    };
    headerTitle.textContent = map[name] || "数据面板";
  }

  if (name === "keys") {
    ensureApiKeysLoaded().catch((error) => {
      if (!handleAccessError(error)) {
        console.error("加载 API Keys 失败", error);
      }
    });
  }
  if (name === "claim") {
    renderQueueStatus();
    ensureClaimsLoaded().catch((error) => {
      if (!handleAccessError(error)) {
        console.error("加载领取记录失败", error);
      }
    });
  }
  if (name === "upload" && shouldLoadUploadResultsDetails()) {
    loadUploadResultsSnapshot().catch((error) => {
      if (!handleAccessError(error)) {
        console.error("刷新上传结果失败", error);
      }
    });
  }
}

function setLoginMessage(message = "", tone = "error") {
  elements.loginMessage.textContent = message;
  elements.loginMessage.dataset.tone = tone;
  if (message) {
    elements.loginMessage.classList.remove("hidden");
  } else {
    elements.loginMessage.classList.add("hidden");
  }
}

const summarySync = createSummarySyncController({
  getTabId: () => state.tabId,
  getCurrentLeader: () => state.currentLeader,
  hasCrossTabCoordination: () => hasCrossTabCoordination(),
  isLeaderTab: () => state.isPollingLeader && state.currentLeader?.tabId === state.tabId,
  canPerformPrimaryRefreshWork: () => canPerformPrimaryRefreshWork(),
  requestLeadership: (...args) => claimPollingLeadership(...args),
  broadcastMessage: (...args) => broadcastMessage(...args),
  fetchJson,
  handleAccessError,
  applyRuntimeSnapshot,
  applyDashboardSummary,
});

const leaderSync = createLeaderSyncController({
  state,
  constants: {
    LEADER_HEARTBEAT_MS,
    LEADER_STALE_MS,
    LEADER_PROBE_WAIT_MS,
    LEADERSHIP_REASON_PRIORITY,
  },
  shouldRunBackgroundRefresh,
  onLeaderStatusChange: handleLeaderStatusChange,
  onLeaderChanged: handleLeaderChanged,
  onMessage: handleCrossTabMessage,
  onTransportFailure: handleBroadcastTransportFailure,
  onTransportReset: handleBroadcastTransportReset,
});

function hasCrossTabCoordination() {
  return leaderSync.hasCrossTabCoordination();
}

function getLeadershipPriority(reason = "background") {
  return leaderSync.getLeadershipPriority(reason);
}

function broadcastMessage(type, payload) {
  return leaderSync.broadcastMessage(type, payload);
}

function releasePollingLeadership(reason = "release") {
  return leaderSync.releasePollingLeadership(reason);
}

function claimPollingLeadership(force = false, options = {}) {
  return leaderSync.claimPollingLeadership(force, options);
}

function takePollingLeadership(reason = "manual") {
  return leaderSync.takePollingLeadership(reason);
}

function initBroadcastChannel() {
  return leaderSync.initBroadcastChannel();
}

function disposeLeaderSync() {
  return leaderSync.dispose();
}

function getDefaultUploadResultsSummary() {
  return {
    total: 0,
    accepted: 0,
    duplicates: 0,
    invalid: 0,
    rejected: 0,
    db_busy: 0,
    queued: 0,
    processing: 0,
  };
}

function normalizeUploadResultsSummary(summary) {
  return {
    ...getDefaultUploadResultsSummary(),
    ...(summary || {}),
  };
}

function renderUploadSummary(summary = {}) {
  const normalized = normalizeUploadResultsSummary(summary);
  const parts = [
    `本次共处理 ${normalized.total ?? 0} 个文件`,
    `成功 ${normalized.accepted ?? 0} 个`,
    `重复 ${normalized.duplicates ?? 0} 个`,
  ];
  if (normalized.queued) {
    parts.push(`等待中 ${normalized.queued} 个`);
  }
  if (normalized.processing) {
    parts.push(`测试或入库中 ${normalized.processing} 个`);
  }
  if (normalized.db_busy) {
    parts.push(`数据库繁忙 ${normalized.db_busy} 个`);
  }
  let message = `${parts.join("，")}。`;
  if ((normalized.queued ?? 0) > 0 || (normalized.processing ?? 0) > 0) {
    message += ` 当前使用${state.uploadStream ? "实时推送" : "自动刷新"}同步上传进度。`;
  }
  elements.uploadSummary.textContent = message;
  if ((normalized.total ?? 0) > 0) {
    elements.uploadSummary.classList.remove("hidden");
  } else {
    elements.uploadSummary.classList.add("hidden");
  }
}

function buildUploadResultsBroadcastPayload(payload) {
  return {
    batch_id: payload?.batch_id || null,
    created_at: payload?.created_at || null,
    summary: normalizeUploadResultsSummary(payload?.summary),
    items: Array.isArray(payload?.items) ? payload.items : null,
    history: Array.isArray(payload?.history) ? payload.history : [],
    queue_status: payload?.queue_status || null,
  };
}

function applyUploadResultsSummaryPayload(payload) {
  const normalized = buildUploadResultsBroadcastPayload(payload);
  state.uploadResultsSummary = normalized.summary;
  if (state.activeTab === "upload") {
    renderUploadSummary(normalized.summary);
  }
  syncUploadResultsTransport();
}

function shouldLoadUploadResultsDetails() {
  return !state.uploadResultsLoaded || state.uploadResultsDirty || shouldPollUploadResults();
}

function broadcastUploadResultsChanged(payload) {
  broadcastMessage("upload_results_changed", buildUploadResultsBroadcastPayload(payload));
}

function broadcastClaimsPayload(payload) {
  broadcastMessage("claims_payload", payload || { items: [] });
}

function clearClaimsRequestTimer() {
  if (state.claimsRequestTimer) {
    clearTimeout(state.claimsRequestTimer);
    state.claimsRequestTimer = null;
  }
}

function shouldRefreshClaimsDetails() {
  return !state.visibilityHidden && state.activeTab === "claim";
}

function requestClaimsRefreshFromLeader(reason = "claims-sync") {
  if (!hasCrossTabCoordination() || canPerformPrimaryRefreshWork()) {
    return false;
  }
  broadcastMessage("claims_refresh_requested", {
    reason,
    sentAt: Date.now(),
  });
  return true;
}

function canFetchClaimsDirectly() {
  return !hasCrossTabCoordination() || (
    state.isPollingLeader &&
    state.currentLeader?.tabId === state.tabId
  );
}

function scheduleLeaderClaimsRefresh(reason = "claims-sync", delayMs = CLAIMS_REFRESH_DEBOUNCE_MS) {
  if (!canFetchClaimsDirectly()) {
    return;
  }
  if (state.claimsLoadPromise) {
    state.claimsLoadShouldBroadcast = state.claimsLoadShouldBroadcast || hasCrossTabCoordination();
    return;
  }
  if (state.claimsRequestTimer) {
    return;
  }
  state.claimsRequestTimer = setTimeout(async () => {
    state.claimsRequestTimer = null;
    if (!canFetchClaimsDirectly()) {
      return;
    }
    try {
      await refreshClaimsByLeader(reason, { requestLeader: false });
    } catch (error) {
      if (!handleAccessError(error)) {
        console.error("刷新领取记录失败", error);
      }
    }
  }, Math.max(0, delayMs));
}

function applyUploadResultsChanged(payload) {
  if (Array.isArray(payload?.items)) {
    applyUploadResultsPayload(payload);
    return;
  }
  state.uploadResultsDirty = true;
  applyUploadResultsSummaryPayload(payload);
}

function applyClaimsPayload(payload) {
  const hiddenIds = state.pendingHiddenClaimIds || new Set();
  const serverIds = new Set((payload.items || []).map((item) => item.claim_id));
  Array.from(hiddenIds).forEach((id) => {
    if (!serverIds.has(id)) {
      hiddenIds.delete(id);
    }
  });
  const items = (payload.items || []).filter((item) => !hiddenIds.has(item.claim_id));
  const total = items.length;
  state.lastClaimTotal = total;
  state.claimsDirty = false;
  state.claimsInitialized = true;
  state.claimResults = items;
  state.claimSelected.clear();
  state.claims = {
    ...(state.claims || {}),
    total,
  };
  renderMyClaims();
  renderClaimResults();
}

async function loadClaimsInternal(options = {}) {
  const shouldBroadcast = Boolean(options.broadcast && hasCrossTabCoordination());
  if (state.claimsLoadPromise) {
    if (shouldBroadcast) {
      state.claimsLoadShouldBroadcast = true;
    }
    return state.claimsLoadPromise;
  }
  state.claimsLoadShouldBroadcast = shouldBroadcast;
  state.claimsLoadPromise = (async () => {
    const payload = await fetchJson("/me/claims");
    applyClaimsPayload(payload || {});
    if (state.claimsLoadShouldBroadcast) {
      broadcastClaimsPayload(payload || {});
    }
    return payload;
  })();
  try {
    return await state.claimsLoadPromise;
  } finally {
    state.claimsLoadPromise = null;
    state.claimsLoadShouldBroadcast = false;
  }
}

async function refreshClaimsByLeader(reason = "claims-sync", options = {}) {
  const requestLeader = options.requestLeader !== false;
  state.claimsDirty = true;
  if (canFetchClaimsDirectly()) {
    return loadClaimsInternal({
      broadcast: options.broadcast !== false && hasCrossTabCoordination(),
      reason,
    });
  }
  if (requestLeader) {
    requestClaimsRefreshFromLeader(reason);
    claimPollingLeadership(false, { reason });
  }
  return null;
}

function applyRuntimeSnapshot(payload) {
  if (!payload) {
    return;
  }
  state.runtimeSnapshotMeta = extractPayloadMeta(payload);
  if (payload.user) {
    state.user = payload.user;
    renderUser();
  }
  if (payload.quota) {
    state.quota = payload.quota;
    renderQuota();
  }
  if (payload.claims) {
    state.claims = payload.claims;
    renderMyClaims();
  }
  if (payload.api_keys?.summary) {
    applyApiKeySummary(payload.api_keys.summary);
    if (state.activeTab === "keys") {
      renderApiKeys();
    }
  }
  if (payload.uploads) {
    state.uploadPolicy = payload.uploads;
    renderUploadPolicy();
  }
  if (payload.upload_results?.summary) {
    applyUploadResultsSummaryPayload(payload.upload_results);
  }
  updateSummaryOriginBadge();
}

function applyDashboardSummary(summary) {
  state.dashboardMeta = extractPayloadMeta(summary);
  state.dashboardSectionMeta = {
    stats: extractPayloadMeta(summary?.stats),
    leaderboard: extractPayloadMeta(summary?.leaderboard),
    recent: extractPayloadMeta(summary?.recent),
    contributors: extractPayloadMeta(summary?.contributors),
    recent_contributors: extractPayloadMeta(summary?.recent_contributors),
    trends: extractPayloadMeta(summary?.trends),
    system: extractPayloadMeta(summary?.system),
  };
  state.stats = summary?.stats || null;
  state.leaderboard = summary?.leaderboard?.items || [];
  state.recentClaims = summary?.recent?.items || [];
  state.contributorLeaderboard = summary?.contributors?.items || [];
  state.recentContributors = summary?.recent_contributors?.items || [];
  state.trends = summary?.trends?.series || [];
  state.trendsMeta = summary?.trends
    ? { window: summary.trends.window, bucket: summary.trends.bucket }
    : null;
  state.systemStatus = summary?.system || null;
  renderStats();
  renderLeaderboard();
  renderRecentClaims();
  renderContributorLeaderboard();
  renderRecentContributors();
  renderSystemStatus();
  renderTrends();
  updateSummaryOriginBadge();
}

function logClaimClientEvent(message, details = {}) {
  const payload = {
    tab_id: state.tabId,
    active_request_id: state.activeClaimRequestId || null,
    ...details,
  };
  console.info(`[claim] ${message}`, payload);
}

function describeClaimTerminalSummary(request) {
  const granted = request?.granted || 0;
  const requested = request?.requested || granted;
  switch (request?.status) {
    case "succeeded":
      return `已领取 ${granted} / 请求 ${requested}。`;
    case "partial":
      return request.reason_message || `已领取 ${granted} / 请求 ${requested}，仍有 ${request.remaining || 0} 个未完成。`;
    case "cancelled":
    case "expired":
    case "failed":
      return request.reason_message || "领取请求未成功完成。";
    default:
      return "";
  }
}

function describeQueuedClaimSummary(request) {
  const position = request?.queue_position || "-";
  const total = request?.queue_total || "-";
  const remaining = request?.remaining ?? request?.requested ?? "-";
  const modeLabel = state.claimStreamConnected ? "实时推送已连接" : "实时连接中断，正在重连";
  return `已进入排队（第 ${position}/${total} 位，待领取 ${remaining}）。${modeLabel}。`;
}

function updateClaimRequestUI(activeRequest = null, queueRequest = null) {
  if (activeRequest?.status === "processing") {
    state.queueStatus = { queued: false };
    state.queueSticky = false;
    setClaimQueued(false);
    setClaimSubmitting(true);
    elements.claimSummary.textContent = "请求已创建，等待服务器结果推送...";
    elements.claimSummary.classList.remove("hidden");
    return;
  }

  if (queueRequest?.status === "queued") {
    state.queueStatus = buildQueueStatusFromRequest(queueRequest);
    state.queueSticky = true;
    setClaimSubmitting(false);
    setClaimQueued(true);
    elements.claimSummary.textContent = describeQueuedClaimSummary(queueRequest);
    elements.claimSummary.classList.remove("hidden");
    return;
  }

  state.queueStatus = { queued: false };
  state.queueSticky = false;
  setClaimQueued(false);
  if (activeRequest?.terminal) {
    setClaimSubmitting(false);
    elements.claimSummary.textContent = describeClaimTerminalSummary(activeRequest);
    elements.claimSummary.classList.remove("hidden");
    return;
  }
  if (state.claimStreamError) {
    elements.claimSummary.textContent = state.claimStreamError;
    elements.claimSummary.classList.remove("hidden");
    return;
  }
  const queueNotice = describeQueueStatusNotice(state.queueStatus || {});
  if (queueNotice) {
    elements.claimSummary.textContent = queueNotice;
    elements.claimSummary.classList.remove("hidden");
    return;
  }
  if (!state.isClaimSubmitting) {
    elements.claimSummary.classList.add("hidden");
  }
}

function handleClaimRealtimeEffects(effects = []) {
  const normalized = Array.isArray(effects)
    ? effects.filter((effect) => effect && typeof effect === "object")
    : [];

  normalized
    .filter((effect) => effect.type === "toast")
    .forEach((effect) => {
      try {
        showModal(effect.title, effect.message);
      } catch (error) {
        console.error("显示领取结果弹窗失败", error, effect);
      }
    });

  normalized
    .filter((effect) => effect.type === "terminal")
    .forEach((effect) => {
      try {
        logClaimClientEvent("received terminal event", {
          request_id: effect.requestId,
          status: effect.status,
        });
        logClaimClientEvent("ui loading finished", {
          request_id: effect.requestId,
          status: effect.status,
        });
        refreshClaimsByLeader("claim-terminal", { requestLeader: false }).catch((error) => {
          state.claimsDirty = true;
          if (!handleAccessError(error)) {
            console.error("刷新领取记录失败", error);
          }
        });
        refreshSummariesByLeader("claim-terminal", { requestLeader: false }).catch((error) => {
          if (!handleAccessError(error)) {
            console.error("刷新摘要失败", error);
          }
        });
      } catch (error) {
        console.error("处理领取终态副作用失败", error, effect);
      }
    });
}

function broadcastClaimRealtimePayload(payload, options = {}) {
  if (options.broadcast === false) {
    return;
  }
  broadcastMessage("claim_realtime", payload);
}

function applyClaimRealtimePayload(payload, options = {}) {
  const result = applyClaimSnapshotState(state.claimRealtimeState, payload || {}, {
    tabId: state.tabId,
    activeRequestId: state.activeClaimRequestId,
    isSubmitting: state.isClaimSubmitting,
    emitToasts: options.emitToasts !== false,
  });
  state.claimRealtimeState = result.state;
  state.activeClaimRequestId = result.state.activeRequestId || "";
  if (options.fromStream) {
    state.claimStreamConnected = true;
    state.claimStreamError = "";
    state.claimStreamRetryCount = 0;
  }
  renderQueueStatus();
  if (result.activeRequest?.terminal) {
    setClaimSubmitting(false);
  }
  broadcastClaimRealtimePayload(payload || {}, options);
  handleClaimRealtimeEffects(result.effects || []);
  syncQueueRealtimeTransport();
}

function normalizeQueueStatusPayload(payload = {}) {
  const meta = extractPayloadMeta(payload);
  return {
    queued: Boolean(payload?.queued),
    queue_id: normalizeOptionalNumber(payload?.queue_id),
    position: normalizeOptionalNumber(payload?.position),
    total_queued: normalizeOptionalNumber(payload?.total_queued),
    requested: normalizeOptionalNumber(payload?.requested),
    remaining: normalizeOptionalNumber(payload?.remaining ?? payload?.requested),
    request_id: typeof payload?.request_id === "string" ? payload.request_id : "",
    available_tokens: normalizeOptionalNumber(payload?.available_tokens),
    claimable_now: normalizeOptionalNumber(payload?.claimable_now),
    claimable_now_state: normalizeDataSource(payload?.claimable_now_state),
    claimable_now_updated_at: typeof payload?.claimable_now_updated_at === "string"
      ? payload.claimable_now_updated_at
      : "",
    ...meta,
  };
}

function buildQueueRequestFromStatus(status = {}) {
  if (!status?.queued) {
    return null;
  }
  return {
    status: "queued",
    queued: true,
    queue_id: status.queue_id ?? 0,
    queue_position: status.position ?? 0,
    queue_total: status.total_queued ?? 0,
    requested: status.requested ?? 0,
    remaining: status.remaining ?? 0,
    request_id: status.request_id || "",
  };
}

function applyQueueStatusPayload(payload) {
  state.queueStatus = normalizeQueueStatusPayload(payload || {});
  renderQueueStatus();
  syncQueueRealtimeTransport();
  updateSummaryOriginBadge();
  return state.queueStatus;
}

function applyBootstrapPayload(payload) {
  resetCrossTabSummaryState("bootstrap");
  const profile = payload?.profile || {};
  state.user = profile.user || null;
  state.quota = profile.quota || null;
  state.claims = profile.claims || null;
  state.uploadPolicy = profile.uploads || null;
  state.uploadResults = [];
  state.uploadHistory = [];
  state.uploadResultsLoaded = false;
  state.uploadResultsDirty = false;
  applyApiKeySummary(profile.api_keys || {});
  applyClaimRealtimePayload(payload?.claim_realtime || { requests: [] }, {
    broadcast: false,
    emitToasts: false,
  });
  renderUser();
  renderQuota();
  renderMyClaims();
  renderUploadPolicy();
  applyDashboardSummary(payload?.dashboard || {});
  renderUploadResults();
  applyUploadResultsSummaryPayload(payload?.upload_results || {});
}

function applyDocsBaseUrl() {
  const baseUrl = window.location.origin;
  const nodes = document.querySelectorAll('.endpoint-block pre');
  nodes.forEach((node) => {
    node.textContent = node.textContent.replace(/\$\{TOKEN_PROVIDER_BASE_URL\}/g, baseUrl);
  });
}

function renderQueueStatus() {
  const requests = Object.values(state.claimRealtimeState?.requestsById || {});
  const activeRequest = state.activeClaimRequestId
    ? requests.find((request) => request.request_id === state.activeClaimRequestId) || null
    : null;
  const queueRequest = requests.find((request) => request.status === "queued")
    || buildQueueRequestFromStatus(state.queueStatus);
  updateClaimRequestUI(activeRequest, queueRequest);
}

function hasPendingClaimRealtimeRequests() {
  const requests = Object.values(state.claimRealtimeState?.requestsById || {});
  return requests.some((request) => request && request.terminal !== true)
    || Boolean(state.queueStatus?.queued);
}

async function fetchJson(url, options = {}) {
	const headers = {
		"X-App-Build": APP_BUILD,
		"X-App-Entry": APP_ENTRY,
		...(options.headers || {}),
	};
	const response = await fetch(url, {
		cache: "no-store",
		credentials: "same-origin",
		...options,
		headers,
	});
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(payload.detail || `请求失败: ${response.status}`);
    error.status = response.status;
    error.payload = payload;
    throw error;
  }
  return payload;
}

async function readResponseErrorMessage(response, fallbackMessage = "") {
  const contentType = (response.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    const payload = await response.json().catch(() => ({}));
    if (payload?.detail) {
      return String(payload.detail);
    }
  }
  const text = await response.text().catch(() => "");
  const normalized = text.trim();
  if (normalized) {
    return normalized.length > 400 ? normalized.slice(0, 400) : normalized;
  }
  return fallbackMessage || `请求失败: ${response.status}`;
}

function parseDownloadFilename(contentDisposition = "") {
  const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch (error) {
      return utf8Match[1];
    }
  }
  const quotedMatch = contentDisposition.match(/filename=\"([^\"]+)\"/i);
  if (quotedMatch?.[1]) {
    return quotedMatch[1];
  }
  const plainMatch = contentDisposition.match(/filename=([^;]+)/i);
  return plainMatch?.[1]?.trim() || "";
}

function triggerBrowserDownload(blob, filename) {
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = filename;
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();
  // 延迟释放大文件下载的 blob URL，避免浏览器尚未接管文件时被提前回收。
  setTimeout(() => URL.revokeObjectURL(objectUrl), 60000);
}

const CLAIM_DOWNLOAD_ALL_LABEL = "下载全部账号(ZIP格式)";
const CLAIM_DOWNLOAD_TEXT_ENCODER = new TextEncoder();
const ZIP_UTF8_FLAG = 0x0800;
const ZIP_STORE_METHOD = 0;
const ZIP_FORMAT_VERSION = 20;
const ZIP_CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let index = 0; index < table.length; index += 1) {
    let value = index;
    for (let bit = 0; bit < 8; bit += 1) {
      value = (value & 1) ? (0xedb88320 ^ (value >>> 1)) : (value >>> 1);
    }
    table[index] = value >>> 0;
  }
  return table;
})();

function normalizeDownloadFileName(fileName, fallbackName = "claimed-token.json") {
  const fallback = String(fallbackName || "claimed-token.json").trim() || "claimed-token.json";
  const raw = String(fileName || "").trim();
  const baseName = raw.split(/[\\/]/).pop() || "";
  const sanitized = baseName
    .replace(/[\u0000-\u001f<>:"|?*]+/g, "_")
    .replace(/\s+/g, " ")
    .trim();
  if (!sanitized || sanitized === "." || sanitized === "..") {
    return fallback;
  }
  return sanitized;
}

function splitFileNameParts(fileName) {
  const lastDot = fileName.lastIndexOf(".");
  if (lastDot <= 0) {
    return { stem: fileName, extension: "" };
  }
  return {
    stem: fileName.slice(0, lastDot),
    extension: fileName.slice(lastDot),
  };
}

function ensureUniqueArchiveFileName(fileName, usedNames) {
  const normalized = fileName.toLowerCase();
  if (!usedNames.has(normalized)) {
    usedNames.add(normalized);
    return fileName;
  }
  const { stem, extension } = splitFileNameParts(fileName);
  for (let suffix = 2; suffix <= 99999; suffix += 1) {
    const candidate = `${stem} (${suffix})${extension}`;
    const candidateKey = candidate.toLowerCase();
    if (!usedNames.has(candidateKey)) {
      usedNames.add(candidateKey);
      return candidate;
    }
  }
  throw new Error(`文件名重复过多，无法打包：${fileName}`);
}

function buildClaimJsonText(item) {
  try {
    const jsonText = JSON.stringify(item?.content, null, 2);
    if (typeof jsonText !== "string") {
      throw new Error("账号内容为空，无法生成下载文件。");
    }
    return `${jsonText}\n`;
  } catch (error) {
    if (error instanceof Error && error.message === "账号内容为空，无法生成下载文件。") {
      throw error;
    }
    throw new Error("账号内容无法序列化为 JSON。");
  }
}

function buildClaimJsonDownload(item, fallbackName = "claimed-token.json") {
  const filename = normalizeDownloadFileName(item?.file_name, fallbackName);
  const jsonText = buildClaimJsonText(item);
  const bytes = CLAIM_DOWNLOAD_TEXT_ENCODER.encode(jsonText);
  return {
    filename,
    bytes,
    blob: new Blob([bytes], { type: "application/json;charset=utf-8" }),
  };
}

function computeCrc32(bytes) {
  let crc = 0xffffffff;
  for (let index = 0; index < bytes.length; index += 1) {
    crc = ZIP_CRC32_TABLE[(crc ^ bytes[index]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function getZipDosTimestamp(date = new Date()) {
  const safeDate = date instanceof Date && !Number.isNaN(date.getTime()) ? date : new Date();
  const year = Math.min(Math.max(safeDate.getFullYear(), 1980), 2107);
  return {
    dosTime: ((safeDate.getHours() & 0x1f) << 11)
      | ((safeDate.getMinutes() & 0x3f) << 5)
      | Math.floor((safeDate.getSeconds() & 0x3f) / 2),
    dosDate: (((year - 1980) & 0x7f) << 9)
      | (((safeDate.getMonth() + 1) & 0x0f) << 5)
      | (safeDate.getDate() & 0x1f),
  };
}

function buildStoredZipBlob(files) {
  if (!Array.isArray(files) || !files.length) {
    throw new Error("当前没有可打包的账号。");
  }
  if (files.length > 0xffff) {
    throw new Error("账号数量过多，无法在前端直接打包。");
  }

  const localParts = [];
  const centralDirectoryParts = [];
  let localOffset = 0;
  let centralDirectorySize = 0;

  files.forEach((file) => {
    const fileNameBytes = CLAIM_DOWNLOAD_TEXT_ENCODER.encode(file.name);
    const contentBytes = file.bytes;
    if (fileNameBytes.length > 0xffff) {
      throw new Error(`文件名过长，无法打包：${file.name}`);
    }
    if (contentBytes.length > 0xffffffff) {
      throw new Error(`文件过大，无法打包：${file.name}`);
    }
    const nextLocalOffset = localOffset + 30 + fileNameBytes.length + contentBytes.length;
    if (nextLocalOffset > 0xffffffff) {
      throw new Error("打包内容过大，请分批下载。");
    }

    const { dosTime, dosDate } = getZipDosTimestamp(file.modifiedAt);
    const crc32 = computeCrc32(contentBytes);

    const localHeader = new ArrayBuffer(30);
    const localView = new DataView(localHeader);
    localView.setUint32(0, 0x04034b50, true);
    localView.setUint16(4, ZIP_FORMAT_VERSION, true);
    localView.setUint16(6, ZIP_UTF8_FLAG, true);
    localView.setUint16(8, ZIP_STORE_METHOD, true);
    localView.setUint16(10, dosTime, true);
    localView.setUint16(12, dosDate, true);
    localView.setUint32(14, crc32, true);
    localView.setUint32(18, contentBytes.length, true);
    localView.setUint32(22, contentBytes.length, true);
    localView.setUint16(26, fileNameBytes.length, true);
    localView.setUint16(28, 0, true);
    localParts.push(localHeader, fileNameBytes, contentBytes);

    const centralHeader = new ArrayBuffer(46);
    const centralView = new DataView(centralHeader);
    centralView.setUint32(0, 0x02014b50, true);
    centralView.setUint16(4, ZIP_FORMAT_VERSION, true);
    centralView.setUint16(6, ZIP_FORMAT_VERSION, true);
    centralView.setUint16(8, ZIP_UTF8_FLAG, true);
    centralView.setUint16(10, ZIP_STORE_METHOD, true);
    centralView.setUint16(12, dosTime, true);
    centralView.setUint16(14, dosDate, true);
    centralView.setUint32(16, crc32, true);
    centralView.setUint32(20, contentBytes.length, true);
    centralView.setUint32(24, contentBytes.length, true);
    centralView.setUint16(28, fileNameBytes.length, true);
    centralView.setUint16(30, 0, true);
    centralView.setUint16(32, 0, true);
    centralView.setUint16(34, 0, true);
    centralView.setUint16(36, 0, true);
    centralView.setUint32(38, 0, true);
    centralView.setUint32(42, localOffset, true);
    centralDirectoryParts.push(centralHeader, fileNameBytes);

    localOffset = nextLocalOffset;
    centralDirectorySize += 46 + fileNameBytes.length;
  });

  if (localOffset + centralDirectorySize > 0xffffffff) {
    throw new Error("打包内容过大，请分批下载。");
  }

  const endRecord = new ArrayBuffer(22);
  const endView = new DataView(endRecord);
  endView.setUint32(0, 0x06054b50, true);
  endView.setUint16(4, 0, true);
  endView.setUint16(6, 0, true);
  endView.setUint16(8, files.length, true);
  endView.setUint16(10, files.length, true);
  endView.setUint32(12, centralDirectorySize, true);
  endView.setUint32(16, localOffset, true);
  endView.setUint16(20, 0, true);

  return new Blob([...localParts, ...centralDirectoryParts, endRecord], { type: "application/zip" });
}

function formatArchiveTimestamp(date = new Date()) {
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
  ].join("") + `-${pad(date.getHours())}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

function buildClaimsArchiveDownload(items) {
  if (!Array.isArray(items) || !items.length) {
    throw new Error("当前没有可下载的账号。");
  }
  const usedNames = new Set();
  const files = items.map((item, index) => {
    const fallbackName = `claimed-token-${index + 1}.json`;
    const payload = buildClaimJsonDownload(item, fallbackName);
    return {
      name: ensureUniqueArchiveFileName(payload.filename, usedNames),
      bytes: payload.bytes,
      modifiedAt: new Date(),
    };
  });

  return {
    filename: `claimed-${formatArchiveTimestamp(new Date())}.zip`,
    blob: buildStoredZipBlob(files),
  };
}

async function downloadFileWithValidation(url, options = {}) {
  const expectedContentType = String(options.expectedContentType || "").toLowerCase();
  const fallbackFilename = options.fallbackFilename || "";
  const response = await fetch(url, {
    cache: "no-store",
    credentials: "same-origin",
    ...options.fetchOptions,
  });
  if (!response.ok) {
    const error = new Error(await readResponseErrorMessage(response, `下载失败: ${response.status}`));
    error.status = response.status;
    throw error;
  }
  const contentType = (response.headers.get("content-type") || "").toLowerCase();
  if (!contentType.includes(expectedContentType)) {
    const error = new Error(await readResponseErrorMessage(response, "下载响应格式无效。"));
    error.status = response.status;
    throw error;
  }
  const contentDisposition = response.headers.get("content-disposition") || "";
  const filename = parseDownloadFilename(contentDisposition) || fallbackFilename;
  if (!filename) {
    throw new Error("下载响应缺少文件名。");
  }
  const blob = await response.blob();
  triggerBrowserDownload(blob, filename);
  return {
    filename,
    size: blob.size,
  };
}

function showClaimErrorMessage(message) {
  if (!elements.claimError) {
    return;
  }
  elements.claimError.textContent = message;
  elements.claimError.classList.remove("hidden");
}

function hideClaimErrorMessage() {
  elements.claimError?.classList.add("hidden");
}

function renderBannedUser(user) {
  if (elements.bannedReason) {
    elements.bannedReason.textContent = user?.ban_reason || "未提供";
  }
  if (elements.bannedExpires) {
    elements.bannedExpires.textContent = user?.ban_expires_at ? formatDateTime(user.ban_expires_at) : "永久";
  }
  if (elements.bannedAdminLink) {
    elements.bannedAdminLink.classList.toggle("hidden", !user?.is_admin);
  }
}

function handleAccessError(error) {
  if (error?.status === 403 && error?.payload?.ban) {
    const bannedUser = {
      ...(error.payload.user || {}),
      ban_reason: error.payload.ban.reason,
      ban_expires_at: error.payload.ban.expires_at,
      is_admin: Boolean(error.payload.user?.is_admin ?? state.user?.is_admin),
    };
    state.user = bannedUser;
    renderBannedUser(bannedUser);
    stopAllBackgroundActivity("access-denied");
    showScreen("banned");
    return true;
  }
  return false;
}

function renderUser() {
  if (!state.user) {
    elements.userName.textContent = "-";
    elements.userUsername.textContent = "-";
    elements.userId.textContent = "-";
    elements.userTrust.textContent = "-";
    elements.authMethod.textContent = "登录方式：未登录";
    elements.authMethod.title = "登录方式：未登录";
    elements.authUsername.textContent = "用户名：-";
    elements.authUsername.title = "用户名：-";
    return;
  }
  elements.userName.textContent = state.user.name || state.user.username;
  elements.userUsername.textContent = state.user.username || "-";
  elements.userId.textContent = state.user.id || "-";
  elements.userTrust.textContent = state.user.trust_level ?? "-";
  const authMethodText = "登录方式：LinuxDo";
  const usernameText = `用户名：${state.user.name || state.user.username}`;
  elements.authMethod.textContent = authMethodText;
  elements.authMethod.title = authMethodText;
  elements.authUsername.textContent = usernameText;
  elements.authUsername.title = usernameText;
}

function renderQuota() {
  if (!state.quota || isUnavailableMeta(state.runtimeSnapshotMeta)) {
    elements.quotaUsed.textContent = "暂不可用";
    elements.quotaRemaining.textContent = "暂不可用";
    elements.quotaLimit.textContent = "暂不可用";
    return;
  }
  elements.quotaUsed.textContent = state.quota.used;
  elements.quotaRemaining.textContent = state.quota.remaining;
  elements.quotaLimit.textContent = state.quota.limit;
}

function renderStats() {
  const statsMeta = state.dashboardSectionMeta?.stats || null;
  if (!state.stats || isUnavailableMeta(statsMeta)) {
    elements.inventoryAvailable.textContent = "暂不可用";
    elements.inventoryTotal.textContent = "暂不可用";
    elements.claimsOthersTotal.textContent = "暂不可用";
    elements.claimsOthersUnique.textContent = "暂不可用";
    elements.claimsTotal.textContent = "暂不可用";
    elements.claimsUnique.textContent = "暂不可用";
    return;
  }
  elements.inventoryAvailable.textContent = formatMetricValue(state.stats.available_tokens, statsMeta);
  elements.inventoryTotal.textContent = formatMetricValue(state.stats.total_tokens, statsMeta);
  elements.claimsOthersTotal.textContent = formatMetricValue(state.stats.others_claimed_total, statsMeta);
  elements.claimsOthersUnique.textContent = formatMetricValue(state.stats.others_claimed_unique, statsMeta);
  elements.claimsTotal.textContent = formatMetricValue(state.stats.claimed_total, statsMeta);
  elements.claimsUnique.textContent = formatMetricValue(state.stats.claimed_unique, statsMeta);
}

function renderMyClaims() {
  if (isUnavailableMeta(state.runtimeSnapshotMeta)) {
    elements.myClaimsTotal.textContent = "暂不可用";
    return;
  }
  const total = Math.max(0, (state.claims?.total ?? 0) - (state.pendingHiddenClaimIds?.size ?? 0));
  elements.myClaimsTotal.textContent = total;
}

function formatBytes(value) {
  const size = Number(value || 0);
  if (size < 1024) {
    return `${size} B`;
  }
  return `${(size / 1024).toFixed(size >= 10 * 1024 ? 0 : 1)} KB`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function renderRankedList(list, items, emptyText, meta = null) {
  if (!list) {
    return;
  }
  list.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = listEmptyText(emptyText, meta);
    list.appendChild(empty);
    return;
  }
  items.forEach((item, index) => {
    const row = document.createElement("div");
    row.className = "leaderboard-row";
    row.innerHTML = `
      <div class="leaderboard-rank">#${index + 1}</div>
      <div class="leaderboard-name">${item.name || item.username || "-"} (@${item.username || "-"})</div>
      <div class="leaderboard-count mono">个数: ${item.count ?? 0}</div>
    `;
    list.appendChild(row);
  });
}

function renderTimedList(list, items, emptyText, timeField, meta = null) {
  if (!list) {
    return;
  }
  list.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = listEmptyText(emptyText, meta);
    list.appendChild(empty);
    return;
  }
  items.forEach((item, index) => {
    const row = document.createElement("div");
    row.className = "leaderboard-row";
    row.innerHTML = `
      <div class=\"leaderboard-rank\">#${index + 1}</div>
      <div class=\"leaderboard-name\">${item.name || item.username || "-"} (@${item.username || "-"})</div>
      <div class=\"leaderboard-count mono\">个数: ${item.count ?? 0} 时间: ${formatDateTime(item[timeField])}</div>
    `;
    list.appendChild(row);
  });
}

function renderLeaderboard() {
  renderRankedList(
    elements.leaderboardList,
    state.leaderboard || [],
    "暂无排行数据。",
    state.dashboardSectionMeta?.leaderboard
  );
}

function renderRecentClaims() {
  renderTimedList(
    elements.recentClaimsList,
    state.recentClaims || [],
    "暂无最近领取记录。",
    "claimed_at",
    state.dashboardSectionMeta?.recent
  );
}

function renderContributorLeaderboard() {
  renderRankedList(
    elements.contributorLeaderboardList,
    state.contributorLeaderboard || [],
    "暂无贡献者数据。",
    state.dashboardSectionMeta?.contributors
  );
}

function renderRecentContributors() {
  renderTimedList(
    elements.recentContributorsList,
    state.recentContributors || [],
    "暂无最近贡献者数据。",
    "uploaded_at",
    state.dashboardSectionMeta?.recent_contributors
  );
}

function renderSystemStatus() {
  const systemMeta = state.dashboardSectionMeta?.system || null;
  const status = state.systemStatus;
  if (!status || isUnavailableMeta(systemMeta)) {
    if (elements.inventoryStatus) {
      elements.inventoryStatus.textContent = "暂不可用";
      elements.inventoryStatus.className = "status-pill";
    }
    if (elements.inventoryHealthValue) {
      elements.inventoryHealthValue.textContent = "暂不可用";
    }
    if (elements.inventoryHealthUnclaimed) {
      elements.inventoryHealthUnclaimed.textContent = "暂不可用";
    }
    if (elements.inventoryHealthHourly) {
      elements.inventoryHealthHourly.textContent = "暂不可用";
    }
    if (elements.systemStatusValue) {
      elements.systemStatusValue.textContent = "暂不可用";
    }
    if (elements.systemStatusQueue) {
      elements.systemStatusQueue.textContent = "暂不可用";
    }
    if (elements.systemStatusIndex) {
      elements.systemStatusIndex.textContent = "暂不可用";
    }
    return;
  }
  const inventory = status.inventory || {};
  const health = status.health || {};
  const unclaimed = inventory.unclaimed ?? 0;
  const statusMap = { healthy: "健康", warning: "警告", critical: "严重" };
  const statusKey = health.status || "healthy";
  if (elements.inventoryHealthValue) {
    elements.inventoryHealthValue.textContent = statusMap[statusKey] || statusKey;
  }
  if (elements.inventoryHealthUnclaimed) {
    elements.inventoryHealthUnclaimed.textContent = unclaimed;
  }
  if (elements.inventoryHealthHourly) {
    elements.inventoryHealthHourly.textContent = health.hourly_limit ?? 0;
  }
  if (elements.systemStatusValue) {
    elements.systemStatusValue.textContent = isStaleMeta(systemMeta) ? "旧快照" : "在线";
  }
  if (elements.systemStatusQueue) {
    elements.systemStatusQueue.textContent = formatMetricValue(status.queue?.total, systemMeta);
  }
  if (elements.systemStatusIndex) {
    elements.systemStatusIndex.textContent = formatDateTime(status.index?.updated_at);
  }
}

function renderTrendChart(series, meta = null) {
  const container = elements.trendChart;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (isUnavailableMeta(meta)) {
    const empty = document.createElement("div");
    empty.className = "trend-empty";
    empty.textContent = "趋势数据暂不可用。";
    container.appendChild(empty);
    return;
  }
  if (!series || !series.length) {
    const empty = document.createElement("div");
    empty.className = "trend-empty";
    empty.textContent = "暂无趋势数据。";
    container.appendChild(empty);
    return;
  }
  const padding = 24;
  const rect = container.getBoundingClientRect();
  const width = Math.max(300, rect.width || 800);
  const height = Math.max(180, rect.height || 220);
  const maxValue = Math.max(...series.map((item) => item.count || 0), 1);
  const points = series.map((item, index) => {
    const x = padding + (index / Math.max(1, series.length - 1)) * (width - padding * 2);
    const y = height - padding - ((item.count || 0) / maxValue) * (height - padding * 2);
    return { x, y };
  });
  const line = points.map((pt) => `${pt.x},${pt.y}`).join(" ");
  let areaPath = `M ${points[0].x} ${points[0].y}`;
  points.slice(1).forEach((pt) => {
    areaPath += ` L ${pt.x} ${pt.y}`;
  });
  areaPath += ` L ${width - padding} ${height - padding} L ${padding} ${height - padding} Z`;
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  const tooltip = document.createElement("div");
  tooltip.className = "trend-tooltip";
  container.appendChild(tooltip);
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("preserveAspectRatio", "none");
  svg.innerHTML = `
    <defs>
      <linearGradient id="trendGradient" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0%" stop-color="var(--primary)" stop-opacity="0.35" />
        <stop offset="100%" stop-color="var(--primary)" stop-opacity="0" />
      </linearGradient>
    </defs>
    <path d="${areaPath}" fill="url(#trendGradient)" stroke="none" />
    <polyline points="${line}" fill="none" stroke="var(--primary)" stroke-width="2" />
  `;
  const hoverDot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  hoverDot.setAttribute("r", "4");
  hoverDot.setAttribute("fill", "var(--primary)");
  hoverDot.setAttribute("stroke", "white");
  hoverDot.setAttribute("stroke-width", "2");
  hoverDot.style.opacity = "0";
  svg.appendChild(hoverDot);
  svg.addEventListener("mousemove", (event) => {
    const box = svg.getBoundingClientRect();
    const x = Math.max(padding, Math.min(box.width - padding, event.clientX - box.left));
    const ratio = (x - padding) / Math.max(1, (box.width - padding * 2));
    const index = Math.round(ratio * (points.length - 1));
    const clamped = Math.max(0, Math.min(points.length - 1, index));
    const point = points[clamped];
    const item = series[clamped];
    const scaleX = box.width / width;
    const scaleY = box.height / height;
    hoverDot.setAttribute("cx", point.x);
    hoverDot.setAttribute("cy", point.y);
    hoverDot.style.opacity = "1";
    tooltip.textContent = `${formatDateTime(item.ts)} ? ${item.count || 0}`;
    tooltip.style.left = `${(point.x * scaleX) / box.width * 100}%`;
    tooltip.style.top = `${(point.y * scaleY) / box.height * 100}%`;
    tooltip.classList.add("show");
  });
  svg.addEventListener("mouseleave", () => {
    hoverDot.style.opacity = "0";
    tooltip.classList.remove("show");
  });
  container.appendChild(svg);
}

function renderTrends() {
  const trendsMeta = state.dashboardSectionMeta?.trends || null;
  const series = state.trends || [];
  renderTrendChart(series, trendsMeta);
  if (!elements.trendSummary) {
    return;
  }
  if (isUnavailableMeta(trendsMeta)) {
    elements.trendSummary.textContent = "趋势数据暂不可用。";
    return;
  }
  if (!series.length) {
    elements.trendSummary.textContent = "暂无趋势数据。";
    return;
  }
  const total = series.reduce((sum, item) => sum + (item.count || 0), 0);
  const peak = Math.max(...series.map((item) => item.count || 0), 0);
  const windowSec = state.trendsMeta?.window || series.length * 3600;
  const days = Math.round(windowSec / 86400);
  elements.trendSummary.textContent = `最近 ${days || 1} 天：累计 ${total} 次认领，峰值为每小时 ${peak} 次。`;
}

async function loadDashboardSummary(options = {}) {
  return summarySync.loadDashboardSummary(options);
}

function formatKeyStatus(status) {
  if (status === "active") {
    return "可用";
  }
  if (status === "revoked") {
    return "已删除";
  }
  return status || "-";
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (error) {
    return false;
  }
}

function showModal(title, message) {
  let overlay = document.getElementById("modal-overlay");
  if (overlay) {
    overlay.remove();
  }
  overlay = document.createElement("div");
  overlay.id = "modal-overlay";
  overlay.className = "modal-overlay";
  overlay.innerHTML = `
    <div class="modal">
      <div class="modal-title"></div>
      <div class="modal-body"></div>
      <div class="modal-actions">
        <button class="btn btn-primary modal-confirm" type="button">确认</button>
      </div>
    </div>
  `;
  overlay.querySelector(".modal-title").textContent = title;
  overlay.querySelector(".modal-body").textContent = message;
  const confirmBtn = overlay.querySelector(".modal-confirm");
  confirmBtn.addEventListener("click", () => {
    overlay.remove();
  });
  document.body.appendChild(overlay);
  requestAnimationFrame(() => {
    overlay.classList.add("show");
  });
}

function syncClaimButtonState() {
  if (!elements.claimBtn) {
    return;
  }
  const busy = state.isClaimSubmitting || state.isClaimQueued;
  elements.claimBtn.disabled = busy;
  elements.claimBtn.classList.toggle("is-loading", busy);
  elements.claimBtn.setAttribute("aria-busy", busy ? "true" : "false");
  if (state.isClaimQueued) {
    elements.claimBtn.textContent = "排队中...";
    return;
  }
  elements.claimBtn.textContent = state.isClaimSubmitting ? "申请中..." : "申请账号";
}

function setClaimSubmitting(submitting) {
  state.isClaimSubmitting = submitting;
  syncClaimButtonState();
}

function setClaimQueued(queued) {
  state.isClaimQueued = queued;
  syncClaimButtonState();
}

function mergeClaimResultsOptimistically(items = []) {
  const incoming = Array.isArray(items) ? items.filter((item) => item && typeof item === "object") : [];
  if (!incoming.length) {
    return;
  }

  const hiddenIds = state.pendingHiddenClaimIds || new Set();
  const seenKeys = new Set();
  const merged = [];
  [...incoming, ...(state.claimResults || [])].forEach((item, index) => {
    const claimId = item?.claim_id;
    if (claimId != null && hiddenIds.has(claimId)) {
      return;
    }
    const dedupeKey = claimId != null
      ? `claim:${claimId}`
      : `fallback:${item?.token_id ?? ""}:${item?.file_name ?? ""}:${index}`;
    if (seenKeys.has(dedupeKey)) {
      return;
    }
    seenKeys.add(dedupeKey);
    merged.push(item);
  });

  state.claimsInitialized = true;
  state.claimResults = merged;
  state.claimSelected.clear();
  state.lastClaimTotal = merged.length;
}

function setClaimDownloadAllBusy(busy, count = 0) {
  if (!elements.claimDownloadAll) {
    return;
  }
  elements.claimDownloadAll.disabled = busy;
  elements.claimDownloadAll.classList.toggle("is-loading", busy);
  elements.claimDownloadAll.textContent = busy
    ? (count > 0 ? `正在打包 ${count} 个账号...` : "正在打包...")
    : CLAIM_DOWNLOAD_ALL_LABEL;
}

function renderApiKeys() {
  elements.apiKeyList.innerHTML = "";
  const payload = state.apiKeysState || { items: [], limit: 0, active: 0, loaded: false };
  const keys = (payload.items || []).filter((key) => key.status === "active");
  elements.apiKeyLimit.textContent = `可用 API Key：${payload.active || keys.length} / ${payload.limit || 0}`;
  if (!keys.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "暂无 API Key";
    elements.apiKeyList.appendChild(empty);
    return;
  }

  keys.forEach((key) => {
    const item = document.createElement("div");
    item.className = "key-item";
    const statusLabel = formatKeyStatus(key.status);
    const keyDisplay = key.token ? `完整 Key：${key.token}` : `Key 前缀：${key.prefix}`;
    item.innerHTML = `
      <div class="key-main">
        <div class="key-title">${key.name || "API Key"}</div>
        <div class="key-meta">${keyDisplay}</div>
        <div class="key-meta">${statusLabel} · ${key.created_at}</div>
      </div>
      <div class="key-actions">
        <button class="btn btn-outline btn-inline" data-key-copy="${key.id}">复制 Key</button>
        <button class="btn btn-outline btn-inline btn-danger" data-key-id="${key.id}">删除</button>
      </div>
    `;
    const revokeBtn = item.querySelector("button[data-key-id]");
    const copyBtn = item.querySelector("button[data-key-copy]");
    revokeBtn.addEventListener("click", () => revokeApiKey(key.id));
    copyBtn.disabled = !key.token;
    copyBtn.addEventListener("click", async () => {
      if (!key.token) {
        return;
      }
      const ok = await copyText(key.token);
      if (ok) {
        copyBtn.textContent = "已复制";
        setTimeout(() => {
          copyBtn.textContent = "复制 Key";
        }, 1200);
      }
    });
    elements.apiKeyList.appendChild(item);
  });
}

function applyApiKeySummary(summary) {
  state.apiKeysState = {
    ...state.apiKeysState,
    limit: summary?.limit || 0,
    active: summary?.active || 0,
  };
}

function applyApiKeysPayload(payload) {
  state.apiKeysState = {
    items: payload?.items || [],
    limit: payload?.limit || 0,
    active: payload?.active || 0,
    loaded: true,
  };
  if (state.activeTab === "keys") {
    renderApiKeys();
  }
}

function renderUploadPolicy() {
  if (!elements.uploadPolicy) {
    return;
  }
  const policy = state.uploadPolicy;
  if (!policy) {
    elements.uploadPolicy.textContent = "上传限制：-";
    return;
  }
  elements.uploadPolicy.textContent =
    `上传限制：单次 ${policy.max_files_per_request} 个 / 单文件 ${formatBytes(policy.max_file_size_bytes)} / ` +
    `每小时最多上传： ${policy.max_success_per_hour} 个 / 最低信任 ${policy.min_trust_level}`;
}

function renderUploadSelected() {
  const list = elements.uploadSelectedList;
  if (!list) {
    return;
  }
  list.innerHTML = "";
  if (!state.uploadQueue.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "还没有选择文件。";
    list.appendChild(empty);
    return;
  }
  state.uploadQueue.forEach((file, index) => {
    const item = document.createElement("div");
    item.className = "upload-selected-item";
    item.innerHTML = `
      <div>
        <div class="upload-selected-name">${escapeHtml(file.name)}</div>
        <div class="upload-selected-meta">${formatBytes(file.size)}</div>
      </div>
      <button class="btn btn-ghost btn-inline" type="button" data-upload-remove="${index}">移除</button>
    `;
    item.querySelector("[data-upload-remove]")?.addEventListener("click", () => {
      state.uploadQueue = state.uploadQueue.filter((_, currentIndex) => currentIndex !== index);
      renderUploadSelected();
      setUploadSubmitting(false);
    });
    list.appendChild(item);
  });
}

function uploadStatusLabel(status) {
  const map = {
    queued: "排队中",
    processing: "处理中",
    accepted: "已入库",
    duplicate: "重复账号",
    invalid_json: "JSON 非法",
    missing_fields: "字段缺失",
    invalid_file: "文件无效",
    file_too_large: "文件过大",
    banned_401: "账号失效",
    probe_timeout: "探活超时",
    probe_failed: "探活失败",
    rate_limited: "额度不足",
    db_busy: "数据库繁忙",
  };
  return map[status] || status || "-";
}

function normalizeUploadEvents(events) {
  if (!Array.isArray(events)) {
    return [];
  }
  return events.filter((event) => event && typeof event === "object");
}

function currentUploadStageLabel(item = {}) {
  return item.stage_label || uploadStatusLabel(item.status);
}

function buildUploadProgressMeta(item = {}) {
  const meta = [];
  const queuePosition = Number(item.queue_position);
  const queueTotal = Number(item.queue_total);
  if (
    item.status === "queued" &&
    Number.isFinite(queuePosition) &&
    queuePosition > 0 &&
    Number.isFinite(queueTotal) &&
    queueTotal > 0
  ) {
    meta.push(`等待 ${queuePosition}/${queueTotal}`);
  }
  if (item.updated_at) {
    meta.push(`更新于 ${formatDateTime(item.updated_at)}`);
  }
  if (item.account_id) {
    meta.push(`账号 ${item.account_id}`);
  }
  return meta;
}

function renderUploadResults() {
  const list = elements.uploadResults;
  if (!list) {
    return;
  }
  list.innerHTML = "";
  if (!state.uploadResults.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "上传完成后会在这里显示每个文件的校验结果。";
    list.appendChild(empty);
    return;
  }
  state.uploadResults.forEach((item) => {
    const stageLabel = currentUploadStageLabel(item);
    const detail = item.detail || item.reason || "-";
    const events = normalizeUploadEvents(item.events).slice(-4);
    const meta = buildUploadProgressMeta(item);
    const row = document.createElement("div");
    row.className = "upload-result-item";
    row.innerHTML = `
      <div class="upload-result-main">
        <div class="upload-result-header">
          <div class="grow">
            <div class="upload-selected-name">${escapeHtml(item.file_name || "-")}</div>
            <div class="upload-result-stage">${escapeHtml(stageLabel)}</div>
          </div>
          <div class="upload-result-status is-${item.status}">
            <span>${uploadStatusLabel(item.status)}</span>
          </div>
        </div>
        <div class="upload-selected-meta">${escapeHtml(detail)}</div>
        ${meta.length ? `
          <div class="upload-result-meta-row">
            ${meta.map((entry) => `<span class="upload-result-meta-chip">${escapeHtml(entry)}</span>`).join("")}
          </div>
        ` : ""}
        ${events.length ? `
          <div class="upload-result-events">
            ${events.map((event) => `
              <div class="upload-result-event">
                <div class="upload-result-event-label">${escapeHtml(event.label || event.stage || "-")}</div>
                <div class="upload-result-event-detail">${escapeHtml(event.detail || "-")}</div>
                <div class="upload-result-event-time">${escapeHtml(event.at ? formatDateTime(event.at) : "-")}</div>
              </div>
            `).join("")}
          </div>
        ` : ""}
      </div>
    `;
    list.appendChild(row);
  });
}

function applyUploadResultsPayload(payload) {
  state.uploadResultsDirty = false;
  state.uploadResultsLoaded = true;
  state.uploadResults = payload.items || [];
  state.uploadHistory = payload.history || [];
  applyUploadResultsSummaryPayload(payload);
  renderUploadResults();
}

async function loadUploadResultsSnapshot(options = {}) {
  const payload = await fetchJson("/me/uploads/results");
  applyUploadResultsPayload(payload || {});
  if (options.broadcast) {
    broadcastUploadResultsChanged(payload || {});
  }
  return payload;
}

function setUploadSubmitting(submitting) {
  state.isUploading = submitting;
  if (!elements.uploadSubmitBtn) {
    return;
  }
  elements.uploadSubmitBtn.disabled = submitting || !state.uploadQueue.length;
  elements.uploadSubmitBtn.classList.toggle("is-loading", submitting);
  elements.uploadSubmitBtn.textContent = submitting ? "正在加入队列..." : "开始上传";
}

function mergeUploadQueue(files) {
  const nextFiles = Array.from(files || []).filter(Boolean);
  if (!nextFiles.length) {
    return;
  }
  state.uploadQueue = [...state.uploadQueue, ...nextFiles];
  renderUploadSelected();
  setUploadSubmitting(false);
}

async function fileToBase64(file) {
  const buffer = await file.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    const chunk = bytes.subarray(index, index + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
}

async function uploadTokens() {
  if (state.isUploading || !state.uploadQueue.length) {
    return;
  }
  elements.uploadSummary.classList.add("hidden");
  elements.uploadError.classList.add("hidden");
  setUploadSubmitting(true);
  try {
    const files = await Promise.all(
      state.uploadQueue.map(async (file) => ({
        name: file.name,
        content_base64: await fileToBase64(file),
      }))
    );
    const payload = await fetchJson("/me/uploads/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Upload-Source": "web",
      },
      body: JSON.stringify({ files }),
    });
    applyUploadResultsPayload(payload || {});
    takePollingLeadership("upload");
    broadcastUploadResultsChanged(payload || {});
    syncUploadResultsTransport();
    const retryIndexes = new Set(
      state.uploadResults
        .filter((item) => item.status === "db_busy" && Number.isInteger(item.request_index))
        .map((item) => item.request_index)
    );
    if (retryIndexes.size) {
      state.uploadQueue = state.uploadQueue.filter((_, index) => retryIndexes.has(index));
    } else {
      state.uploadQueue = [];
    }
    renderUploadSelected();
    const followUpResults = await Promise.allSettled([
      refreshSummariesByLeader("upload-success"),
      loadUploadResultsSnapshot(),
    ]);
    followUpResults.forEach((result) => {
      if (result.status === "rejected") {
        handleAccessError(result.reason);
      }
    });
  } catch (error) {
    elements.uploadError.textContent = error.message;
    elements.uploadError.classList.remove("hidden");
  } finally {
    setUploadSubmitting(false);
  }
}

async function removeSelectedClaims() {
  if (!state.claimResults.length || !state.claimSelected.size) {
    return;
  }
  const ids = Array.from(state.claimSelected);
  const removedCount = state.claimResults.filter((item) => state.claimSelected.has(item.claim_id)).length;
  ids.forEach((id) => state.pendingHiddenClaimIds.add(id));
  state.claimResults = state.claimResults.filter((item) => !state.claimSelected.has(item.claim_id));
  state.claimSelected.clear();
  if (state.claims) {
    state.claims = {
      ...state.claims,
      total: Math.max(0, (state.claims.total || 0) - removedCount),
    };
    renderMyClaims();
  }
  renderClaimResults();
  fetchJson("/me/claims/hide", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ claim_ids: ids }),
  }).then(() => {
    refreshClaimsByLeader("claims-hidden").catch((error) => {
      state.claimsDirty = true;
      if (!handleAccessError(error)) {
        console.error("刷新领取记录失败", error);
      }
    });
  }).catch((error) => {
    ids.forEach((id) => state.pendingHiddenClaimIds.delete(id));
    elements.claimError.textContent = error.message;
    elements.claimError.classList.remove("hidden");
    refreshClaimsByLeader("claims-hide-recover").catch(() => {});
  });
}

function toggleClaimSelection(claimId, checked) {
  if (checked) {
    state.claimSelected.add(claimId);
  } else {
    state.claimSelected.delete(claimId);
  }
}

function isClaimActionTarget(target) {
  return Boolean(target.closest("button, a, input, label"));
}

function syncClaimMultiModeButton() {
  if (elements.claimMultiMode) {
    elements.claimMultiMode.classList.toggle("active", state.claimMultiMode);
    elements.claimMultiMode.setAttribute("aria-pressed", state.claimMultiMode ? "true" : "false");
  }
  if (elements.claimSelectAll) {
    elements.claimSelectAll.classList.toggle("hidden", !state.claimMultiMode);
  }
}

function renderClaimResults() {
  syncClaimMultiModeButton();
  elements.claimResults.innerHTML = "";
  const items = state.claimResults || [];
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "No claimed accounts yet.";
    elements.claimResults.appendChild(empty);
    return;
  }

  items.forEach((item, index) => {
    const card = document.createElement("div");
    const content = JSON.stringify(item.content, null, 2);
    const claimId = item.claim_id ?? index;
    const selected = state.claimSelected.has(claimId);
    const providerName = item.provider?.name || item.provider?.username || "-";
    const providerUsername = item.provider?.username || "-";
    card.className = "claim-card";
    card.classList.toggle("selectable", state.claimMultiMode);
    card.classList.toggle("selected", selected);
    card.innerHTML = `
  <div class="claim-card-header">
    <div>
      <div class="claim-file">#${index + 1} | ${escapeHtml(item.file_name)}</div>
      <div class="claim-meta">Path: ${escapeHtml(item.file_path)} | Encoding: ${escapeHtml(item.encoding)}</div>
      ${item.provider ? `<div class="claim-provider">该账号由用户 ${escapeHtml(providerName)} (@${escapeHtml(providerUsername)}) 提供，感谢支持</div>` : ""}
    </div>
    <div class="claim-actions">
      <span class="claim-selection-state ${selected ? "is-visible" : ""}">Selected</span>
      <button class="btn btn-outline btn-inline" data-claim-copy="${claimId}">复制账号 JSON</button>
      <button class="btn btn-outline btn-inline" data-claim-download="${claimId}" type="button">下载 JSON</button>
      <button class="btn btn-ghost btn-inline btn-danger" data-claim-remove="${claimId}">删除账号</button>
    </div>
  </div>
  <pre class="token-json">${escapeHtml(content)}</pre>
`;
    const copyBtn = card.querySelector(`button[data-claim-copy="${claimId}"]`);
    const removeBtn = card.querySelector(`button[data-claim-remove="${claimId}"]`);
    const downloadBtn = card.querySelector(`button[data-claim-download="${claimId}"]`);
    copyBtn.addEventListener("click", async () => {
      const ok = await copyText(content);
      if (ok) {
        copyBtn.textContent = "Copied";
        setTimeout(() => {
          copyBtn.textContent = "Copy JSON";
        }, 1200);
      }
    });
    downloadBtn?.addEventListener("click", async () => {
      await downloadClaimFile(item, downloadBtn);
    });
    card.addEventListener("click", (event) => {
      if (!state.claimMultiMode || isClaimActionTarget(event.target)) {
        return;
      }
      toggleClaimSelection(claimId, !state.claimSelected.has(claimId));
      renderClaimResults();
    });
    removeBtn.addEventListener("click", async () => {
      if (item.claim_id != null) {
        state.pendingHiddenClaimIds.add(item.claim_id);
      }
      state.claimResults = state.claimResults.filter((row) => row.claim_id !== item.claim_id);
      state.claimSelected.clear();
      if (item.claim_id != null && state.claims) {
        state.claims = {
          ...state.claims,
          total: Math.max(0, (state.claims.total || 0) - 1),
        };
        renderMyClaims();
      }
      renderClaimResults();
      if (item.claim_id != null) {
        fetchJson("/me/claims/hide", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ claim_ids: [item.claim_id] }),
        }).then(() => {
          refreshClaimsByLeader("claims-hidden").catch((error) => {
            state.claimsDirty = true;
            if (!handleAccessError(error)) {
              console.error("刷新领取记录失败", error);
            }
          });
        }).catch((error) => {
          state.pendingHiddenClaimIds.delete(item.claim_id);
          elements.claimError.textContent = error.message;
          elements.claimError.classList.remove("hidden");
          refreshClaimsByLeader("claims-hide-recover").catch(() => {});
        });
      }
    });
    elements.claimResults.appendChild(card);
  });
}

async function loadQueueStatus() {
  const payload = await fetchJson("/me/queue-status");
  applyQueueStatusPayload(payload || {});
  return payload;
}

async function loadDashboard() {
  return loadDashboardSummary();
}

async function loadAppShellData() {
  const [runtimeSnapshot, queueStatus] = await Promise.all([
    loadRuntimeSnapshot(),
    loadQueueStatus(),
  ]);
  return { runtimeSnapshot, queueStatus };
}

async function bootstrapAppShell() {
  if (state.bootstrapPromise) {
    return state.bootstrapPromise;
  }

  setLoadingState({
    title: "正在加载",
    subtitle: "正在确认当前会话并加载运行时数据。",
    message: "",
    showRetry: false,
    showSpinner: true,
  });
  showScreen("loading");

  state.bootstrapPromise = (async () => {
    try {
      applyDocsBaseUrl();
      await loadAppShellData();
      showScreen("app");
      switchTab("data");
      claimPollingLeadership();
      syncQueueRealtimeTransport();
      syncUploadResultsTransport();
      loadDashboard().catch((error) => {
        if (!handleAccessError(error)) {
          console.error("异步加载数据面板失败", error);
        }
      });
    } catch (error) {
      if (error?.status === 401) {
        stopAllBackgroundActivity("auth-expired");
        showScreen("login");
        setLoginMessage("", "error");
        if (state.authErrorParam) {
          setLoginMessage(`登录失败：${state.authErrorParam}`, "error");
        }
        return;
      }
      if (handleAccessError(error)) {
        return;
      }
      setLoadingState({
        title: "启动失败",
        subtitle: "首屏数据加载失败，当前不会误判为未登录。",
        message: describeBootstrapError(error),
        tone: "error",
        showRetry: true,
        showSpinner: false,
      });
      showScreen("loading");
    } finally {
      state.bootstrapPromise = null;
      if (elements.loadingRetryBtn) {
        elements.loadingRetryBtn.disabled = false;
      }
    }
  })();

  return state.bootstrapPromise;
}

async function loadRuntimeSnapshot(options = {}) {
  return summarySync.loadRuntimeSnapshot(options);
}

async function loadApiKeys() {
  const keys = await fetchJson("/me/api-keys");
  applyApiKeysPayload(keys);
}

async function ensureClaimsLoaded(force = false) {
  if (!force && state.claimsInitialized && !state.claimsDirty) {
    renderClaimResults();
    return;
  }
  if (!shouldRefreshClaimsDetails()) {
    state.claimsDirty = true;
    return;
  }
  await refreshClaimsByLeader(force ? "claim-view" : "claims-view");
}

async function ensureApiKeysLoaded(force = false) {
  if (!force && state.apiKeysState.loaded) {
    renderApiKeys();
    return;
  }
  await loadApiKeys();
}

async function createApiKey() {
  elements.apiKeyCreated.classList.add("hidden");
  const name = elements.apiKeyName.value.trim();
  const payload = name ? { name } : {};
  const apiKeys = await fetchJson("/me/api-keys", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  elements.apiKeyName.value = "";

  applyApiKeysPayload(apiKeys);
  applyRuntimeSnapshot({
    api_keys: {
      summary: {
        limit: apiKeys?.limit || 0,
        active: apiKeys?.active || 0,
      },
    },
  });
  const displayKey = apiKeys?.items?.[0]?.token || "-";
  elements.apiKeyCreated.textContent = `已创建 Key：${displayKey}`;
  elements.apiKeyCreated.classList.remove("hidden");
}

async function revokeApiKey(keyId) {
  const apiKeys = await fetchJson(`/me/api-keys/${keyId}/revoke`, { method: "POST" });
  applyApiKeysPayload(apiKeys);
  applyRuntimeSnapshot({
    api_keys: {
      summary: {
        limit: apiKeys?.limit || 0,
        active: apiKeys?.active || 0,
      },
    },
  });
}

async function claimTokens() {
  if (state.isClaimSubmitting || state.isClaimQueued) {
    return;
  }
  elements.claimSummary.classList.add("hidden");
  elements.claimError.classList.add("hidden");

  const count = Number.parseInt(elements.claimCount.value, 10);
  if (!Number.isFinite(count) || count < 1) {
    elements.claimError.textContent = "请输入有效数量。";
    elements.claimError.classList.remove("hidden");
    return;
  }

  setClaimSubmitting(true);
  setClaimQueued(false);
  state.activeClaimRequestId = "";
  elements.claimSummary.textContent = "正在申请账号，请稍候...";
  elements.claimSummary.classList.remove("hidden");
  logClaimClientEvent("submit click", { requested: count });

  try {
    const result = await fetchJson("/me/claim", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count, client_tab_id: state.tabId }),
    });
    const accepted = applyClaimAcceptedState(state.claimRealtimeState, result || {}, { tabId: state.tabId });
    state.claimRealtimeState = accepted.state;
    state.activeClaimRequestId = accepted.state.activeRequestId || "";
    updateClaimRequestUI(accepted.activeRequest, accepted.queueRequest);
    syncQueueRealtimeTransport();
    logClaimClientEvent("request accepted", {
      request_id: result?.request_id,
      status: result?.status,
      queued: Boolean(result?.queued),
    });
  } catch (error) {
    setClaimQueued(false);
    setClaimSubmitting(false);
    state.activeClaimRequestId = "";
    elements.claimSummary.classList.add("hidden");
    elements.claimError.textContent = error.message;
    elements.claimError.classList.remove("hidden");
  }
}

async function downloadClaimFile(item, trigger = null) {
  hideClaimErrorMessage();
  if (trigger) {
    trigger.disabled = true;
  }
  try {
    const fallbackFilename = normalizeDownloadFileName(item?.file_name, "claimed-token.json");
    try {
      const payload = buildClaimJsonDownload(item, fallbackFilename);
      triggerBrowserDownload(payload.blob, payload.filename);
    } catch (directDownloadError) {
      if (!item?.download_url) {
        throw directDownloadError;
      }
      await downloadFileWithValidation(item.download_url, {
        expectedContentType: "application/json",
        fallbackFilename,
      });
    }
  } catch (error) {
    if (!handleAccessError(error)) {
      showClaimErrorMessage(error.message || "下载账号 JSON 失败。");
    }
  } finally {
    if (trigger) {
      trigger.disabled = false;
    }
  }
}

async function downloadAllClaims() {
  hideClaimErrorMessage();
  const previousSkipNextClaimModal = state.skipNextClaimModal;
  setClaimDownloadAllBusy(true);
  try {
    state.skipNextClaimModal = true;
    try {
      await loadClaimsInternal({
        broadcast: hasCrossTabCoordination(),
      });
    } catch (error) {
      state.skipNextClaimModal = previousSkipNextClaimModal;
      throw error;
    }

    const items = Array.isArray(state.claimResults) ? state.claimResults.filter(Boolean) : [];
    if (!items.length) {
      throw new Error("当前没有可下载的账号。");
    }

    setClaimDownloadAllBusy(true, items.length);
    const payload = buildClaimsArchiveDownload(items);
    triggerBrowserDownload(payload.blob, payload.filename);
  } catch (error) {
    if (!handleAccessError(error)) {
      showClaimErrorMessage(error.message || "下载账号归档失败。");
    }
  } finally {
    setClaimDownloadAllBusy(false);
  }
}

async function clearClaimResults() {
  if (!state.claimResults.length) {
    return;
  }
  const ids = state.claimResults.map((item) => item.claim_id).filter((id) => id != null);
  ids.forEach((id) => state.pendingHiddenClaimIds.add(id));
  const removedCount = state.claimResults.length;
  state.claimResults = [];
  state.claimSelected.clear();
  if (state.claims) {
    state.claims = {
      ...state.claims,
      total: Math.max(0, (state.claims.total || 0) - removedCount),
    };
    renderMyClaims();
  }
  renderClaimResults();
  if (ids.length) {
    fetchJson("/me/claims/hide", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ claim_ids: ids }),
    }).then(() => {
      refreshClaimsByLeader("claims-hidden").catch((error) => {
        state.claimsDirty = true;
        if (!handleAccessError(error)) {
          console.error("刷新领取记录失败", error);
        }
      });
    }).catch((error) => {
      ids.forEach((id) => state.pendingHiddenClaimIds.delete(id));
      elements.claimError.textContent = error.message;
      elements.claimError.classList.remove("hidden");
      refreshClaimsByLeader("claims-hide-recover").catch(() => {});
    });
  }
}

async function logout() {
  try {
    await fetch("/auth/logout", { method: "POST" });
  } catch (error) {
    console.error("退出登录失败", error);
  }
  releasePollingLeadership("logout");
  stopAllBackgroundActivity("logout");
  showScreen("login");
}

function shouldRunBackgroundRefresh() {
  return !state.visibilityHidden && !elements.appScreen.classList.contains("hidden");
}

function canPerformPrimaryRefreshWork() {
  return !hasCrossTabCoordination() || state.isPollingLeader;
}

function resetCrossTabSummaryState(reason = "reset") {
  summarySync.resetSessionState(reason);
}

function applyRuntimeSnapshotEnvelope(payload, options = {}) {
  return summarySync.applyRuntimeSnapshotEnvelope(payload, options);
}

function applyDashboardSummaryEnvelope(payload, options = {}) {
  return summarySync.applyDashboardSummaryEnvelope(payload, options);
}

function flushSummaryRefreshRequests() {
  return summarySync.flushSummaryRefreshRequests();
}

function refreshSummariesByLeader(reason = "summary-sync", options = {}) {
  return summarySync.refreshSummariesByLeader(reason, options);
}

function handleLeaderStatusChange(change = {}) {
  if (change.isLeader) {
    if (!change.wasLeader) {
      scheduleLowFrequencyRefresh(0);
      if (state.claimsDirty || (shouldRefreshClaimsDetails() && !state.claimsInitialized)) {
        scheduleLeaderClaimsRefresh("leader-acquired", 0);
      }
    }
    summarySync.handleLeaderStatusChange(change);
    syncUploadResultsTransport();
    syncQueueRealtimeTransport();
    return;
  }
  summarySync.handleLeaderStatusChange(change);
  stopLowFrequencyRefresh();
  closeUploadResultsStream();
  clearUploadResultsPoller();
  stopQueueRealtime();
  clearClaimsRequestTimer();
}

function handleLeaderChanged(change = {}) {
  summarySync.handleLeaderChanged(change);
}

function handleBroadcastTransportFailure() {
  summarySync.resetTransportState();
}

function handleBroadcastTransportReset() {
  summarySync.resetTransportState();
}

function handleCrossTabMessage(message) {
  if (message.type === "dashboard_summary") {
    applyDashboardSummaryEnvelope(message.payload || {}, { requireVersion: true });
    return;
  }
  if (message.type === "runtime_snapshot") {
    applyRuntimeSnapshotEnvelope(message.payload || {}, { requireVersion: true });
    return;
  }
  if (message.type === "claim_realtime") {
    applyClaimRealtimePayload(message.payload || { requests: [] }, {
      broadcast: false,
      emitToasts: true,
    });
    return;
  }
  if (message.type === "upload_results_changed") {
    applyUploadResultsChanged(message.payload || {});
    return;
  }
  if (message.type === "claims_payload") {
    applyClaimsPayload(message.payload || {});
    return;
  }
  if (message.type === "claims_refresh_requested") {
    state.claimsDirty = true;
    if (state.isPollingLeader && state.currentLeader?.tabId === state.tabId) {
      scheduleLeaderClaimsRefresh("claims-requested", CLAIMS_REFRESH_DEBOUNCE_MS);
    }
    return;
  }
  if (message.type === "summary_refresh_requested") {
    summarySync.handleSummaryRefreshRequested(message.payload || {}, message);
  }
}




function stopLowFrequencyRefresh() {
  if (state.lowFrequencyTimer) {
    clearTimeout(state.lowFrequencyTimer);
    state.lowFrequencyTimer = null;
  }
}

function scheduleLowFrequencyRefresh(delayMs = LOW_FREQUENCY_REFRESH_MS) {
  stopLowFrequencyRefresh();
  if (!canPerformPrimaryRefreshWork() || !shouldRunBackgroundRefresh()) {
    return;
  }
  state.lowFrequencyTimer = setTimeout(() => {
    runLowFrequencyRefresh().catch((error) => {
      if (!handleAccessError(error)) {
        scheduleLowFrequencyRefresh(LOW_FREQUENCY_REFRESH_MS);
      }
    });
  }, Math.max(0, delayMs));
}

async function runLowFrequencyRefresh() {
  if (!canPerformPrimaryRefreshWork() || !shouldRunBackgroundRefresh() || state.refreshing) {
    return;
  }
  state.refreshing = true;
  try {
    await Promise.all([
      summarySync.loadRuntimeSnapshot({ broadcast: true }),
      loadQueueStatus(),
      loadDashboardSummary({ broadcast: true }),
    ]);
    state.lastLowFrequencyAt = Date.now();
  } catch (error) {
    if (handleAccessError(error)) {
      return;
    }
    throw error;
  } finally {
    state.refreshing = false;
    scheduleLowFrequencyRefresh(LOW_FREQUENCY_REFRESH_MS);
  }
}

function clearClaimStreamRetry() {
  if (state.claimStreamRetryTimer) {
    clearTimeout(state.claimStreamRetryTimer);
    state.claimStreamRetryTimer = null;
  }
}

function closeQueueStream() {
  if (state.queueStream) {
    state.queueStream.close();
    state.queueStream = null;
  }
}

function stopQueueRealtime() {
  clearClaimStreamRetry();
  closeQueueStream();
  state.claimStreamConnected = false;
}

function canUseLeaderQueueTransport() {
  return Boolean(
    canPerformPrimaryRefreshWork() &&
    !state.visibilityHidden &&
    hasPendingClaimRealtimeRequests()
  );
}

function syncQueueRealtimeTransport() {
  if (!canUseLeaderQueueTransport()) {
    state.claimStreamError = "";
    stopQueueRealtime();
    return;
  }
  if (state.queueStream) {
    return;
  }
  startQueueRealtime();
}

function scheduleClaimStreamReconnect() {
  clearClaimStreamRetry();
  if (!canUseLeaderQueueTransport()) {
    return;
  }
  const delay = Math.min(
    CLAIM_STREAM_RETRY_MAX_MS,
    CLAIM_STREAM_RETRY_BASE_MS * (2 ** Math.max(0, state.claimStreamRetryCount))
  );
  state.claimStreamRetryTimer = setTimeout(() => {
    state.claimStreamRetryTimer = null;
    startQueueRealtime();
  }, delay);
}

function startQueueRealtime() {
  if (!canUseLeaderQueueTransport()) {
    stopQueueRealtime();
    return;
  }
  if (typeof EventSource === "undefined") {
    state.claimStreamConnected = false;
    state.claimStreamError = "当前浏览器不支持实时推送连接。";
    updateClaimRequestUI();
    return;
  }
  if (state.queueStream) {
    return;
  }
  const stream = new EventSource("/me/queue-stream");
  state.queueStream = stream;
  stream.addEventListener("stream_status", () => {
    state.claimStreamConnected = true;
    state.claimStreamError = "";
    state.claimStreamRetryCount = 0;
    updateClaimRequestUI();
  });
  stream.addEventListener("claim_snapshot", (event) => {
    try {
      const payload = JSON.parse(event.data || "{}");
      applyClaimRealtimePayload(payload || { requests: [] }, {
        fromStream: true,
        broadcast: true,
        emitToasts: true,
      });
    } catch (error) {
      console.error("解析领取实时事件失败", error);
    }
  });
  stream.onerror = () => {
    if (state.queueStream !== stream) {
      return;
    }
    closeQueueStream();
    state.claimStreamConnected = false;
    state.claimStreamRetryCount += 1;
    state.claimStreamError = "实时连接中断，正在重连...";
    updateClaimRequestUI();
    scheduleClaimStreamReconnect();
  };
}

function clearUploadResultsPoller() {
  if (state.uploadPollTimer) {
    clearTimeout(state.uploadPollTimer);
    state.uploadPollTimer = null;
  }
}

function setLoadingState({
  title = "正在加载",
  subtitle = "正在请求数据面板并确认当前会话。",
  message = "",
  tone = "error",
  showRetry = false,
  showSpinner = true,
} = {}) {
  if (elements.loadingTitle) {
    elements.loadingTitle.textContent = title;
  }
  if (elements.loadingSubtitle) {
    elements.loadingSubtitle.textContent = subtitle;
  }
  if (elements.loadingMessage) {
    elements.loadingMessage.textContent = message;
    elements.loadingMessage.dataset.tone = tone;
    elements.loadingMessage.classList.toggle("hidden", !message);
  }
  if (elements.loadingSpinner) {
    elements.loadingSpinner.classList.toggle("hidden", !showSpinner);
  }
  if (elements.loadingRetryBtn) {
    elements.loadingRetryBtn.classList.toggle("hidden", !showRetry);
    elements.loadingRetryBtn.disabled = Boolean(state.bootstrapPromise);
  }
}

function describeBootstrapError(error, fallback = "无法加载数据面板，请稍后重试。") {
  if (!error) {
    return fallback;
  }
  const message = String(error.message || "").trim();
  if (!message) {
    return fallback;
  }
  const lower = message.toLowerCase();
  if (
    lower === "failed to fetch" ||
    lower.includes("networkerror") ||
    lower.includes("load failed") ||
    lower.includes("network request failed")
  ) {
    return "无法连接服务器，请确认服务已启动且当前网络可访问。";
  }
  return message;
}

function closeUploadResultsStream() {
  if (state.uploadStream) {
    state.uploadStream.close();
    state.uploadStream = null;
  }
}

function shouldPollUploadResults() {
  const summary = state.uploadResultsSummary || {};
  return (summary.queued || 0) > 0 || (summary.processing || 0) > 0;
}

function canUseLeaderUploadTransport() {
  return !state.visibilityHidden && canPerformPrimaryRefreshWork() && shouldPollUploadResults();
}

function startUploadResultsStream() {
  if (!canUseLeaderUploadTransport()) {
    closeUploadResultsStream();
    return false;
  }
  if (typeof EventSource === "undefined") {
    closeUploadResultsStream();
    return false;
  }
  if (state.uploadStream) {
    return true;
  }
  clearUploadResultsPoller();
  const stream = new EventSource("/me/uploads/stream");
  state.uploadStream = stream;
  stream.addEventListener("upload_results", (event) => {
    try {
      const payload = JSON.parse(event.data || "{}");
      state.uploadPollFailureCount = 0;
      applyUploadResultsPayload(payload || {});
      broadcastUploadResultsChanged(payload || {});
    } catch (error) {
      console.error("解析上传进度事件失败", error);
    }
  });
  stream.onerror = () => {
    if (state.uploadStream !== stream) {
      return;
    }
    closeUploadResultsStream();
    state.uploadPollFailureCount += 1;
    syncUploadResultsPolling();
  };
  return true;
}

function syncUploadResultsTransport() {
  if (!canUseLeaderUploadTransport()) {
    closeUploadResultsStream();
    clearUploadResultsPoller();
    return;
  }
  if (startUploadResultsStream()) {
    return;
  }
  syncUploadResultsPolling();
}

function syncUploadResultsPolling() {
  clearUploadResultsPoller();
  if (!canUseLeaderUploadTransport() || state.uploadStream) {
    return;
  }
  const delay = Math.min(
    QUEUE_POLL_MAX_BACKOFF_MS,
    QUEUE_POLL_FAST_MS * (2 ** Math.max(0, state.uploadPollFailureCount))
  );
  state.uploadPollTimer = setTimeout(async () => {
    try {
      await loadUploadResultsSnapshot({ broadcast: true });
      state.uploadPollFailureCount = 0;
    } catch (error) {
      state.uploadPollFailureCount += 1;
      if (handleAccessError(error)) {
        return;
      }
    }
    syncUploadResultsPolling();
  }, delay);
}

function stopAllBackgroundActivity(reason = "stop") {
  if (state.isPollingLeader) {
    releasePollingLeadership(reason);
  }
  disposeLeaderSync();
  stopLowFrequencyRefresh();
  stopQueueRealtime();
  closeUploadResultsStream();
  clearUploadResultsPoller();
  clearClaimsRequestTimer();
  resetCrossTabSummaryState(reason);
}

function bindEvents() {
  initBroadcastChannel();
  if (!state.leaderMonitorTimer) {
    state.leaderMonitorTimer = setInterval(() => {
      if (!state.isPollingLeader) {
        claimPollingLeadership();
      }
    }, LEADER_HEARTBEAT_MS);
  }
  if (elements.loginBtn) {
    elements.loginBtn.addEventListener("click", () => {
      window.location.href = "/auth/linuxdo/login";
    });
  }
  if (elements.loadingRetryBtn) {
    elements.loadingRetryBtn.addEventListener("click", () => {
      bootstrapAppShell().catch(() => {});
    });
  }
  if (elements.logoutBtn) {
    elements.logoutBtn.addEventListener("click", logout);
  }
  if (elements.bannedLogoutBtn) {
    elements.bannedLogoutBtn.addEventListener("click", logout);
  }
  if (elements.tabData) {
    elements.tabData.addEventListener("click", () => switchTab("data"));
  }
  if (elements.tabKeys) {
    elements.tabKeys.addEventListener("click", () => switchTab("keys"));
  }
  if (elements.tabClaim) {
    elements.tabClaim.addEventListener("click", () => switchTab("claim"));
  }
  if (elements.tabUpload) {
    elements.tabUpload.addEventListener("click", () => switchTab("upload"));
  }
  if (elements.tabDocs) {
    elements.tabDocs.addEventListener("click", () => switchTab("docs"));
  }
  if (elements.apiKeyCreateBtn) {
    elements.apiKeyCreateBtn.addEventListener("click", createApiKey);
  }
  if (elements.claimBtn) {
    elements.claimBtn.addEventListener("click", claimTokens);
  }
  if (elements.claimMultiMode) {
    elements.claimMultiMode.addEventListener("click", () => {
      state.claimMultiMode = !state.claimMultiMode;
      if (!state.claimMultiMode) {
        state.claimSelected.clear();
      }
      renderClaimResults();
    });
  }
  if (elements.claimSelectAll) {
    elements.claimSelectAll.addEventListener("click", () => {
      state.claimSelected = new Set(
        (state.claimResults || []).map((item, index) => item.claim_id ?? index)
      );
      renderClaimResults();
    });
  }
  if (elements.claimDownloadAll) {
    elements.claimDownloadAll.addEventListener("click", downloadAllClaims);
  }
  if (elements.claimRemoveSelected) {
    elements.claimRemoveSelected.addEventListener("click", removeSelectedClaims);
  }
  if (elements.claimClear) {
    elements.claimClear.addEventListener("click", clearClaimResults);
  }
  if (elements.uploadSelectBtn && elements.uploadInput) {
    elements.uploadSelectBtn.addEventListener("click", () => elements.uploadInput.click());
  }
  if (elements.uploadInput) {
    elements.uploadInput.addEventListener("change", (event) => {
      mergeUploadQueue(event.target.files);
      elements.uploadInput.value = "";
    });
  }
  if (elements.uploadSubmitBtn) {
    elements.uploadSubmitBtn.addEventListener("click", uploadTokens);
  }
  if (elements.uploadDropzone) {
    ["dragenter", "dragover"].forEach((type) => {
      elements.uploadDropzone.addEventListener(type, (event) => {
        event.preventDefault();
        elements.uploadDropzone.classList.add("is-dragover");
      });
    });
    ["dragleave", "drop"].forEach((type) => {
      elements.uploadDropzone.addEventListener(type, (event) => {
        event.preventDefault();
        elements.uploadDropzone.classList.remove("is-dragover");
      });
    });
    elements.uploadDropzone.addEventListener("drop", (event) => {
      mergeUploadQueue(event.dataTransfer?.files);
    });
    elements.uploadDropzone.addEventListener("click", (event) => {
      if (event.target.closest("button")) {
        return;
      }
      elements.uploadInput?.click();
    });
    elements.uploadDropzone.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        elements.uploadInput?.click();
      }
    });
  }
  document.addEventListener("visibilitychange", () => {
    state.visibilityHidden = document.visibilityState !== "visible";
    if (state.visibilityHidden) {
      releasePollingLeadership("hidden");
      stopLowFrequencyRefresh();
      stopQueueRealtime();
      closeUploadResultsStream();
      clearUploadResultsPoller();
      clearClaimsRequestTimer();
      return;
    }
    claimPollingLeadership();
    if (state.activeTab === "claim") {
      ensureClaimsLoaded(true).catch(() => {});
    }
    syncQueueRealtimeTransport();
    syncUploadResultsTransport();
  });
  window.addEventListener("beforeunload", () => {
    releasePollingLeadership("unload");
    if (state.leaderMonitorTimer) {
      clearInterval(state.leaderMonitorTimer);
      state.leaderMonitorTimer = null;
    }
    disposeLeaderSync();
    stopAllBackgroundActivity("unload");
  });
}

async function init() {
  bindEvents();
  renderUploadSelected();
  renderUploadResults();
  setUploadSubmitting(false);
  updateSummaryOriginBadge();
  const url = new URL(window.location.href);
  state.authErrorParam = url.searchParams.get("auth_error") || "";
  await bootstrapAppShell();

  if (state.authErrorParam) {
    url.searchParams.delete("auth_error");
    window.history.replaceState({}, document.title, `${url.pathname}${url.search}`);
    state.authErrorParam = "";
  }
}

init();
