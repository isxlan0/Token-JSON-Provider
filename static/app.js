const state = {
  user: null,
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
  refreshTimer: null,
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
  refreshCounter: 0,
  pendingHiddenClaimIds: new Set(),
  isClaimSubmitting: false,
  isClaimQueued: false,
  uploadPolicy: null,
  uploadQueue: [],
  uploadResults: [],
  uploadHistory: [],
  isUploading: false,
  activeTab: "data",
};

const elements = {
  loadingScreen: document.getElementById("loading-screen"),
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
  authSummary: document.getElementById("auth-summary"),
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

  if (name === "claim" || name === "upload") {
    refreshAll();
  }
  if (name === "keys") {
    ensureApiKeysLoaded().catch((error) => {
      if (!handleAccessError(error)) {
        console.error("加载 API Keys 失败", error);
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

async function loadClaims() {
  const payload = await fetchJson("/me/claims");
  const hiddenIds = state.pendingHiddenClaimIds || new Set();
  const serverIds = new Set((payload.items || []).map((item) => item.claim_id));
  Array.from(hiddenIds).forEach((id) => {
    if (!serverIds.has(id)) {
      hiddenIds.delete(id);
    }
  });
  const items = (payload.items || []).filter((item) => !hiddenIds.has(item.claim_id));
  const total = items.length;
  if (state.claimsInitialized) {
    const lastTotal = state.lastClaimTotal ?? total;
    const delta = total - lastTotal;
    if (delta > 0 && !state.skipNextClaimModal) {
      showModal("账号已到账", `共 ${delta} 个账号，可能来自排队自动发放或其他会话。`);
    }
  }
  state.skipNextClaimModal = false;
  state.lastClaimTotal = total;
  state.claimsInitialized = true;
  state.claimResults = items;
  state.claimSelected.clear();
  renderClaimResults();
}

async function loadQueueStatus() {
  const payload = await fetchJson("/me/queue-status");
  state.queueStatus = payload;
  renderQueueStatus();
}


function applyDocsBaseUrl() {
  const baseUrl = window.location.origin;
  const nodes = document.querySelectorAll('.endpoint-block pre');
  nodes.forEach((node) => {
    node.textContent = node.textContent.replace(/\$\{TOKEN_PROVIDER_BASE_URL\}/g, baseUrl);
  });
}

function renderQueueStatus() {
  const status = state.queueStatus;
  if (!status || !status.queued) {
    state.isClaimQueued = false;
    syncClaimButtonState();
    if (state.queueSticky) {
      elements.claimSummary.classList.add("hidden");
      state.queueSticky = false;
    }
    return;
  }
  state.isClaimQueued = true;
  syncClaimButtonState();
  const position = status.position ?? "-";
  const total = status.total_queued ?? "-";
  const available = status.available_tokens ?? "-";
  const remaining = status.remaining ?? status.requested ?? "-";
  elements.claimSummary.textContent =
    `已进入排队（第 ${position}/${total} 位，待领取 ${remaining}）。` +
    `当前可领取库存 ${available} 次，库存会优先发给前面排队用户。` +
    "系统每隔 5 秒自动刷新。";
  elements.claimSummary.classList.remove("hidden");
  state.queueSticky = true;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    cache: "no-store",
    credentials: "same-origin",
    ...options,
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
      is_admin: Boolean(state.user?.is_admin),
    };
    state.user = bannedUser;
    renderBannedUser(bannedUser);
    if (state.refreshTimer) {
      clearInterval(state.refreshTimer);
      state.refreshTimer = null;
    }
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
    elements.authSummary.textContent = "未登录";
    return;
  }
  elements.userName.textContent = state.user.name || state.user.username;
  elements.userUsername.textContent = state.user.username || "-";
  elements.userId.textContent = state.user.id || "-";
  elements.userTrust.textContent = state.user.trust_level ?? "-";
  elements.authSummary.textContent = `LinuxDo / ${state.user.name || state.user.username}`;
}

function renderQuota() {
  if (!state.quota) {
    elements.quotaUsed.textContent = "0";
    elements.quotaRemaining.textContent = "0";
    elements.quotaLimit.textContent = "0";
    return;
  }
  elements.quotaUsed.textContent = state.quota.used;
  elements.quotaRemaining.textContent = state.quota.remaining;
  elements.quotaLimit.textContent = state.quota.limit;
}

function renderStats() {
  if (!state.stats) {
    elements.inventoryAvailable.textContent = "0";
    elements.inventoryTotal.textContent = "0";
    elements.claimsOthersTotal.textContent = "0";
    elements.claimsOthersUnique.textContent = "0";
    elements.claimsTotal.textContent = "0";
    elements.claimsUnique.textContent = "0";
    return;
  }
  elements.inventoryAvailable.textContent = state.stats.available_tokens;
  elements.inventoryTotal.textContent = state.stats.total_tokens;
  elements.claimsOthersTotal.textContent = state.stats.others_claimed_total;
  elements.claimsOthersUnique.textContent = state.stats.others_claimed_unique;
  elements.claimsTotal.textContent = state.stats.claimed_total;
  elements.claimsUnique.textContent = state.stats.claimed_unique;
}

function renderMyClaims() {
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

function renderRankedList(list, items, emptyText) {
  if (!list) {
    return;
  }
  list.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = emptyText;
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

function renderTimedList(list, items, emptyText, timeField) {
  if (!list) {
    return;
  }
  list.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = emptyText;
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
  renderRankedList(elements.leaderboardList, state.leaderboard || [], "No leaderboard data.");
}

function renderRecentClaims() {
  renderTimedList(elements.recentClaimsList, state.recentClaims || [], "No recent claims.", "claimed_at");
}

function renderContributorLeaderboard() {
  renderRankedList(
    elements.contributorLeaderboardList,
    state.contributorLeaderboard || [],
    "No contributor data."
  );
}

function renderRecentContributors() {
  renderTimedList(
    elements.recentContributorsList,
    state.recentContributors || [],
    "No recent contributors.",
    "uploaded_at"
  );
}

function renderSystemStatus() {
  const status = state.systemStatus;
  if (!status) {
    if (elements.inventoryStatus) {
      elements.inventoryStatus.textContent = "-";
      elements.inventoryStatus.className = "status-pill";
    }
    return;
  }
  const inventory = status.inventory || {};
  const health = status.health || {};
  const total = inventory.total ?? 0;
  const available = inventory.available ?? 0;
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
    elements.systemStatusValue.textContent = "在线";
  }
  if (elements.systemStatusQueue) {
    elements.systemStatusQueue.textContent = status.queue?.total ?? 0;
  }
  if (elements.systemStatusIndex) {
    elements.systemStatusIndex.textContent = formatDateTime(status.index?.updated_at);
  }
}

function renderTrendChart(series) {
  const container = elements.trendChart;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!series || !series.length) {
    const empty = document.createElement("div");
    empty.className = "trend-empty";
    empty.textContent = "No trend data.";
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
  const series = state.trends || [];
  renderTrendChart(series);
  if (!elements.trendSummary) {
    return;
  }
  if (!series.length) {
    elements.trendSummary.textContent = "No trend data.";
    return;
  }
  const total = series.reduce((sum, item) => sum + (item.count || 0), 0);
  const peak = Math.max(...series.map((item) => item.count || 0), 0);
  const windowSec = state.trendsMeta?.window || series.length * 3600;
  const days = Math.round(windowSec / 86400);
  elements.trendSummary.textContent = `最近 ${days || 1} 天：累计 ${total} 次认领，峰值为每小时 ${peak} 次。`;
}

async function loadDashboardSummary() {
  try {
    const summary = await fetchJson(
      "/dashboard/summary?window=7d&bucket=1h&leaderboard_window=24h&leaderboard_limit=10&recent_limit=10&contributor_limit=10&recent_contributor_limit=10"
    );
    state.stats = summary.stats || null;
    state.leaderboard = summary.leaderboard?.items || [];
    state.recentClaims = summary.recent?.items || [];
    state.contributorLeaderboard = summary.contributors?.items || [];
    state.recentContributors = summary.recent_contributors?.items || [];
    state.trends = summary.trends?.series || [];
    state.trendsMeta = summary.trends
      ? { window: summary.trends.window, bucket: summary.trends.bucket }
      : null;
    state.systemStatus = summary.system || null;
  } catch (error) {
    return;
  }
  renderStats();
  renderLeaderboard();
  renderRecentClaims();
  renderContributorLeaderboard();
  renderRecentContributors();
  renderSystemStatus();
  renderTrends();
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
    const row = document.createElement("div");
    row.className = "upload-result-item";
    row.innerHTML = `
      <div class="upload-result-main">
        <div class="upload-selected-name">${escapeHtml(item.file_name || "-")}</div>
        <div class="upload-selected-meta">${escapeHtml(item.reason || "-")}</div>
      </div>
      <div class="upload-result-status is-${item.status}">
        <span>${uploadStatusLabel(item.status)}</span>
      </div>
    `;
    list.appendChild(row);
  });
}

function applyUploadResultsPayload(payload) {
  state.uploadResults = payload.items || [];
  state.uploadHistory = [];
  renderUploadResults();
  const summary = payload.summary || {};
  elements.uploadSummary.textContent =
    `本次共处理 ${summary.total ?? 0} 个文件，成功 ${summary.accepted ?? 0} 个，重复 ${summary.duplicates ?? 0} 个` +
    `${summary.queued ? `，排队中 ${summary.queued} 个` : ""}` +
    `${summary.processing ? `，处理中 ${summary.processing} 个` : ""}` +
    `${summary.db_busy ? `，数据库繁忙 ${summary.db_busy} 个` : ""}。`;
  if ((summary.total ?? 0) > 0) {
    elements.uploadSummary.classList.remove("hidden");
  } else {
    elements.uploadSummary.classList.add("hidden");
  }
}

async function loadUploadResultsSnapshot() {
  const payload = await fetchJson("/me/uploads/results");
  applyUploadResultsPayload(payload || {});
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
      loadDashboardSummary(),
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
  }).catch((error) => {
    ids.forEach((id) => state.pendingHiddenClaimIds.delete(id));
    elements.claimError.textContent = error.message;
    elements.claimError.classList.remove("hidden");
    loadClaimSummary().catch(() => {});
    loadClaims().catch(() => {});
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
      <a class="btn btn-outline btn-inline" data-claim-download="${claimId}" target="_blank" rel="noopener noreferrer">下载 JSON</a>
      <button class="btn btn-ghost btn-inline btn-danger" data-claim-remove="${claimId}">删除账号</button>
    </div>
  </div>
  <pre class="token-json">${escapeHtml(content)}</pre>
`;
    const copyBtn = card.querySelector(`button[data-claim-copy="${claimId}"]`);
    const removeBtn = card.querySelector(`button[data-claim-remove="${claimId}"]`);
    const downloadLink = card.querySelector(`a[data-claim-download="${claimId}"]`);
    if (downloadLink) {
      downloadLink.href = item.download_url || "#";
    }
    copyBtn.addEventListener("click", async () => {
      const ok = await copyText(content);
      if (ok) {
        copyBtn.textContent = "Copied";
        setTimeout(() => {
          copyBtn.textContent = "Copy JSON";
        }, 1200);
      }
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
        }).catch((error) => {
          state.pendingHiddenClaimIds.delete(item.claim_id);
          elements.claimError.textContent = error.message;
          elements.claimError.classList.remove("hidden");
          loadClaimSummary().catch(() => {});
          loadClaims().catch(() => {});
        });
      }
    });
    elements.claimResults.appendChild(card);
  });
}

async function loadDashboard() {
  const me = await fetchJson("/me");
  state.user = me.user;
  state.quota = me.quota;
  state.claims = me.claims;
  state.uploadPolicy = me.uploads || null;
  applyApiKeySummary(me.api_keys || {});
  renderUser();
  renderQuota();
  renderMyClaims();
  renderUploadPolicy();
}

async function loadQuotaSummary() {
  const quota = await fetchJson("/me/quota");
  state.quota = quota;
  renderQuota();
}

async function loadClaimSummary() {
  const claims = await fetchJson("/me/claims-summary");
  state.claims = claims;
  renderMyClaims();
}

async function loadApiKeySummary() {
  const summary = await fetchJson("/me/api-key-summary");
  applyApiKeySummary(summary || {});
}

async function loadApiKeys() {
  const keys = await fetchJson("/me/api-keys");
  applyApiKeysPayload(keys);
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
  const displayKey = apiKeys?.items?.[0]?.token || "-";
  elements.apiKeyCreated.textContent = `已创建 Key：${displayKey}`;
  elements.apiKeyCreated.classList.remove("hidden");
}

async function revokeApiKey(keyId) {
  const apiKeys = await fetchJson(`/me/api-keys/${keyId}/revoke`, { method: "POST" });
  applyApiKeysPayload(apiKeys);
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
  elements.claimSummary.textContent = "正在申请账号，请稍候...";
  elements.claimSummary.classList.remove("hidden");

  try {
    const result = await fetchJson("/me/claim", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count }),
    });
    if (result.granted && result.granted > 0) {
      state.skipNextClaimModal = true;
    }
    await loadClaims();
    state.quota = result.quota || state.quota;
    renderQuota();
    if (result.granted && result.granted > 0) {
      state.claims = {
        ...(state.claims || {}),
        total: (state.claims?.total || 0) + result.granted,
      };
      renderMyClaims();
      if (state.stats) {
        state.stats = {
          ...state.stats,
          available_tokens: Math.max(0, (state.stats.available_tokens || 0) - result.granted),
          claimed_total: (state.stats.claimed_total || 0) + result.granted,
        };
        renderStats();
      }
    }
    if (result.queued) {
      setClaimQueued(true);
      state.queueStatus = {
        queued: true,
        position: result.queue_position,
        remaining: result.queue_remaining,
        requested: result.requested,
      };
      renderQueueStatus();
    } else {
      setClaimQueued(false);
      state.queueStatus = { queued: false };
      state.queueSticky = false;
      elements.claimSummary.textContent = `已领取 ${result.granted} / 请求 ${result.requested}，本小时剩余 ${result.quota.remaining}`;
      elements.claimSummary.classList.remove("hidden");
      if (result.granted && result.granted > 0) {
        showModal("申请成功", `共 ${result.granted} 个账号`);
      }
    }
    renderClaimResults();
    await loadQueueStatus();
  } catch (error) {
    setClaimQueued(false);
    elements.claimSummary.classList.add("hidden");
    elements.claimError.textContent = error.message;
    elements.claimError.classList.remove("hidden");
  } finally {
    setClaimSubmitting(false);
  }
}

function downloadAllClaims() {
  window.open("/me/claims/archive", "_blank");
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
    }).catch((error) => {
      ids.forEach((id) => state.pendingHiddenClaimIds.delete(id));
      elements.claimError.textContent = error.message;
      elements.claimError.classList.remove("hidden");
      loadClaimSummary().catch(() => {});
      loadClaims().catch(() => {});
    });
  }
}

async function logout() {
  try {
    await fetch("/auth/logout", { method: "POST" });
  } catch (error) {
    console.error("退出登录失败", error);
  }
  if (state.refreshTimer) {
    clearInterval(state.refreshTimer);
    state.refreshTimer = null;
  }
  showScreen("login");
}

async function refreshAll() {
  if (state.refreshing) {
    return;
  }
  state.refreshing = true;
  try {
    state.refreshCounter += 1;
    const shouldRefreshClaims = state.refreshCounter % 4 === 0;
    const results = await Promise.allSettled([
      loadDashboardSummary(),
      loadQueueStatus(),
      loadQuotaSummary(),
      loadApiKeySummary(),
      loadUploadResultsSnapshot(),
      ...(shouldRefreshClaims ? [loadClaimSummary(), loadClaims()] : []),
    ]);
    const accessHandled = results.some((result) => {
      if (result.status !== "rejected") {
        return false;
      }
      return handleAccessError(result.reason);
    });
    if (accessHandled) {
      return;
    }
    if (shouldRefreshClaims) {
      // ignore refresh errors
    }
  } catch (error) {
    if (!handleAccessError(error)) {
      // ignore refresh errors
    }
  } finally {
    state.refreshing = false;
  }
}

function startAutoRefresh() {
  if (state.refreshTimer) {
    clearInterval(state.refreshTimer);
  }
  state.refreshTimer = setInterval(refreshAll, 5000);
}

function bindEvents() {
  if (elements.loginBtn) {
    elements.loginBtn.addEventListener("click", () => {
      window.location.href = "/auth/linuxdo/login";
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
}

async function init() {
  bindEvents();
  renderUploadSelected();
  renderUploadResults();
  setUploadSubmitting(false);
  showScreen("loading");
  elements.summaryOrigin.textContent = `来源：${window.location.origin}`;
  const url = new URL(window.location.href);
  const authError = url.searchParams.get("auth_error");

  try {
    applyDocsBaseUrl();
    const status = await fetchJson("/auth/status");
    if (!status.authenticated) {
      if (state.refreshTimer) {
        clearInterval(state.refreshTimer);
        state.refreshTimer = null;
      }
      showScreen("login");
      if (authError) {
        setLoginMessage(`登录失败：${authError}`, "error");
      }
    } else if (status.user?.is_banned) {
      state.user = status.user;
      renderBannedUser(status.user);
      showScreen("banned");
    } else {
      showScreen("app");
      switchTab("data");
      await Promise.all([
        loadDashboard(),
        loadDashboardSummary(),
        loadClaims(),
        loadQueueStatus(),
        loadUploadResultsSnapshot(),
      ]);
      startAutoRefresh();
    }
  } catch (error) {
    if (error.status === 401) {
      if (state.refreshTimer) {
        clearInterval(state.refreshTimer);
        state.refreshTimer = null;
      }
      showScreen("login");
      if (authError) {
        setLoginMessage(`登录失败：${authError}`, "error");
      }
    } else if (!handleAccessError(error)) {
      setLoginMessage(error.message, "error");
      showScreen("login");
    }
  }

  if (authError) {
    url.searchParams.delete("auth_error");
    window.history.replaceState({}, document.title, `${url.pathname}${url.search}`);
  }
}

init();
