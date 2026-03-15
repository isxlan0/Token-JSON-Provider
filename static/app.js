const state = {
  user: null,
  quota: null,
  stats: null,
  apiKeys: [],
  claimResults: [],
  claimSelected: new Set(),
  refreshTimer: null,
  refreshing: false,
  queueStatus: null,
  queueSticky: false,
  lastClaimTotal: null,
  claimsInitialized: false,
  skipNextClaimModal: false,
};

const elements = {
  loginScreen: document.getElementById("login-screen"),
  appScreen: document.getElementById("app-screen"),
  loginBtn: document.getElementById("linuxdo-login-btn"),
  loginMessage: document.getElementById("login-message"),
  logoutBtn: document.getElementById("logout-btn"),
  summaryOrigin: document.getElementById("summary-origin"),
  authSummary: document.getElementById("auth-summary"),
  tabData: document.getElementById("tab-data"),
  tabKeys: document.getElementById("tab-keys"),
  tabClaim: document.getElementById("tab-claim"),
  tabDocs: document.getElementById("tab-docs"),
  viewData: document.getElementById("view-data"),
  viewKeys: document.getElementById("view-keys"),
  viewClaim: document.getElementById("view-claim"),
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
  apiKeyName: document.getElementById("api-key-name"),
  apiKeyCreateBtn: document.getElementById("api-key-create-btn"),
  apiKeyCreated: document.getElementById("api-key-created"),
  apiKeyLimit: document.getElementById("api-key-limit"),
  apiKeyList: document.getElementById("api-key-list"),
  claimCount: document.getElementById("claim-count"),
  claimBtn: document.getElementById("claim-btn"),
  claimDownloadAll: document.getElementById("claim-download-all"),
  claimRemoveSelected: document.getElementById("claim-remove-selected"),
  claimClear: document.getElementById("claim-clear"),
  claimSummary: document.getElementById("claim-summary"),
  claimError: document.getElementById("claim-error"),
  claimResults: document.getElementById("claim-results"),
};

function showLoggedIn(loggedIn) {
  if (loggedIn) {
    elements.loginScreen.classList.add("hidden");
    elements.appScreen.classList.remove("hidden");
  } else {
    elements.appScreen.classList.add("hidden");
    elements.loginScreen.classList.remove("hidden");
  }
}

function switchTab(name) {
  const views = [
    { key: "data", tab: elements.tabData, view: elements.viewData },
    { key: "keys", tab: elements.tabKeys, view: elements.viewKeys },
    { key: "claim", tab: elements.tabClaim, view: elements.viewClaim },
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
      docs: "API 使用指南",
    };
    headerTitle.textContent = map[name] || "数据面板";
  }

  if (name === "claim") {
    refreshAll();
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
  const items = payload.items || [];
  const total = items.length;
  if (state.claimsInitialized) {
    const lastTotal = state.lastClaimTotal ?? total;
    const delta = total - lastTotal;
    if (delta > 0 && !state.skipNextClaimModal) {
      showModal("领取成功", `共 ${delta} 个账号`);
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

function renderQueueStatus() {
  const status = state.queueStatus;
  if (!status || !status.queued) {
    if (state.queueSticky) {
      elements.claimSummary.classList.add("hidden");
      state.queueSticky = false;
    }
    return;
  }
  const position = status.position ?? "-";
  const total = status.total_queued ?? "-";
  const available = status.available_tokens ?? "-";
  const remaining = status.remaining ?? status.requested ?? "-";
  elements.claimSummary.textContent =
    `已进入排队（第 ${position}/${total} 位，待领取 ${remaining}）。` +
    `当前可用库存 ${available}，库存会优先发给前面排队用户。` +
    "系统每隔 15 秒自动刷新。";
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
    throw error;
  }
  return payload;
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
  const total = state.claims?.total ?? 0;
  elements.myClaimsTotal.textContent = total;
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

function renderApiKeys(limit) {
  elements.apiKeyList.innerHTML = "";
  const keys = (state.apiKeys || []).filter((key) => key.status === "active");
  elements.apiKeyLimit.textContent = `可用 API Key：${keys.length} / ${limit}`;
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

async function removeSelectedClaims() {
  if (!state.claimResults.length || !state.claimSelected.size) {
    return;
  }
  const ids = Array.from(state.claimSelected);
  await fetchJson("/me/claims/hide", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ claim_ids: ids }),
  });
  state.claimResults = state.claimResults.filter((item) => !state.claimSelected.has(item.claim_id));
  state.claimSelected.clear();
  renderClaimResults();
}

function toggleClaimSelection(claimId, checked) {
  if (checked) {
    state.claimSelected.add(claimId);
  } else {
    state.claimSelected.delete(claimId);
  }
}

function renderClaimResults() {
  elements.claimResults.innerHTML = "";
  const items = state.claimResults || [];
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "暂无领取记录";
    elements.claimResults.appendChild(empty);
    return;
  }

  items.forEach((item, index) => {
    const card = document.createElement("div");
    card.className = "claim-card";
    const content = JSON.stringify(item.content, null, 2);
    const claimId = item.claim_id ?? index;
    const checked = state.claimSelected.has(claimId) ? "checked" : "";
    card.innerHTML = `
      <div class="claim-card-header">
        <div>
          <div class="claim-file">#${index + 1} · ${item.file_name}</div>
          <div class="claim-meta">${item.file_path} · ${item.encoding}</div>
        </div>
        <div class="claim-actions">
          <label class="claim-select" title="选择">
            <input type="checkbox" data-claim-select="${claimId}" ${checked} />
          </label>
          <button class="btn btn-outline btn-inline" data-claim-copy="${claimId}">复制 JSON</button>
          <a class="btn btn-outline btn-inline" href="${item.download_url}" target="_blank" rel="noopener noreferrer">下载</a>
          <button class="btn btn-ghost btn-inline btn-danger" data-claim-remove="${claimId}">删除（标记隐藏）</button>
        </div>
      </div>
      <pre class="token-json">${content}</pre>
    `;
    const copyBtn = card.querySelector(`button[data-claim-copy="${claimId}"]`);
    const selectInput = card.querySelector(`input[data-claim-select="${claimId}"]`);
    const removeBtn = card.querySelector(`button[data-claim-remove="${claimId}"]`);
    copyBtn.addEventListener("click", async () => {
      const ok = await copyText(content);
      if (ok) {
        copyBtn.textContent = "已复制";
        setTimeout(() => {
          copyBtn.textContent = "复制 JSON";
        }, 1200);
      }
    });
    selectInput.addEventListener("change", (event) => {
      toggleClaimSelection(claimId, event.target.checked);
    });
    removeBtn.addEventListener("click", async () => {
      if (item.claim_id != null) {
        await fetchJson("/me/claims/hide", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ claim_ids: [item.claim_id] }),
        });
      }
      state.claimResults = state.claimResults.filter((row) => row.claim_id !== item.claim_id);
      state.claimSelected.clear();
      renderClaimResults();
    });
    elements.claimResults.appendChild(card);
  });
}

async function loadDashboard() {
  const me = await fetchJson("/me");
  state.user = me.user;
  state.quota = me.quota;
  state.claims = me.claims;
  renderUser();
  renderQuota();
  renderMyClaims();

  const stats = await fetchJson("/dashboard/stats");
  state.stats = stats;
  renderStats();

  const keys = await fetchJson("/me/api-keys");
  state.apiKeys = keys.items || [];
  renderApiKeys(keys.limit || 0);
}

async function createApiKey() {
  elements.apiKeyCreated.classList.add("hidden");
  const name = elements.apiKeyName.value.trim();
  const payload = name ? { name } : {};
  const created = await fetchJson("/me/api-keys", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  elements.apiKeyName.value = "";

  const displayKey = created.token || "-";
  elements.apiKeyCreated.textContent = `已创建 Key：${displayKey}`;
  elements.apiKeyCreated.classList.remove("hidden");
  await loadDashboard();
}

async function revokeApiKey(keyId) {
  await fetchJson(`/me/api-keys/${keyId}/revoke`, { method: "POST" });
  state.apiKeys = (state.apiKeys || []).filter((key) => key.id !== keyId);
  renderApiKeys(state.apiKeys.length || 0);
}

async function claimTokens() {
  elements.claimSummary.classList.add("hidden");
  elements.claimError.classList.add("hidden");

  const count = Number.parseInt(elements.claimCount.value, 10);
  if (!Number.isFinite(count) || count < 1) {
    elements.claimError.textContent = "请输入有效数量。";
    elements.claimError.classList.remove("hidden");
    return;
  }

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
    if (result.queued) {
      state.queueStatus = {
        queued: true,
        position: result.queue_position,
        remaining: result.queue_remaining,
        requested: result.requested,
      };
      renderQueueStatus();
    } else {
      state.queueStatus = { queued: false };
      state.queueSticky = false;
      elements.claimSummary.textContent = `已领取 ${result.granted} / 请求 ${result.requested}，本小时剩余 ${result.quota.remaining}`;
      elements.claimSummary.classList.remove("hidden");
      if (result.granted && result.granted > 0) {
        showModal("领取成功", `共 ${result.granted} 个账号`);
      }
    }
    renderClaimResults();
    await loadDashboard();
    await loadQueueStatus();
  } catch (error) {
    elements.claimError.textContent = error.message;
    elements.claimError.classList.remove("hidden");
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
  if (ids.length) {
    await fetchJson("/me/claims/hide", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ claim_ids: ids }),
    });
  }
  state.claimResults = [];
  state.claimSelected.clear();
  renderClaimResults();
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
  showLoggedIn(false);
}

async function refreshAll() {
  if (state.refreshing) {
    return;
  }
  state.refreshing = true;
  try {
    await loadDashboard();
    await loadClaims();
    await loadQueueStatus();
  } catch (error) {
    // ignore refresh errors
  } finally {
    state.refreshing = false;
  }
}

function startAutoRefresh() {
  if (state.refreshTimer) {
    clearInterval(state.refreshTimer);
  }
  state.refreshTimer = setInterval(refreshAll, 15000);
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
  if (elements.tabData) {
    elements.tabData.addEventListener("click", () => switchTab("data"));
  }
  if (elements.tabKeys) {
    elements.tabKeys.addEventListener("click", () => switchTab("keys"));
  }
  if (elements.tabClaim) {
    elements.tabClaim.addEventListener("click", () => switchTab("claim"));
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
  if (elements.claimDownloadAll) {
    elements.claimDownloadAll.addEventListener("click", downloadAllClaims);
  }
  if (elements.claimRemoveSelected) {
    elements.claimRemoveSelected.addEventListener("click", removeSelectedClaims);
  }
  if (elements.claimClear) {
    elements.claimClear.addEventListener("click", clearClaimResults);
  }
}

async function init() {
  bindEvents();
  elements.summaryOrigin.textContent = `来源：${window.location.origin}`;

  try {
    const status = await fetchJson("/auth/status");
    if (!status.authenticated) {
      if (state.refreshTimer) {
        clearInterval(state.refreshTimer);
        state.refreshTimer = null;
      }
      showLoggedIn(false);
      return;
    }
    showLoggedIn(true);
    switchTab("data");
    await loadDashboard();
    await loadClaims();
    await loadQueueStatus();
    startAutoRefresh();
  } catch (error) {
    if (error.status === 401) {
      if (state.refreshTimer) {
        clearInterval(state.refreshTimer);
        state.refreshTimer = null;
      }
      showLoggedIn(false);
      return;
    }
    setLoginMessage(error.message, "error");
  }

  const url = new URL(window.location.href);
  const authError = url.searchParams.get("auth_error");
  if (authError) {
    setLoginMessage(`登录失败：${authError}`, "error");
    url.searchParams.delete("auth_error");
    window.history.replaceState({}, document.title, `${url.pathname}${url.search}`);
  }
}

init();
