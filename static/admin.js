const state = {
  authErrorParam: "",
  bootstrapPromise: null,
  monitorRefreshPromise: null,
  monitorTimer: null,
  user: null,
  me: null,
  users: [],
  selectedUserId: null,
  userDetail: null,
  bans: [],
  tokens: [],
  queue: [],
  policy: null,
  activeTab: "users",
  queueOnly: "all",
  usersPage: { offset: 0, limit: 50, total: 0 },
  bansPage: { offset: 0, limit: 50, total: 0 },
  tokensPage: { offset: 0, limit: 50, total: 0 },
  queuePage: { offset: 0, limit: 50, total: 0 },
};

const ADMIN_MONITOR_REFRESH_MS = 10_000;

const elements = {
  loadingScreen: document.getElementById("admin-loading-screen"),
  loadingSpinner: document.getElementById("admin-loading-spinner"),
  loadingTitle: document.getElementById("admin-loading-title"),
  loadingSubtitle: document.getElementById("admin-loading-subtitle"),
  loadingMessage: document.getElementById("admin-loading-message"),
  loadingRetryBtn: document.getElementById("admin-loading-retry-btn"),
  loginScreen: document.getElementById("admin-login-screen"),
  deniedScreen: document.getElementById("admin-denied-screen"),
  appScreen: document.getElementById("admin-app-screen"),
  loginBtn: document.getElementById("admin-login-btn"),
  loginMessage: document.getElementById("admin-login-message"),
  deniedMessage: document.getElementById("admin-denied-message"),
  deniedLogoutBtn: document.getElementById("admin-denied-logout-btn"),
  logoutBtn: document.getElementById("admin-logout-btn"),
  authSummary: document.getElementById("admin-auth-summary"),
  policySummary: document.getElementById("admin-policy-summary"),
  headerTitle: document.getElementById("admin-header-title"),
  tabUsers: document.getElementById("admin-tab-users"),
  tabBans: document.getElementById("admin-tab-bans"),
  tabTokens: document.getElementById("admin-tab-tokens"),
  tabQueue: document.getElementById("admin-tab-queue"),
  tabPolicy: document.getElementById("admin-tab-policy"),
  viewUsers: document.getElementById("admin-view-users"),
  viewBans: document.getElementById("admin-view-bans"),
  viewTokens: document.getElementById("admin-view-tokens"),
  viewQueue: document.getElementById("admin-view-queue"),
  viewPolicy: document.getElementById("admin-view-policy"),
  userSearch: document.getElementById("admin-user-search"),
  userFilter: document.getElementById("admin-user-filter"),
  userLimit: document.getElementById("admin-user-limit"),
  userRefresh: document.getElementById("admin-user-refresh"),
  userList: document.getElementById("admin-user-list"),
  userPager: document.getElementById("admin-user-pager"),
  userDetail: document.getElementById("admin-user-detail"),
  banSearch: document.getElementById("admin-ban-search"),
  banFilter: document.getElementById("admin-ban-filter"),
  banLimit: document.getElementById("admin-ban-limit"),
  banRefresh: document.getElementById("admin-ban-refresh"),
  banList: document.getElementById("admin-ban-list"),
  banPager: document.getElementById("admin-ban-pager"),
  tokenSearch: document.getElementById("admin-token-search"),
  tokenFilter: document.getElementById("admin-token-filter"),
  tokenLimit: document.getElementById("admin-token-limit"),
  tokenRefresh: document.getElementById("admin-token-refresh"),
  tokenCleanupFiles: document.getElementById("admin-token-cleanup-files"),
  tokenCleanupDb: document.getElementById("admin-token-cleanup-db"),
  tokenList: document.getElementById("admin-token-list"),
  tokenPager: document.getElementById("admin-token-pager"),
  queueSearch: document.getElementById("admin-queue-search"),
  queueStatus: document.getElementById("admin-queue-status"),
  queueLimit: document.getElementById("admin-queue-limit"),
  queueShowAll: document.getElementById("admin-queue-show-all"),
  queueShowAbnormal: document.getElementById("admin-queue-show-abnormal"),
  queueShowTimeout: document.getElementById("admin-queue-show-timeout"),
  queueRefresh: document.getElementById("admin-queue-refresh"),
  queueList: document.getElementById("admin-queue-list"),
  queuePager: document.getElementById("admin-queue-pager"),
  policyPanel: document.getElementById("admin-policy-panel"),
  notice: document.getElementById("admin-notice"),
};

function showScreen(name) {
  elements.loadingScreen.classList.toggle("hidden", name !== "loading");
  elements.loginScreen.classList.toggle("hidden", name !== "login");
  elements.deniedScreen.classList.toggle("hidden", name !== "denied");
  elements.appScreen.classList.toggle("hidden", name !== "app");
  if (name === "app") {
    startAdminMonitor();
    refreshTokenMonitoring({ silent: true }).catch(() => {});
    return;
  }
  stopAdminMonitor();
}

function setLoginMessage(message = "", tone = "error") {
  elements.loginMessage.textContent = message;
  elements.loginMessage.dataset.tone = tone;
  elements.loginMessage.classList.toggle("hidden", !message);
}

function setLoadingState({
  title = "正在加载后台",
  subtitle = "正在请求后台数据并确认管理员权限。",
  message = "",
  tone = "error",
  showRetry = false,
  showSpinner = true,
} = {}) {
  elements.loadingTitle.textContent = title;
  elements.loadingSubtitle.textContent = subtitle;
  elements.loadingMessage.textContent = message;
  elements.loadingMessage.dataset.tone = tone;
  elements.loadingMessage.classList.toggle("hidden", !message);
  elements.loadingSpinner.classList.toggle("hidden", !showSpinner);
  elements.loadingRetryBtn.classList.toggle("hidden", !showRetry);
  elements.loadingRetryBtn.disabled = Boolean(state.bootstrapPromise);
}

function describeStartupError(error, fallback = "无法加载后台数据，请稍后重试。") {
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

function showNotice(message = "", tone = "success") {
  if (!elements.notice) {
    return;
  }
  elements.notice.textContent = message;
  elements.notice.className = `admin-notice ${tone}`;
  elements.notice.classList.toggle("hidden", !message);
}

function formatUserLabel(username, id) {
  return username ? `@${username} (${id})` : String(id);
}

function getUserLabelFromDetail() {
  const user = state.userDetail?.user;
  return formatUserLabel(user?.username, state.selectedUserId);
}

function setButtonPending(button, pendingText) {
  if (!button) {
    return null;
  }
  const originalText = button.textContent;
  button.disabled = true;
  button.dataset.pending = "true";
  if (pendingText) {
    button.textContent = pendingText;
  }
  return () => {
    button.disabled = false;
    button.dataset.pending = "false";
    button.textContent = originalText;
  };
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

function formatDurationSeconds(value) {
  const total = Math.max(0, Number(value || 0));
  if (!total) {
    return "0 秒";
  }
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = Math.floor(total % 60);
  const parts = [];
  if (days) {
    parts.push(`${days} 天`);
  }
  if (hours) {
    parts.push(`${hours} 小时`);
  }
  if (minutes) {
    parts.push(`${minutes} 分钟`);
  }
  if (!parts.length || (!days && !hours && seconds)) {
    parts.push(`${seconds} 秒`);
  }
  return parts.join(" ");
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

function handleAdminAuthError(error) {
  if (error?.status === 401) {
    stopAdminMonitor();
    setLoginMessage("", "error");
    showScreen("login");
    return true;
  }
  if (error?.status === 403) {
    stopAdminMonitor();
    elements.deniedMessage.textContent = error.message;
    showScreen("denied");
    return true;
  }
  return false;
}

function stopAdminMonitor() {
  if (state.monitorTimer) {
    window.clearInterval(state.monitorTimer);
    state.monitorTimer = null;
  }
}

function startAdminMonitor() {
  if (state.monitorTimer) {
    return;
  }
  state.monitorTimer = window.setInterval(() => {
    if (document.hidden || state.bootstrapPromise || elements.appScreen.classList.contains("hidden")) {
      return;
    }
    refreshTokenMonitoring({ silent: true }).catch((error) => {
      if (!handleAdminAuthError(error)) {
        console.error("admin background refresh failed", error);
      }
    });
  }, ADMIN_MONITOR_REFRESH_MS);
}

async function refreshTokenMonitoring({ resetOffset = false, silent = false } = {}) {
  if (state.monitorRefreshPromise) {
    return state.monitorRefreshPromise;
  }
  state.monitorRefreshPromise = (async () => {
    try {
      const tasks = [loadPolicy()];
      const shouldRefreshTokens = state.activeTab === "tokens" || state.tokens.length > 0;
      const shouldRefreshQueue = state.activeTab === "queue" || state.queue.length > 0;
      if (shouldRefreshTokens) {
        tasks.push(loadTokens(resetOffset && state.activeTab === "tokens"));
      }
      if (shouldRefreshQueue) {
        tasks.push(loadQueue(resetOffset && state.activeTab === "queue"));
      }
      await Promise.all(tasks);
    } catch (error) {
      if (!handleAdminAuthError(error)) {
        throw error;
      }
    } finally {
      state.monitorRefreshPromise = null;
    }
  })();

  try {
    await state.monitorRefreshPromise;
  } catch (error) {
    if (!silent) {
      showNotice(error.message, "error");
    }
    throw error;
  }
}

function switchTab(name) {
  state.activeTab = name;
  const views = [
    { key: "users", tab: elements.tabUsers, view: elements.viewUsers, title: "用户管理" },
    { key: "bans", tab: elements.tabBans, view: elements.viewBans, title: "封禁记录" },
    { key: "tokens", tab: elements.tabTokens, view: elements.viewTokens, title: "Token 管理" },
    { key: "queue", tab: elements.tabQueue, view: elements.viewQueue, title: "排队管理" },
    { key: "policy", tab: elements.tabPolicy, view: elements.viewPolicy, title: "额度策略" },
  ];
  views.forEach((item) => {
    const active = item.key === name;
    item.tab.classList.toggle("active", active);
    item.view.classList.toggle("active", active);
    if (active) {
      elements.headerTitle.textContent = item.title;
    }
  });
  if (name === "tokens" || name === "queue" || name === "policy") {
    refreshTokenMonitoring({ silent: true }).catch(() => {});
  }
}

function renderAuthSummary() {
  if (!state.user) {
    elements.authSummary.textContent = "-";
    return;
  }
  const banText = state.user.is_banned ? " · 当前账号已被封禁，但仍可进入后台" : "";
  elements.authSummary.textContent = `LinuxDo / ${state.user.name || state.user.username} (@${state.user.username})${banText}`;
}

function renderPolicySummary() {
  if (!state.me?.policy) {
    elements.policySummary.textContent = "策略 -";
    return;
  }
  const policy = state.me.policy;
  elements.policySummary.textContent = `策略 ${policy.status} · 每小时 ${policy.hourly_limit} · 排队 ${policy.system?.queue?.total ?? 0}`;
}

function renderPager(container, page, onPageChange) {
  if (!container) {
    return;
  }
  const total = Number(page.total || 0);
  const limit = Math.max(1, Number(page.limit || 50));
  const offset = Math.max(0, Number(page.offset || 0));
  const current = Math.floor(offset / limit) + 1;
  const pages = Math.max(1, Math.ceil(Math.max(total, 1) / limit));
  const prevDisabled = offset <= 0 ? "disabled" : "";
  const nextDisabled = offset + limit >= total ? "disabled" : "";
  container.classList.remove("hidden");
  container.innerHTML = `
    <button class="btn btn-outline btn-inline" data-page="prev" ${prevDisabled}>上一页</button>
    <span>第 ${current} / ${pages} 页，共 ${total} 条</span>
    <button class="btn btn-outline btn-inline" data-page="next" ${nextDisabled}>下一页</button>
  `;
  container.querySelector('[data-page="prev"]')?.addEventListener("click", () => {
    if (offset <= 0) {
      return;
    }
    onPageChange(Math.max(0, offset - limit));
  });
  container.querySelector('[data-page="next"]')?.addEventListener("click", () => {
    if (offset + limit >= total) {
      return;
    }
    onPageChange(offset + limit);
  });
}

function renderUsers() {
  elements.userList.innerHTML = "";
  if (!state.users.length) {
    elements.userList.innerHTML = '<div class="card admin-card empty-state">没有匹配的用户</div>';
  } else {
    state.users.forEach((item) => {
      const card = document.createElement("div");
      card.className = "admin-card";
      card.innerHTML = `
        <div class="admin-card-header">
          <div>
            <div class="admin-card-title">${item.name}</div>
            <div class="admin-card-meta mono">@${item.username} · ID ${item.linuxdo_user_id}</div>
          </div>
          <span class="admin-badge ${item.is_banned ? "active" : "normal"}">${item.is_banned ? "已封禁" : "正常"}</span>
        </div>
        <div class="admin-card-meta">最近登录：${formatDateTime(item.last_login_at)}</div>
        <div class="admin-card-meta">领取 ${item.claim_count} 次 · 活跃 Key ${item.active_api_keys}</div>
        <div class="admin-card-actions">
          <button class="btn btn-outline btn-inline" data-user-view="${item.linuxdo_user_id}">查看详情</button>
        </div>
      `;
      card.querySelector("[data-user-view]").addEventListener("click", () => loadUserDetail(item.linuxdo_user_id));
      elements.userList.appendChild(card);
    });
  }
  renderPager(elements.userPager, state.usersPage, (nextOffset) => {
    state.usersPage.offset = nextOffset;
    loadUsers();
  });
}

function renderUserDetail() {
  const detail = state.userDetail;
  if (!detail) {
    elements.userDetail.className = "card admin-detail empty-state";
    elements.userDetail.textContent = "选择一个用户查看详情";
    return;
  }
  const user = detail.user;
  const ban = detail.ban;
  const expiresLocalValue = ban?.expires_at ? new Date(ban.expires_at).toISOString().slice(0, 16) : "";
  const recentClaims = (detail.claims?.recent || [])
    .map((item) => `<div class="admin-card-meta mono">${formatDateTime(item.claimed_at)} · ${item.file_name}</div>`)
    .join("");
  elements.userDetail.className = "card admin-detail";
  elements.userDetail.innerHTML = `
    <div class="admin-detail-header">
      <div>
        <div class="admin-detail-title">${user.name}</div>
        <div class="admin-detail-meta mono">@${user.username} · ID ${user.linuxdo_user_id}</div>
      </div>
      <span class="admin-badge ${ban ? "active" : "normal"}">${ban ? "封禁中" : "正常"}</span>
    </div>
    <div class="admin-detail-grid">
      <div class="admin-stat"><div class="admin-stat-label">信任等级</div><div class="admin-stat-value">${user.trust_level}</div></div>
      <div class="admin-stat"><div class="admin-stat-label">领取总数</div><div class="admin-stat-value">${detail.claims.total}</div></div>
      <div class="admin-stat"><div class="admin-stat-label">去重领取</div><div class="admin-stat-value">${detail.claims.unique}</div></div>
      <div class="admin-stat"><div class="admin-stat-label">活跃 Key</div><div class="admin-stat-value">${detail.api_keys.active}</div></div>
    </div>
    <div class="admin-note">注册：${formatDateTime(user.created_at)} · 最近登录：${formatDateTime(user.last_login_at)}</div>
    <div class="admin-note">当前封禁：${ban ? `${ban.reason}（到期：${ban.expires_at ? formatDateTime(ban.expires_at) : "永久"}）` : "无"}</div>
    <div>
      <div class="admin-detail-title">最近领取</div>
      <div class="admin-list-plain">${recentClaims || '<div class="admin-note">暂无领取记录</div>'}</div>
    </div>
    <div>
      <div class="admin-detail-title">封禁操作</div>
      <div class="admin-ban-form">
        <textarea id="admin-ban-reason" placeholder="封禁原因（必填）">${ban?.reason || ""}</textarea>
        <input id="admin-ban-expires" type="datetime-local" value="${expiresLocalValue}">
        <div class="admin-inline-actions">
          <button id="admin-ban-submit" class="btn btn-primary btn-inline" type="button">保存封禁</button>
          <button id="admin-unban-submit" class="btn btn-outline btn-inline" type="button">解封用户</button>
        </div>
      </div>
    </div>
  `;
  elements.userDetail.querySelector("#admin-ban-submit").addEventListener("click", banSelectedUser);
  elements.userDetail.querySelector("#admin-unban-submit").addEventListener("click", unbanSelectedUser);
}

function renderBans() {
  elements.banList.innerHTML = "";
  if (!state.bans.length) {
    elements.banList.innerHTML = '<div class="card admin-card empty-state">没有匹配的封禁记录</div>';
  } else {
    state.bans.forEach((item) => {
      const card = document.createElement("div");
      card.className = "admin-card";
      const unbanHtml = item.is_active
        ? `<button class="btn btn-outline btn-inline" data-ban-unban="${item.linuxdo_user_id}">解封</button>`
        : "";
      card.innerHTML = `
        <div class="admin-card-header">
          <div>
            <div class="admin-card-title">ID ${item.linuxdo_user_id}</div>
            <div class="admin-card-meta mono">@${item.username_snapshot || "-"}</div>
          </div>
          <span class="admin-badge ${item.is_active ? "active" : "disabled"}">${item.is_active ? "有效" : "历史"}</span>
        </div>
        <div class="admin-card-meta">原因：${item.reason}</div>
        <div class="admin-card-meta">封禁时间：${formatDateTime(item.banned_at)}</div>
        <div class="admin-card-meta">到期：${item.expires_at ? formatDateTime(item.expires_at) : "永久"}</div>
        <div class="admin-card-actions">
          <button class="btn btn-outline btn-inline" data-ban-open="${item.linuxdo_user_id}">查看用户</button>
          ${unbanHtml}
        </div>
      `;
      card.querySelector("[data-ban-open]").addEventListener("click", async () => {
        const userLabel = formatUserLabel(item.username_snapshot, item.linuxdo_user_id);
        switchTab("users");
        showNotice(`正在定位 ${userLabel}...`);
        await loadUserDetail(item.linuxdo_user_id);
        loadUsers();
        showNotice(`已定位到 ${userLabel}`);
      });
      const unbanBtn = card.querySelector("[data-ban-unban]");
      if (unbanBtn) {
        unbanBtn.addEventListener("click", async () => {
          const userLabel = formatUserLabel(item.username_snapshot, item.linuxdo_user_id);
          const restoreButton = setButtonPending(unbanBtn, "解封中...");
          showNotice(`正在解封 ${userLabel}...`);
          try {
            await fetchJson(`/admin/users/${encodeURIComponent(item.linuxdo_user_id)}/unban`, { method: "POST" });
            showNotice(`已解封 ${userLabel}`);
            await Promise.all([loadUsers(), loadBans()]);
            if (state.selectedUserId === item.linuxdo_user_id) {
              await loadUserDetail(item.linuxdo_user_id);
            }
          } catch (error) {
            showNotice(error.message, "error");
          } finally {
            restoreButton?.();
          }
        });
      }
      elements.banList.appendChild(card);
    });
  }
  renderPager(elements.banPager, state.bansPage, (nextOffset) => {
    state.bansPage.offset = nextOffset;
    loadBans();
  });
}

function renderTokens() {
  elements.tokenList.innerHTML = "";
  if (!state.tokens.length) {
    elements.tokenList.innerHTML = '<div class="card admin-card empty-state">没有匹配的 Token</div>';
  } else {
    state.tokens.forEach((item) => {
      const statusClass = item.is_cleaned ? "cleaned" : item.is_banned ? "disabled" : !item.is_active ? "inactive" : item.is_enabled ? "enabled" : "disabled";
      const statusText = item.is_cleaned ? "已清理" : item.is_banned ? "已封禁" : !item.is_active ? "文件缺失" : item.is_enabled ? "已启用" : "已停用";
      const actionText = item.is_enabled ? "停用" : "启用";
      const actionPath = item.is_enabled ? "deactivate" : "activate";
      const probeMeta = item.last_probe_status ? ` · 探活 ${item.last_probe_status}${item.last_probe_at ? ` @ ${formatDateTime(item.last_probe_at)}` : ""}` : "";
      const banMeta = item.is_banned ? ` · 原因 ${item.ban_reason || "upstream_401"}` : "";
      const card = document.createElement("div");
      card.className = "admin-card";
      card.innerHTML = `
        <div class="admin-card-header">
          <div>
            <div class="admin-card-title">${item.file_name}</div>
            <div class="admin-card-meta mono">${item.file_path}</div>
          </div>
          <span class="admin-badge ${statusClass}">${statusText}</span>
        </div>
        <div class="admin-card-meta">编码：${item.encoding} · 领取 ${item.claim_count} / 上限 ${item.max_claims}</div>
        <div class="admin-card-meta">最后同步：${formatDateTime(item.last_seen_at)}${probeMeta}${banMeta}</div>
        <div class="admin-card-actions">
          <button class="btn btn-outline btn-inline" data-token-action="${item.id}" ${!item.is_active || item.is_cleaned || item.is_banned ? "disabled" : ""}>${actionText}</button>
        </div>
      `;
      const actionBtn = card.querySelector("[data-token-action]");
      actionBtn.addEventListener("click", async () => {
        const restoreButton = setButtonPending(actionBtn, `${actionText}中...`);
        showNotice(`正在${actionText} ${item.file_name}...`);
        try {
          await fetchJson(`/admin/tokens/${item.id}/${actionPath}`, { method: "POST" });
          showNotice(`${item.file_name} 已${actionText}`);
          await refreshTokenMonitoring({ silent: true });
        } catch (error) {
          showNotice(error.message, "error");
        } finally {
          restoreButton?.();
        }
      });
      elements.tokenList.appendChild(card);
    });
  }
  renderPager(elements.tokenPager, state.tokensPage, (nextOffset) => {
    state.tokensPage.offset = nextOffset;
    refreshTokenMonitoring({ silent: true }).catch(() => {});
  });
}

function setQueueOnlyFilter(mode) {
  state.queueOnly = mode || "all";
  renderQueueOnlyButtons();
}

function renderQueueOnlyButtons() {
  const modes = [
    { button: elements.queueShowAll, mode: "all" },
    { button: elements.queueShowAbnormal, mode: "abnormal" },
    { button: elements.queueShowTimeout, mode: "timeout" },
  ];
  modes.forEach(({ button, mode }) => {
    if (!button) {
      return;
    }
    const active = state.queueOnly === mode;
    button.classList.toggle("btn-primary", active);
    button.classList.toggle("btn-outline", !active);
  });
}

function queueStatusLabel(item) {
  switch (item?.status) {
    case "cancelled":
      return "已取消";
    case "expired":
      return "已过期";
    case "fulfilled":
      return "已完成";
    case "queued":
      return "排队中";
    default:
      return item?.status ? `未知状态(${item.status})` : "未知状态";
  }
}

function queueBadgeClass(item) {
  if (item?.status === "expired" || item?.status === "fulfilled") {
    return "cleaned";
  }
  if (item?.status === "cancelled") {
    return "disabled";
  }
  if (item?.status !== "queued") {
    return "disabled";
  }
  if (item?.is_abnormal || item?.is_timeout) {
    return "active";
  }
  return "enabled";
}

function summarizeQueueRefresh(result) {
  if (!result) {
    return "队列已刷新";
  }
  const scanned = Number(result.scanned || 0);
  const cancelled = Number(result.cancelled || 0);
  const expired = Number(result.expired || 0);
  const totalQueued = Number(result.total_queued || 0);
  return `队列已刷新：扫描 ${scanned} 条，取消 ${cancelled} 条，过期 ${expired} 条，当前排队 ${totalQueued} 条`;
}

async function promptQueueCancelReason(subject) {
  const value = window.prompt(`请输入取消原因：${subject}`);
  const reason = String(value || "").trim();
  return reason || null;
}

async function cancelQueueEntryItem(item, button) {
  const reason = await promptQueueCancelReason(`队列 #${item.queue_id}`);
  if (!reason) {
    return;
  }
  if (!window.confirm(`确认取消队列 #${item.queue_id} 吗？`)) {
    return;
  }
  const restoreButton = setButtonPending(button, "取消中...");
  showNotice(`正在取消队列 #${item.queue_id}...`);
  try {
    await fetchJson(`/admin/queue/${item.queue_id}/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason }),
    });
    showNotice(`已取消队列 #${item.queue_id}`);
    await Promise.all([loadQueue(), loadPolicy()]);
  } catch (error) {
    showNotice(error.message, "error");
  } finally {
    restoreButton?.();
  }
}

async function cancelUserQueueItems(item, button) {
  const reason = await promptQueueCancelReason(`用户 ${item.user_id} 的全部当前排队`);
  if (!reason) {
    return;
  }
  if (!window.confirm(`确认取消用户 ${item.user_id} 的全部当前排队吗？`)) {
    return;
  }
  const restoreButton = setButtonPending(button, "取消中...");
  showNotice(`正在取消用户 ${item.user_id} 的全部当前排队...`);
  try {
    const result = await fetchJson(`/admin/queue/users/${item.user_id}/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason }),
    });
    showNotice(`已取消用户 ${item.user_id} 的 ${result.cancelled || 0} 条排队`);
    await Promise.all([loadQueue(), loadPolicy()]);
  } catch (error) {
    showNotice(error.message, "error");
  } finally {
    restoreButton?.();
  }
}

function renderQueue() {
  renderQueueOnlyButtons();
  elements.queueList.innerHTML = "";
  if (!state.queue.length) {
    elements.queueList.innerHTML = '<div class="card admin-card empty-state">没有匹配的排队项</div>';
  } else {
    state.queue.forEach((item) => {
      const card = document.createElement("div");
      card.className = "admin-card";
      const statusLabel = queueStatusLabel(item);
      const waitText = formatDurationSeconds(item.wait_duration_seconds);
      const userLabel = item.username ? `@${item.username}` : `用户 ${item.user_id}`;
      const queuePosition = item.queue_position ?? "-";
      const apiKeyText = item.api_key_id ?? "-";
      const errorMeta = item.last_error_reason
        ? `<div class="admin-note queue-card-reason">最近错误：${item.last_error_reason}${item.last_error_at ? ` · ${formatDateTime(item.last_error_at)}` : ""} · 连续失败 ${item.failure_count || 0} 次</div>`
        : "";
      const cancelMeta = item.cancel_reason
        ? `<div class="admin-note queue-card-reason">取消信息：${item.cancel_reason}${item.cancelled_at ? ` · ${formatDateTime(item.cancelled_at)}` : ""}${item.cancelled_by_user_id ? ` · 操作者 ${item.cancelled_by_user_id}` : ""}</div>`
        : "";
      const timeoutBadge = item.is_timeout ? '<span class="admin-badge active">超时</span>' : "";
      const abnormalBadge = item.is_abnormal ? '<span class="admin-badge active">异常</span>' : "";
      const actionHtml = item.status === "queued"
        ? `
          <button class="btn btn-outline btn-inline" data-queue-cancel="${item.queue_id}">取消本条</button>
          <button class="btn btn-outline btn-inline" data-queue-cancel-user="${item.user_id}">取消该用户全部排队</button>
        `
        : "";
      card.innerHTML = `
        <div class="admin-card-header">
          <div>
            <div class="admin-card-title">队列 #${item.queue_id} · ${userLabel}</div>
            <div class="admin-card-meta mono">用户 ID ${item.user_id} · LinuxDo ${item.linuxdo_user_id || "-"} · API Key ${apiKeyText}</div>
          </div>
          <div class="admin-inline-actions">
            ${timeoutBadge}
            ${abnormalBadge}
            <span class="admin-badge ${queueBadgeClass(item)}">${statusLabel}</span>
          </div>
        </div>
        <div class="queue-card-grid">
          <div class="queue-card-block">
            <div class="queue-card-label">请求与剩余</div>
            <div class="queue-card-value">${item.requested} / ${item.remaining}</div>
          </div>
          <div class="queue-card-block">
            <div class="queue-card-label">排队位置</div>
            <div class="queue-card-value">${queuePosition}</div>
          </div>
          <div class="queue-card-block">
            <div class="queue-card-label">等待时长</div>
            <div class="queue-card-value">${waitText}</div>
          </div>
          <div class="queue-card-block">
            <div class="queue-card-label">请求 ID</div>
            <div class="queue-card-value mono">${item.request_id || "-"}</div>
          </div>
        </div>
        <div class="admin-card-meta">入队时间：${formatDateTime(item.enqueued_at)}</div>
        ${errorMeta}
        ${cancelMeta}
        <div class="admin-card-actions">${actionHtml}</div>
      `;
      const cancelBtn = card.querySelector("[data-queue-cancel]");
      if (cancelBtn) {
        cancelBtn.addEventListener("click", () => cancelQueueEntryItem(item, cancelBtn));
      }
      const cancelUserBtn = card.querySelector("[data-queue-cancel-user]");
      if (cancelUserBtn) {
        cancelUserBtn.addEventListener("click", () => cancelUserQueueItems(item, cancelUserBtn));
      }
      elements.queueList.appendChild(card);
    });
  }
  renderPager(elements.queuePager, state.queuePage, (nextOffset) => {
    state.queuePage.offset = nextOffset;
    loadQueue();
  });
}

function renderPolicy() {
  const policy = state.policy;
  if (!policy) {
    elements.policyPanel.innerHTML = '<div class="empty-state">暂无策略数据</div>';
    return;
  }
  const thresholds = policy.thresholds || {};
  const system = policy.system || {};
  elements.policyPanel.innerHTML = `
    <div class="admin-detail-header">
      <div>
        <div class="admin-detail-title">当前运行策略</div>
        <div class="admin-detail-meta">配置来源：${policy.source}</div>
      </div>
      <span class="admin-badge ${policy.status === "critical" ? "active" : "normal"}">${policy.status}</span>
    </div>
    <div class="admin-policy-grid">
      <div class="admin-policy-item"><strong>每小时额度</strong><span>${policy.hourly_limit}</span></div>
      <div class="admin-policy-item"><strong>每 Token 最大领取</strong><span>${policy.max_claims}</span></div>
      <div class="admin-policy-item"><strong>健康阈值</strong><span>${thresholds.healthy ?? "-"}</span></div>
      <div class="admin-policy-item"><strong>警告阈值</strong><span>${thresholds.warning ?? "-"}</span></div>
      <div class="admin-policy-item"><strong>严重阈值</strong><span>${thresholds.critical ?? "-"}</span></div>
      <div class="admin-policy-item"><strong>当前排队</strong><span>${system.queue?.total ?? 0}</span></div>
      <div class="admin-policy-item"><strong>启用库存总量</strong><span>${system.inventory?.total ?? 0}</span></div>
      <div class="admin-policy-item"><strong>可领取次数</strong><span>${system.inventory?.available ?? 0}</span></div>
    </div>
    <div class="admin-note">说明：本页只读展示 .env 与运行时推导结果，不提供在线改额度。</div>
  `;
}

async function loadAdminMe() {
  state.me = await fetchJson("/admin/me");
  state.user = state.me.user;
  renderAuthSummary();
  renderPolicySummary();
}

async function loadUsers(resetOffset = false) {
  if (resetOffset) {
    state.usersPage.offset = 0;
  }
  state.usersPage.limit = Number.parseInt(elements.userLimit.value, 10) || 50;
  const search = encodeURIComponent(elements.userSearch.value.trim());
  const banStatus = encodeURIComponent(elements.userFilter.value);
  const payload = await fetchJson(`/admin/users?search=${search}&ban_status=${banStatus}&limit=${state.usersPage.limit}&offset=${state.usersPage.offset}`);
  state.users = payload.items || [];
  state.usersPage.total = payload.total || 0;
  state.usersPage.limit = payload.limit || state.usersPage.limit;
  state.usersPage.offset = payload.offset || 0;
  renderUsers();
}

async function loadUserDetail(linuxdoUserId) {
  state.selectedUserId = linuxdoUserId;
  state.userDetail = await fetchJson(`/admin/users/${encodeURIComponent(linuxdoUserId)}`);
  renderUserDetail();
}

async function loadBans(resetOffset = false) {
  if (resetOffset) {
    state.bansPage.offset = 0;
  }
  state.bansPage.limit = Number.parseInt(elements.banLimit.value, 10) || 50;
  const search = encodeURIComponent(elements.banSearch.value.trim());
  const filter = encodeURIComponent(elements.banFilter.value);
  const payload = await fetchJson(`/admin/bans?search=${search}&status=${filter}&limit=${state.bansPage.limit}&offset=${state.bansPage.offset}`);
  state.bans = payload.items || [];
  state.bansPage.total = payload.total || 0;
  state.bansPage.limit = payload.limit || state.bansPage.limit;
  state.bansPage.offset = payload.offset || 0;
  renderBans();
}

async function loadTokens(resetOffset = false) {
  if (resetOffset) {
    state.tokensPage.offset = 0;
  }
  state.tokensPage.limit = Number.parseInt(elements.tokenLimit.value, 10) || 50;
  const search = encodeURIComponent(elements.tokenSearch.value.trim());
  const filter = encodeURIComponent(elements.tokenFilter.value);
  const payload = await fetchJson(`/admin/tokens?search=${search}&status=${filter}&limit=${state.tokensPage.limit}&offset=${state.tokensPage.offset}`);
  state.tokens = payload.items || [];
  state.tokensPage.total = payload.total || 0;
  state.tokensPage.limit = payload.limit || state.tokensPage.limit;
  state.tokensPage.offset = payload.offset || 0;
  renderTokens();
}

async function loadQueue(resetOffset = false) {
  if (resetOffset) {
    state.queuePage.offset = 0;
  }
  state.queuePage.limit = Number.parseInt(elements.queueLimit.value, 10) || 50;
  const search = encodeURIComponent(elements.queueSearch.value.trim());
  const status = encodeURIComponent(elements.queueStatus.value || "queued");
  const only = encodeURIComponent(state.queueOnly || "all");
  const payload = await fetchJson(`/admin/queue?search=${search}&status=${status}&only=${only}&limit=${state.queuePage.limit}&offset=${state.queuePage.offset}`);
  state.queue = payload.items || [];
  state.queuePage.total = payload.total || 0;
  state.queuePage.limit = payload.limit || state.queuePage.limit;
  state.queuePage.offset = payload.offset || 0;
  renderQueue();
}

async function loadPolicy() {
  state.policy = await fetchJson("/admin/policy");
  state.me = { ...(state.me || {}), policy: state.policy };
  renderPolicySummary();
  renderPolicy();
}

function applyPagedAdminPayload(targetKey, pageState, payload, render) {
  state[targetKey] = payload?.items || [];
  pageState.total = payload?.total || 0;
  pageState.limit = payload?.limit || pageState.limit;
  pageState.offset = payload?.offset || 0;
  render();
}

function applyAdminBootstrapPayload(payload) {
  state.me = payload?.me ? { ...payload.me, policy: payload?.policy || payload.me?.policy || null } : null;
  state.user = state.me?.user || null;
  state.policy = payload?.policy || state.me?.policy || null;
  renderAuthSummary();
  renderPolicySummary();

  applyPagedAdminPayload("users", state.usersPage, payload?.users || {}, renderUsers);
  applyPagedAdminPayload("bans", state.bansPage, payload?.bans || {}, renderBans);
  applyPagedAdminPayload("tokens", state.tokensPage, payload?.tokens || {}, renderTokens);
  applyPagedAdminPayload("queue", state.queuePage, payload?.queue || {}, renderQueue);
  renderPolicy();
}

async function loadAdminBootstrap() {
  state.usersPage.offset = 0;
  state.bansPage.offset = 0;
  state.tokensPage.offset = 0;
  state.queuePage.offset = 0;
  state.usersPage.limit = Number.parseInt(elements.userLimit.value, 10) || state.usersPage.limit || 50;
  state.bansPage.limit = Number.parseInt(elements.banLimit.value, 10) || state.bansPage.limit || 50;
  state.tokensPage.limit = Number.parseInt(elements.tokenLimit.value, 10) || state.tokensPage.limit || 50;
  state.queuePage.limit = Number.parseInt(elements.queueLimit.value, 10) || state.queuePage.limit || 50;
  const payload = await fetchJson("/admin/bootstrap");
  applyAdminBootstrapPayload(payload || {});
}

async function runQueueRefresh() {
  const restoreButton = setButtonPending(elements.queueRefresh, "刷新中...");
  showNotice("正在执行队列修复与推进...");
  try {
    const result = await fetchJson("/admin/queue/refresh", { method: "POST" });
    showNotice(summarizeQueueRefresh(result));
    await Promise.all([loadQueue(), loadPolicy()]);
  } catch (error) {
    showNotice(error.message, "error");
  } finally {
    restoreButton?.();
  }
}

async function cleanupExhaustedTokens(mode) {
  const filesOnly = mode === "files_only";
  const confirmText = filesOnly
    ? "这会删除 token 目录中所有已领完账号的文件，并在数据库里标记为已删除。历史已领取记录仍可查看，但不会再分发给新用户。是否继续？"
    : "这会删除 token 目录中所有已领完账号的文件，清空数据库中的账号内容并压缩数据库。历史已领取记录仍可查看。是否继续？";
  if (!window.confirm(confirmText)) {
    return;
  }
  const targetButton = filesOnly ? elements.tokenCleanupFiles : elements.tokenCleanupDb;
  const restoreButton = setButtonPending(targetButton, "清理中...");
  showNotice(filesOnly ? "正在删除文件并标记已删除..." : "正在删除文件、清理数据库并压缩...");
  try {
    const result = await fetchJson("/admin/tokens/cleanup-exhausted", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode }),
    });
    const vacuumText = result.vacuumed ? "，已压缩数据库" : "";
    showNotice(
      filesOnly
        ? `清理完成：处理 ${result.cleaned || 0} 个账号，删除文件 ${result.deleted_files || 0} 个${vacuumText}`
        : `清理完成：处理 ${result.cleaned || 0} 个账号，删除文件 ${result.deleted_files || 0} 个，清空数据库内容 ${result.compacted_content || 0} 个${vacuumText}`
    );
    await refreshTokenMonitoring({ silent: true });
  } catch (error) {
    showNotice(error.message, "error");
  } finally {
    restoreButton?.();
  }
}

async function banSelectedUser() {
  if (!state.selectedUserId) {
    return;
  }
  const submitBtn = document.getElementById("admin-ban-submit");
  const reason = document.getElementById("admin-ban-reason")?.value.trim() || "";
  if (!reason) {
    showNotice("封禁原因必填", "error");
    return;
  }
  const expiresValue = document.getElementById("admin-ban-expires")?.value || "";
  const expiresDate = expiresValue ? new Date(expiresValue) : null;
  if (expiresDate && Number.isNaN(expiresDate.getTime())) {
    showNotice("封禁到期时间格式无效", "error");
    return;
  }
  const expiresAt = expiresDate ? expiresDate.toISOString() : null;
  const userLabel = getUserLabelFromDetail();
  const restoreButton = setButtonPending(submitBtn, "保存中...");
  showNotice(`正在保存 ${userLabel} 的封禁...`);
  try {
    await fetchJson(`/admin/users/${encodeURIComponent(state.selectedUserId)}/ban`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason, expires_at: expiresAt }),
    });
    showNotice(`已保存对 ${userLabel} 的封禁`);
    await loadUserDetail(state.selectedUserId);
    await Promise.all([loadUsers(), loadBans(true)]);
  } catch (error) {
    showNotice(error.message, "error");
  } finally {
    restoreButton?.();
  }
}

async function unbanSelectedUser() {
  if (!state.selectedUserId) {
    return;
  }
  const unbanBtn = document.getElementById("admin-unban-submit");
  const userLabel = getUserLabelFromDetail();
  const restoreButton = setButtonPending(unbanBtn, "解封中...");
  showNotice(`正在解封 ${userLabel}...`);
  try {
    await fetchJson(`/admin/users/${encodeURIComponent(state.selectedUserId)}/unban`, { method: "POST" });
    showNotice(`已解封 ${userLabel}`);
    await loadUserDetail(state.selectedUserId);
    await Promise.all([loadUsers(), loadBans(true)]);
  } catch (error) {
    showNotice(error.message, "error");
  } finally {
    restoreButton?.();
  }
}

async function logout() {
  try {
    await fetch("/auth/logout", { method: "POST" });
  } catch (error) {
    console.error(error);
  }
  showScreen("login");
}

function bindEvents() {
  elements.loginBtn?.addEventListener("click", () => {
    window.location.href = "/auth/linuxdo/login?next=/admin";
  });
  elements.loadingRetryBtn?.addEventListener("click", () => {
    bootstrapAdminShell().catch(() => {});
  });
  elements.logoutBtn?.addEventListener("click", logout);
  elements.deniedLogoutBtn?.addEventListener("click", logout);
  elements.tabUsers?.addEventListener("click", () => switchTab("users"));
  elements.tabBans?.addEventListener("click", () => switchTab("bans"));
  elements.tabTokens?.addEventListener("click", () => switchTab("tokens"));
  elements.tabQueue?.addEventListener("click", () => switchTab("queue"));
  elements.tabPolicy?.addEventListener("click", () => switchTab("policy"));
  elements.userRefresh?.addEventListener("click", () => loadUsers(true));
  elements.banRefresh?.addEventListener("click", () => loadBans(true));
  elements.tokenRefresh?.addEventListener("click", () => {
    refreshTokenMonitoring({ resetOffset: true }).catch(() => {});
  });
  elements.queueRefresh?.addEventListener("click", () => {
    runQueueRefresh().catch(() => {});
  });
  elements.tokenCleanupFiles?.addEventListener("click", () => cleanupExhaustedTokens("files_only"));
  elements.tokenCleanupDb?.addEventListener("click", () => cleanupExhaustedTokens("files_and_db"));
  elements.userSearch?.addEventListener("keydown", (event) => event.key === "Enter" && loadUsers(true));
  elements.banSearch?.addEventListener("keydown", (event) => event.key === "Enter" && loadBans(true));
  elements.tokenSearch?.addEventListener("keydown", (event) => event.key === "Enter" && refreshTokenMonitoring({ resetOffset: true }).catch(() => {}));
  elements.queueSearch?.addEventListener("keydown", (event) => event.key === "Enter" && loadQueue(true));
  elements.userFilter?.addEventListener("change", () => loadUsers(true));
  elements.banFilter?.addEventListener("change", () => loadBans(true));
  elements.tokenFilter?.addEventListener("change", () => refreshTokenMonitoring({ resetOffset: true }).catch(() => {}));
  elements.queueStatus?.addEventListener("change", () => loadQueue(true));
  elements.queueShowAll?.addEventListener("click", () => {
    setQueueOnlyFilter("all");
    loadQueue(true);
  });
  elements.queueShowAbnormal?.addEventListener("click", () => {
    setQueueOnlyFilter("abnormal");
    loadQueue(true);
  });
  elements.queueShowTimeout?.addEventListener("click", () => {
    setQueueOnlyFilter("timeout");
    loadQueue(true);
  });
  elements.userLimit?.addEventListener("change", () => loadUsers(true));
  elements.banLimit?.addEventListener("change", () => loadBans(true));
  elements.tokenLimit?.addEventListener("change", () => refreshTokenMonitoring({ resetOffset: true }).catch(() => {}));
  elements.queueLimit?.addEventListener("change", () => loadQueue(true));
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && !elements.appScreen.classList.contains("hidden")) {
      refreshTokenMonitoring({ silent: true }).catch(() => {});
    }
  });
}

async function bootstrapAdminShell() {
  if (state.bootstrapPromise) {
    return state.bootstrapPromise;
  }

  setLoadingState({
    title: "正在加载后台",
    subtitle: "正在请求后台数据并确认管理员权限。",
    message: "",
    showRetry: false,
    showSpinner: true,
  });
  showScreen("loading");

  state.bootstrapPromise = (async () => {
    try {
      await loadAdminBootstrap();
      showScreen("app");
      switchTab("users");
    } catch (error) {
      if (error?.status === 401) {
        showScreen("login");
        setLoginMessage("", "error");
        if (state.authErrorParam) {
          setLoginMessage(`登录失败：${state.authErrorParam}`, "error");
        }
        return;
      }
      if (error?.status === 403) {
        elements.deniedMessage.textContent = error.message;
        showScreen("denied");
        return;
      }
      setLoadingState({
        title: "后台启动失败",
        subtitle: "首屏后台数据加载失败，当前不会误判为未登录。",
        message: describeStartupError(error),
        tone: "error",
        showRetry: true,
        showSpinner: false,
      });
      showScreen("loading");
    } finally {
      state.bootstrapPromise = null;
      elements.loadingRetryBtn.disabled = false;
    }
  })();

  return state.bootstrapPromise;
}

async function init() {
  bindEvents();
  renderQueueOnlyButtons();
  const url = new URL(window.location.href);
  state.authErrorParam = url.searchParams.get("auth_error") || "";
  await bootstrapAdminShell();

  if (state.authErrorParam) {
    url.searchParams.delete("auth_error");
    window.history.replaceState({}, document.title, `${url.pathname}${url.search}`);
    state.authErrorParam = "";
  }
}

init();
