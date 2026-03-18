const state = {
  user: null,
  me: null,
  users: [],
  selectedUserId: null,
  userDetail: null,
  bans: [],
  tokens: [],
  policy: null,
};

const elements = {
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
  tabPolicy: document.getElementById("admin-tab-policy"),
  viewUsers: document.getElementById("admin-view-users"),
  viewBans: document.getElementById("admin-view-bans"),
  viewTokens: document.getElementById("admin-view-tokens"),
  viewPolicy: document.getElementById("admin-view-policy"),
  userSearch: document.getElementById("admin-user-search"),
  userFilter: document.getElementById("admin-user-filter"),
  userRefresh: document.getElementById("admin-user-refresh"),
  userList: document.getElementById("admin-user-list"),
  userDetail: document.getElementById("admin-user-detail"),
  banSearch: document.getElementById("admin-ban-search"),
  banFilter: document.getElementById("admin-ban-filter"),
  banRefresh: document.getElementById("admin-ban-refresh"),
  banList: document.getElementById("admin-ban-list"),
  tokenSearch: document.getElementById("admin-token-search"),
  tokenFilter: document.getElementById("admin-token-filter"),
  tokenRefresh: document.getElementById("admin-token-refresh"),
  tokenList: document.getElementById("admin-token-list"),
  policyPanel: document.getElementById("admin-policy-panel"),
};

function showScreen(name) {
  elements.loginScreen.classList.toggle("hidden", name !== "login");
  elements.deniedScreen.classList.toggle("hidden", name !== "denied");
  elements.appScreen.classList.toggle("hidden", name !== "app");
}

function setLoginMessage(message = "", tone = "error") {
  elements.loginMessage.textContent = message;
  elements.loginMessage.dataset.tone = tone;
  elements.loginMessage.classList.toggle("hidden", !message);
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

function switchTab(name) {
  const views = [
    { key: "users", tab: elements.tabUsers, view: elements.viewUsers, title: "用户管理" },
    { key: "bans", tab: elements.tabBans, view: elements.viewBans, title: "封禁记录" },
    { key: "tokens", tab: elements.tabTokens, view: elements.viewTokens, title: "Token 管理" },
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
}

function renderAuthSummary() {
  if (!state.user) {
    elements.authSummary.textContent = "-";
    return;
  }
  const banText = state.user.is_banned ? " · 当前账号已被封禁." : "";
  elements.authSummary.textContent = `LinuxDo / ${state.user.name || state.user.username} (@${state.user.username})${banText}`;
}

function renderPolicySummary() {
  if (!state.me?.policy) {
    elements.policySummary.textContent = "策略 -";
    return;
  }
  const policy = state.me.policy;
  elements.policySummary.textContent = `策略 ${policy.status} · 每小时 ${policy.hourly_limit}`;
}

function renderUsers() {
  elements.userList.innerHTML = "";
  if (!state.users.length) {
    elements.userList.innerHTML = '<div class="card admin-card empty-state">没有匹配的用户</div>';
    return;
  }
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

function renderUserDetail() {
  const detail = state.userDetail;
  if (!detail) {
    elements.userDetail.className = "card admin-detail empty-state";
    elements.userDetail.textContent = "选择一个用户查看详情";
    return;
  }
  const user = detail.user;
  const ban = detail.ban;
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
        <input id="admin-ban-expires" type="datetime-local" value="${ban?.expires_at ? ban.expires_at.slice(0, 16) : ""}">
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
    return;
  }
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
      switchTab("users");
      await loadUsers();
      await loadUserDetail(item.linuxdo_user_id);
    });
    const unbanBtn = card.querySelector("[data-ban-unban]");
    if (unbanBtn) {
      unbanBtn.addEventListener("click", async () => {
        await fetchJson(`/admin/users/${encodeURIComponent(item.linuxdo_user_id)}/unban`, { method: "POST" });
        await Promise.all([loadUsers(), loadBans()]);
        if (state.selectedUserId === item.linuxdo_user_id) {
          await loadUserDetail(item.linuxdo_user_id);
        }
      });
    }
    elements.banList.appendChild(card);
  });
}

function renderTokens() {
  elements.tokenList.innerHTML = "";
  if (!state.tokens.length) {
    elements.tokenList.innerHTML = '<div class="card admin-card empty-state">没有匹配的 Token</div>';
    return;
  }
  state.tokens.forEach((item) => {
    const statusClass = !item.is_active ? "inactive" : item.is_enabled ? "enabled" : "disabled";
    const statusText = !item.is_active ? "文件缺失" : item.is_enabled ? "已启用" : "已停用";
    const actionText = item.is_enabled ? "停用" : "启用";
    const actionPath = item.is_enabled ? "deactivate" : "activate";
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
      <div class="admin-card-meta">领取 ${item.claim_count} / 上限 ${item.max_claims}</div>
      <div class="admin-card-meta">最后同步：${formatDateTime(item.last_seen_at)}</div>
      <div class="admin-card-actions">
        <button class="btn btn-outline btn-inline" data-token-action="${item.id}" ${!item.is_active ? "disabled" : ""}>${actionText}</button>
      </div>
    `;
    const actionBtn = card.querySelector("[data-token-action]");
    actionBtn.addEventListener("click", async () => {
      await fetchJson(`/admin/tokens/${item.id}/${actionPath}`, { method: "POST" });
      await Promise.all([loadTokens(), loadPolicy()]);
    });
    elements.tokenList.appendChild(card);
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

async function loadUsers() {
  const search = encodeURIComponent(elements.userSearch.value.trim());
  const banStatus = encodeURIComponent(elements.userFilter.value);
  const payload = await fetchJson(`/admin/users?search=${search}&ban_status=${banStatus}&limit=100`);
  state.users = payload.items || [];
  renderUsers();
}

async function loadUserDetail(linuxdoUserId) {
  state.selectedUserId = linuxdoUserId;
  state.userDetail = await fetchJson(`/admin/users/${encodeURIComponent(linuxdoUserId)}`);
  renderUserDetail();
}

async function loadBans() {
  const search = encodeURIComponent(elements.banSearch.value.trim());
  const filter = encodeURIComponent(elements.banFilter.value);
  const payload = await fetchJson(`/admin/bans?search=${search}&status=${filter}&limit=100`);
  state.bans = payload.items || [];
  renderBans();
}

async function loadTokens() {
  const search = encodeURIComponent(elements.tokenSearch.value.trim());
  const filter = encodeURIComponent(elements.tokenFilter.value);
  const payload = await fetchJson(`/admin/tokens?search=${search}&status=${filter}&limit=200`);
  state.tokens = payload.items || [];
  renderTokens();
}

async function loadPolicy() {
  state.policy = await fetchJson("/admin/policy");
  state.me = { ...(state.me || {}), policy: state.policy };
  renderPolicySummary();
  renderPolicy();
}

async function banSelectedUser() {
  if (!state.selectedUserId) {
    return;
  }
  const reason = document.getElementById("admin-ban-reason")?.value.trim() || "";
  if (!reason) {
    alert("封禁原因必填");
    return;
  }
  const expiresValue = document.getElementById("admin-ban-expires")?.value || "";
  await fetchJson(`/admin/users/${encodeURIComponent(state.selectedUserId)}/ban`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason, expires_at: expiresValue || null }),
  });
  await Promise.all([loadUsers(), loadBans()]);
  await loadUserDetail(state.selectedUserId);
}

async function unbanSelectedUser() {
  if (!state.selectedUserId) {
    return;
  }
  await fetchJson(`/admin/users/${encodeURIComponent(state.selectedUserId)}/unban`, { method: "POST" });
  await Promise.all([loadUsers(), loadBans()]);
  await loadUserDetail(state.selectedUserId);
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
  elements.logoutBtn?.addEventListener("click", logout);
  elements.deniedLogoutBtn?.addEventListener("click", logout);
  elements.tabUsers?.addEventListener("click", () => switchTab("users"));
  elements.tabBans?.addEventListener("click", () => switchTab("bans"));
  elements.tabTokens?.addEventListener("click", () => switchTab("tokens"));
  elements.tabPolicy?.addEventListener("click", () => switchTab("policy"));
  elements.userRefresh?.addEventListener("click", loadUsers);
  elements.banRefresh?.addEventListener("click", loadBans);
  elements.tokenRefresh?.addEventListener("click", loadTokens);
  elements.userSearch?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      loadUsers();
    }
  });
  elements.banSearch?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      loadBans();
    }
  });
  elements.tokenSearch?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      loadTokens();
    }
  });
  elements.userFilter?.addEventListener("change", loadUsers);
  elements.banFilter?.addEventListener("change", loadBans);
  elements.tokenFilter?.addEventListener("change", loadTokens);
}

async function init() {
  bindEvents();
  const url = new URL(window.location.href);
  const authError = url.searchParams.get("auth_error");
  try {
    const status = await fetchJson("/auth/status");
    if (!status.authenticated) {
      showScreen("login");
      if (authError) {
        setLoginMessage(`登录失败：${authError}`, "error");
      }
    } else if (!status.user?.is_admin) {
      elements.deniedMessage.textContent = "当前账号无权限。";
      showScreen("denied");
    } else {
      state.user = status.user;
      showScreen("app");
      switchTab("users");
      await Promise.all([loadAdminMe(), loadUsers(), loadBans(), loadTokens(), loadPolicy()]);
    }
  } catch (error) {
    if (error.status === 403) {
      elements.deniedMessage.textContent = error.message;
      showScreen("denied");
    } else {
      showScreen("login");
      setLoginMessage(error.message, "error");
    }
  }

  if (authError) {
    url.searchParams.delete("auth_error");
    window.history.replaceState({}, document.title, `${url.pathname}${url.search}`);
  }
}

init();