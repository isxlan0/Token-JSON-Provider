const ACCESS_KEY_HEADER = "X-Access-Key";

const state = {
  accessKey: "",
  items: [],
  filteredItems: [],
  selectedName: null,
  lastUpdated: null,
};

const elements = {
  loginScreen: document.getElementById("login-screen"),
  appScreen: document.getElementById("app-screen"),
  passwordInput: document.getElementById("password-input"),
  loginBtn: document.getElementById("login-btn"),
  logoutBtn: document.getElementById("logout-btn"),
  loginMessage: document.getElementById("login-message"),
  tabData: document.getElementById("tab-data"),
  tabDocs: document.getElementById("tab-docs"),
  viewData: document.getElementById("view-data"),
  viewDocs: document.getElementById("view-docs"),
  searchInput: document.getElementById("search-input"),
  sortSelect: document.getElementById("sort-select"),
  refreshBtn: document.getElementById("refresh-btn"),
  downloadZipBtn: document.getElementById("download-zip-btn"),
  summaryCount: document.getElementById("summary-count"),
  summaryUpdated: document.getElementById("summary-updated"),
  summaryOrigin: document.getElementById("summary-origin"),
  indexList: document.getElementById("index-list"),
  detailRoute: document.getElementById("detail-route"),
  detailMeta: document.getElementById("detail-meta"),
  detailContent: document.getElementById("detail-content"),
  template: document.getElementById("index-item-template"),
};

function getHeaders() {
  return {
    [ACCESS_KEY_HEADER]: state.accessKey,
  };
}

function setLoginMessage(message) {
  elements.loginMessage.textContent = message;
}

function setAuthPlaceholders(key) {
  const placeholders = document.querySelectorAll(".auth-placeholder");
  const displayKey = key || "你的密码";
  placeholders.forEach((el) => {
    el.textContent = displayKey;
  });
}

function clearData() {
  state.items = [];
  state.filteredItems = [];
  state.selectedName = null;
  state.lastUpdated = null;
  elements.indexList.innerHTML = "";
  elements.summaryCount.textContent = "0 个文件";
  elements.summaryUpdated.textContent = "最近同步: -";
  elements.summaryOrigin.textContent = `地址: ${window.location.origin}`;
  elements.detailRoute.textContent = "/json/item";
  elements.detailMeta.textContent = "未选择文件";
  elements.detailContent.textContent = "登录后选择文件查看完整 JSON。";
  setAuthPlaceholders("");
}

function showLoggedIn(loggedIn) {
  if (loggedIn) {
    elements.loginScreen.classList.add("hidden");
    elements.appScreen.classList.remove("hidden");
    setAuthPlaceholders(state.accessKey);
    return;
  }

  elements.appScreen.classList.add("hidden");
  elements.loginScreen.classList.remove("hidden");
}

function switchTab(name) {
  const isData = name === "data";
  elements.tabData.classList.toggle("active", isData);
  elements.tabDocs.classList.toggle("active", !isData);
  elements.viewData.classList.toggle("active", isData);
  elements.viewDocs.classList.toggle("active", !isData);
}

function formatTime(value) {
  if (!value) {
    return "-";
  }

  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

function applyFilters() {
  const query = elements.searchInput.value.trim().toLowerCase();
  const sortMode = elements.sortSelect.value;

  const items = state.items.filter((item) => {
    if (!query) {
      return true;
    }

    return [item.name, item.id, item.path].some((value) =>
      String(value).toLowerCase().includes(query),
    );
  });

  items.sort((left, right) => {
    if (sortMode === "name-asc") {
      return left.name.localeCompare(right.name);
    }

    if (sortMode === "name-desc") {
      return right.name.localeCompare(left.name);
    }

    const leftTime = Date.parse(left.mtime);
    const rightTime = Date.parse(right.mtime);
    return sortMode === "mtime-asc" ? leftTime - rightTime : rightTime - leftTime;
  });

  state.filteredItems = items;
  renderIndex();
}

function renderIndex() {
  elements.indexList.innerHTML = "";
  elements.summaryCount.textContent = `${state.items.length} 个文件`;
  elements.summaryUpdated.textContent = `最近同步: ${formatTime(state.lastUpdated)}`;
  elements.summaryOrigin.textContent = `地址: ${window.location.origin}`;

  if (state.filteredItems.length === 0) {
    elements.indexList.textContent = "没有可显示的数据。";
    return;
  }

  for (const item of state.filteredItems) {
    const fragment = elements.template.content.cloneNode(true);
    const button = fragment.querySelector(".index-item");
    const name = fragment.querySelector(".index-name");
    const meta = fragment.querySelector(".index-meta");

    name.textContent = `${item.index}. ${item.name}`;
    meta.textContent = `${item.path}\n${item.encoding} | ${item.size} bytes | ${formatTime(item.mtime)}`;

    if (item.name === state.selectedName) {
      button.classList.add("active");
    }

    button.addEventListener("click", () => selectItem(item));
    elements.indexList.append(fragment);
  }
}

function renderDetail(detail) {
  const item = detail.item;
  elements.detailRoute.textContent = `/json/item?name=${encodeURIComponent(item.name)}`;
  elements.detailMeta.textContent = `index=${item.index} | id=${item.id} | path=${item.path} | encoding=${item.encoding}`;
  elements.detailContent.textContent = JSON.stringify(detail.content, null, 2);
}

function renderError(message) {
  elements.detailRoute.textContent = "/json/item";
  elements.detailMeta.textContent = message;
  elements.detailContent.textContent = message;
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: getHeaders(),
  });

  if (response.status === 401) {
    const error = new Error("访问密码无效或缺失");
    error.code = 401;
    throw error;
  }

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.detail || `请求失败: ${response.status}`);
  }

  return payload;
}

function logout(message = "已退出登录") {
  state.accessKey = "";
  elements.passwordInput.value = "";
  clearData();
  showLoggedIn(false);
  setLoginMessage(message);
}

async function login() {
  await loginWithKey(elements.passwordInput.value.trim());
}

async function loginWithKey(password) {
  if (!password) {
    setLoginMessage("请输入访问密码");
    return;
  }

  try {
    const response = await fetch(`/auth/login?key=${encodeURIComponent(password)}`, {
      method: "POST",
    });

    if (response.status !== 204) {
      throw new Error("登录失败");
    }

    state.accessKey = password;
    setLoginMessage("登录成功");
    showLoggedIn(true);
    switchTab("data");
    await loadIndex();
  } catch {
    logout("密码错误，未返回任何数据");
  }
}

async function loadIndex() {
  if (!state.accessKey) {
    clearData();
    return;
  }

  try {
    const data = await fetchJson("/json");
    state.items = data.items || [];
    state.lastUpdated = data.updated_at || null;
    applyFilters();

    if (state.items.length > 0) {
      await selectItem(state.items[0]);
    } else {
      renderError("当前没有可用的 JSON 文件。");
    }
  } catch (error) {
    if (error.code === 401) {
      logout("访问密码无效或缺失，未返回任何数据");
      return;
    }

    renderError(error.message);
  }
}

async function selectItem(item) {
  state.selectedName = item.name;
  renderIndex();

  try {
    const detail = await fetchJson(`/json/item?name=${encodeURIComponent(item.name)}`);
    renderDetail(detail);
  } catch (error) {
    if (error.code === 401) {
      logout("访问密码无效或缺失，未返回任何数据");
      return;
    }

    renderError(error.message);
  }
}

async function downloadZip() {
  const btn = elements.downloadZipBtn;
  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = "下载中...";
  try {
    const response = await fetch("/zip", { headers: getHeaders() });
    if (response.status === 401) {
      logout("访问密码无效，请重新登录");
      return;
    }
    if (!response.ok) {
      throw new Error(`下载失败: ${response.status}`);
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `tokens_${new Date().toISOString().slice(0, 10)}.zip`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch (error) {
    alert(`下载失败: ${error.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = original;
  }
}

function bindEvents() {
  elements.loginBtn.addEventListener("click", login);
  elements.logoutBtn.addEventListener("click", () => logout());
  elements.refreshBtn.addEventListener("click", async () => {
    const btn = elements.refreshBtn;
    const originalHtml = btn.innerHTML;
    btn.disabled = true;
    btn.textContent = "正在同步...";
    try {
      await loadIndex();
    } finally {
      btn.innerHTML = originalHtml;
      btn.disabled = false;
    }
  });
  elements.downloadZipBtn.addEventListener("click", downloadZip);
  elements.tabData.addEventListener("click", () => switchTab("data"));
  elements.tabDocs.addEventListener("click", () => switchTab("docs"));
  elements.searchInput.addEventListener("input", applyFilters);
  elements.sortSelect.addEventListener("change", applyFilters);
  elements.passwordInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      login();
    }
  });
}

function setupCopyButtons() {
  document.querySelectorAll(".code-block").forEach((block) => {
    block.style.position = "relative";
    block.style.paddingRight = "3rem";

    const btn = document.createElement("button");
    btn.className = "copy-btn";
    btn.title = "复制到剪贴板";
    btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>`;

    btn.addEventListener("click", async () => {
      const clone = block.cloneNode(true);
      const cloneBtn = clone.querySelector(".copy-btn");
      if (cloneBtn) {
        clone.removeChild(cloneBtn);
      }
      const textToCopy = clone.textContent.trim();

      try {
        await navigator.clipboard.writeText(textToCopy);
        const originalHtml = btn.innerHTML;
        btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--success)"><polyline points="20 6 9 17 4 12"/></svg>`;
        setTimeout(() => {
          btn.innerHTML = originalHtml;
        }, 2000);
      } catch (e) {
        console.error("Copy failed", e);
      }
    });

    block.appendChild(btn);
  });
}

function init() {
  bindEvents();
  setupCopyButtons();
  clearData();
  showLoggedIn(false);
  switchTab("data");

  const autoKey = new URLSearchParams(window.location.search).get("key");
  if (autoKey) {
    elements.passwordInput.value = autoKey;
    setLoginMessage("检测到 key 参数，正在自动登录...");
    loginWithKey(autoKey);
  }
}

init();