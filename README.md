# Token Atlas

基于 LinuxDo OAuth2 的 JSON 账号分发服务。把可分发的 `.json` 文件放到 `TOKEN_FILES_DIR` 指向的目录中，默认是项目根目录下的 `token/`。用户登录后可领取账号，并通过 API Key 下载对应 JSON。

当前默认运行的是 Go 重构版服务，旧版 Python 主程序已归档到 `legacy_python_backend/`，仅作留档。

## 功能

- LinuxDo OAuth2 登录
- `.env` 配置后台管理员（支持 LinuxDo ID / `@用户名`）
- 独立 `/admin` 管理后台
- 用户封禁 / 解封，封禁后禁止进入主站操作
- 自动同步 `token/` 目录
- 领取额度与速率限制可配置
- API Key 管理与限流
- 领取结果可直接下载 JSON
- 数据库存档，防止重复领取

## 安装

```powershell
.\build-go.bat
```

```bash
./build-go.sh
```

构建结果：

- 所有平台二进制都会直接输出到 `bin/`
- 当前默认产物包含：`token-atlas-windows-amd64.exe`、`token-atlas-windows-arm64.exe`、`token-atlas-linux-amd64`、`token-atlas-linux-arm64`、`token-atlas-macos-amd64`、`token-atlas-macos-arm64`

## 配置

在项目根目录创建 `.env`，可参考 `.env.example`。

```dotenv
# LinuxDo OAuth2 配置
TOKEN_INDEX_SESSION_SECRET=change-me-session-secret
TOKEN_INDEX_LINUXDO_CLIENT_ID=
TOKEN_INDEX_LINUXDO_CLIENT_SECRET=
TOKEN_INDEX_LINUXDO_REDIRECT_URI=
TOKEN_INDEX_LINUXDO_SCOPE=read
TOKEN_INDEX_LINUXDO_MIN_TRUST_LEVEL=0
TOKEN_INDEX_LINUXDO_ALLOWED_IDS=
TOKEN_INDEX_ADMIN_IDENTITIES=

# 数据库路径（默认 ./token_atlas.db）
TOKEN_DB_PATH=

# Token 文件目录（默认项目根目录下的 token）
TOKEN_FILES_DIR=token

# 领取额度与分发规则
TOKEN_CLAIM_HOURLY_LIMIT=50
TOKEN_CLAIM_BATCH_LIMIT=50
TOKEN_MAX_CLAIMS_PER_TOKEN=1
TOKEN_APIKEY_MAX_PER_USER=5
TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE=60
TOKEN_CODEX_PROBE_DELAY_SEC=1.5
TOKEN_CODEX_PROBE_TIMEOUT_SEC=20
TOKEN_CODEX_PROBE_RESERVE_SEC=30

# 服务地址
TOKEN_PROVIDER_BASE_URL=http://127.0.0.1:8000
PORT=8000

# 调试日志
TOKEN_LOG_LEVEL=info
TOKEN_CLAIM_TRACE=false
```

LinuxDo OAuth2 配置步骤：

1. 在 LinuxDo Connect 创建应用
2. 回调域名填写 `https://your-domain.example/`
3. 回调地址填写 `https://your-domain.example/auth/linuxdo/callback`
4. 将 Client ID 与 Client Secret 写入 `.env`

## 启动

```powershell
python .\start.py
```

如果已经编译完成，也可以直接运行 Go 二进制：

```powershell
.\bin\token-atlas-windows-amd64.exe
```

```bash
./bin/token-atlas-linux-amd64
```

`start.py` 会自动按当前系统与架构选择对应二进制；如果没有 `bin/` 目录，则会回退到项目根目录查找同名文件。无论从哪个入口启动，工作目录都会固定到项目根目录，不再依赖“当前目录大概差不多”。

如果需要同时启动 `GptCodexApi/runner.py`，保留原有 `.env` 配置即可，`start.py` 会按需创建 `.venv` 并安装 `GptCodexApi/requirements.txt`。Go 服务和 `GptCodexApi` 会共用项目根目录 `.env` 中的 `TOKEN_FILES_DIR` 配置。

服务默认监听 `0.0.0.0:8000`，浏览器访问：

```text
http://<服务器IP>:8000/
```

## 使用流程

1. 访问 `/`
2. 使用 LinuxDo 登录完成 OAuth2
3. 在页面查看额度、库存与统计
4. 创建 API Key（只展示一次，请及时保存）
5. 领取账号并下载 JSON

## 问题排查

### 1. 打开后端调试日志

在 `.env` 中增加或修改：

```dotenv
TOKEN_LOG_LEVEL=debug
TOKEN_CLAIM_TRACE=true
```

说明：

- `TOKEN_LOG_LEVEL=debug`：打开更详细的服务日志
- `TOKEN_CLAIM_TRACE=true`：额外输出领取、排队推进、SSE 推送、启动等待等全链路日志

修改后重启服务，再观察服务端控制台输出。

### 2. 打开前端控制台调试

推荐直接在页面地址后拼接查询参数：

```text
http://127.0.0.1:8000/?claim_debug=1
```

然后打开浏览器开发者工具 Console，点击“申请账号”后会看到从按钮点击开始的完整日志，包括：

- 提交领取请求
- `/me/claim` 返回是否已受理
- 当前 `request_id` 与 `client_tab_id`
- `/me/queue-stream` SSE 建连、断开、重连
- 实时队列快照与终态事件

也可以在浏览器控制台手动持久开启：

```js
localStorage.setItem("token_atlas_claim_debug", "1");
location.reload();
```

关闭方法：

```js
localStorage.removeItem("token_atlas_claim_debug");
location.reload();
```

调试开关优先级如下：

1. URL 参数 `?claim_debug=1`
2. 浏览器 `localStorage["token_atlas_claim_debug"]`
3. 服务端返回的 `debug` 配置


## 接口

### 登录相关

- `GET /auth/status`：查询登录状态
- `POST /auth/logout`：退出登录
- `GET /auth/linuxdo/login`：跳转 LinuxDo 登录
- `GET /auth/linuxdo/callback`：OAuth2 回调
- `GET /me`：返回当前用户、额度、领取统计
- `GET /dashboard/stats`：返回库存与全站领取统计

### 管理后台

- `GET /admin`：管理后台页面
- `GET /admin/me`：后台当前管理员与策略摘要
- `GET /admin/users`：用户列表
- `GET /admin/users/{linuxdo_user_id}`：用户详情
- `POST /admin/users/{linuxdo_user_id}/ban`：封禁用户
- `POST /admin/users/{linuxdo_user_id}/unban`：解封用户
- `GET /admin/bans`：封禁记录
- `GET /admin/tokens`：Token 列表
- `POST /admin/tokens/{id}/activate`：启用 Token
- `POST /admin/tokens/{id}/deactivate`：停用 Token
- `GET /admin/policy`：只读查看当前额度策略

### API Key 管理

- `GET /me/api-keys`：获取当前用户 API Key 列表
- `POST /me/api-keys`：创建 API Key
- `POST /me/api-keys/{id}/revoke`：撤销 API Key

### 领取与下载

- `POST /me/claim`：登录态领取账号
- `POST /api/claim`：API Key 领取账号，Header: `X-API-Key`
- `GET /api/download/{token_id}`：下载指定账号 JSON

### 保留兼容接口（不改动）

- `GET /json`
- `GET /json/item`
- `POST /json/archive`
- `GET /zip`

## curl 示例

```powershell
# 使用 API Key 领取 5 个账号
curl -X POST http://127.0.0.1:8000/api/claim `
  -H "Content-Type: application/json" `
  -H "X-API-Key: your-api-key" `
  -d "{\"count\":5}"
```

```powershell
# 下载领取到的 JSON
curl -L http://127.0.0.1:8000/api/download/123 `
  -H "X-API-Key: your-api-key"
```

## 目录结构

- `cmd/server/`：Go 服务入口
- `internal/`：Go 版核心业务逻辑
- `legacy_python_backend/`：归档的旧版 Python 主程序
- `token/`：默认账号 JSON 文件目录，可通过 `TOKEN_FILES_DIR` 改为其他路径
- `static/`：前端页面资源
- `.env.example`：配置模板
