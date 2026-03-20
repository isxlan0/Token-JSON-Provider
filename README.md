# Token Atlas

基于 LinuxDo OAuth2 的 JSON 账号分发服务。把可分发的 `.json` 文件放到 `token/` 目录，用户登录后可领取账号，并通过 API Key 下载对应 JSON。

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
python -m pip install -r requirements.txt
```

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
```

LinuxDo OAuth2 配置步骤：

1. 在 LinuxDo Connect 创建应用
2. 回调域名填写 `https://your-domain.example/`
3. 回调地址填写 `https://your-domain.example/auth/linuxdo/callback`
4. 将 Client ID 与 Client Secret 写入 `.env`

## 启动

```powershell
python .\app.py
```

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

- `app.py`：FastAPI 服务
- `token/`：账号 JSON 文件
- `static/`：前端页面资源
- `.env.example`：配置模板
