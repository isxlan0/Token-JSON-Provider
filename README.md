# Token Atlas

局域网 JSON 索引服务与前端控制台。服务会监听 `token/` 目录中的 `.json` 文件变化，自动维护索引，并通过前端页面和 JSON 接口提供访问能力。

## 功能概览

- 监听 `token/` 目录的新增、修改、删除
- 自动生成索引，返回 `index`、`id`、`name`、`path`、`size`、`mtime`、`encoding`、`keys`
- 支持通过 `name`、`id`、`index` 获取完整 JSON 内容
- 前端使用最简登录界面，密码正确后才显示数据
- 前端内置“文档”标签页，列出全部调用方式
- 访问密码错误或缺失时，接口返回 `401`，不返回任何数据体
- 提供交互式 `client_demo.py`，可按数字菜单调用接口
- 支持通过 `.env` 或系统环境变量配置访问密钥

## 安装

```powershell
python -m pip install -r requirements.txt
```

## 配置方式

服务和客户端支持两种配置来源：

1. 项目根目录 `.env`
2. 系统环境变量

优先级规则：

- 如果系统环境变量已经存在，则优先使用系统环境变量
- 如果系统环境变量不存在，则读取 `.env` 中的值

### 方式一：使用 `.env`

先在项目根目录创建 `.env` 文件，可参考 `.env.example`：

```dotenv
TOKEN_INDEX_ACCESS_KEY=your-access-key
TOKEN_PROVIDER_BASE_URL=http://127.0.0.1:8000
PORT=8000
```

### 方式二：使用环境变量

```powershell
$env:TOKEN_INDEX_ACCESS_KEY = "your-access-key"
$env:TOKEN_PROVIDER_BASE_URL = "http://127.0.0.1:8000"
$env:PORT = "8000"
```

## 运行服务

```powershell
python .\app.py
```

默认监听 `0.0.0.0:8000`，局域网内可通过：

```text
http://<你的IP>:8000/
```

访问前端页面。

## 运行客户端 Demo

```powershell
python .\client_demo.py
```

客户端默认读取：

- `TOKEN_PROVIDER_BASE_URL`
- `TOKEN_INDEX_ACCESS_KEY`

启动后可直接输入数字执行操作，也支持一次输入多个编号，例如：

```text
1 2 34
```

这表示依次执行：

- `1` 登录校验
- `2` 获取全部索引
- `3` 获取全部索引 + 内容
- `4` 获取指定 ID 内容

客户端菜单如下：

```text
1. 登录校验
2. 获取全部索引
3. 获取全部索引 + 内容
4. 获取指定 ID 内容
5. 获取指定 Index 内容
6. 健康检测
7. 按文件名获取内容
8. 修改服务地址
9. 修改访问密码
0. 退出
```

## 前端使用说明

1. 打开首页 `/`
2. 输入访问密码
3. 点击“登录”
4. 登录成功后进入单独的应用界面
5. 顶部 `数据` / `文档` 标签可切换页面
6. 在 `数据` 页中查看索引和 JSON 详情
7. 在 `文档` 页中查看全部接口和调用示例
8. 如果密码错误或失效，前端会自动清空数据并退回未登录状态

## 接口说明

### 1. 登录校验

```http
POST /auth/login
Content-Type: application/json
```

请求体：

```json
{
  "password": "your-access-key"
}
```

返回：

- 成功：`204 No Content`
- 失败：`401 Unauthorized`，空响应体

### 2. 获取全部索引

请求头：

```text
X-Access-Key: your-access-key
```

接口：

```http
GET /json
```

返回示例：

```json
{
  "count": 29,
  "updated_at": "2026-03-14T12:34:56+08:00",
  "items": [
    {
      "index": 0,
      "id": "02b3d9f6-93a0-4ad6-ba2c-044c87feb3a2",
      "name": "02b3d9f6-93a0-4ad6-ba2c-044c87feb3a2.json",
      "path": "token/02b3d9f6-93a0-4ad6-ba2c-044c87feb3a2.json",
      "size": 4208,
      "mtime": "2026-03-03T14:57:40+08:00",
      "encoding": "utf-8",
      "keys": ["access_token", "account_id", "email"]
    }
  ]
}
```

### 3. 通过文件名获取详情

```http
GET /json/item?name=02b3d9f6-93a0-4ad6-ba2c-044c87feb3a2.json
```

### 4. 通过 ID 获取详情

```http
GET /json/item?id=02b3d9f6-93a0-4ad6-ba2c-044c87feb3a2
```

### 5. 通过索引序号获取详情

```http
GET /json/item?index=0
```

### 6. 健康检查

```http
GET /health
```

## curl 调用示例

### 登录验证

```powershell
curl -X POST http://127.0.0.1:8000/auth/login -H "Content-Type: application/json" -d "{\"password\":\"your-access-key\"}"
```

### 获取索引

```powershell
curl http://127.0.0.1:8000/json -H "X-Access-Key: your-access-key"
```

### 通过 index 获取详情

```powershell
curl "http://127.0.0.1:8000/json/item?index=0" -H "X-Access-Key: your-access-key"
```

### 通过 id 获取详情

```powershell
curl "http://127.0.0.1:8000/json/item?id=02b3d9f6-93a0-4ad6-ba2c-044c87feb3a2" -H "X-Access-Key: your-access-key"
```

### 通过 name 获取详情

```powershell
curl "http://127.0.0.1:8000/json/item?name=02b3d9f6-93a0-4ad6-ba2c-044c87feb3a2.json" -H "X-Access-Key: your-access-key"
```

## 常见问题

### 页面打开了，但登录失败

检查：

- `TOKEN_INDEX_ACCESS_KEY` 是否配置正确
- `.env` 是否放在项目根目录
- 系统环境变量是否覆盖了 `.env` 中的值

### 客户端 Demo 连不上服务

检查：

- `TOKEN_PROVIDER_BASE_URL` 是否正确
- 服务是否已经启动
- 目标端口是否可访问

### 登录成功但没有数据

检查：

- `token/` 目录下是否存在 `.json` 文件
- 文件内容是否是合法 JSON
- 文件是否正在被其他程序占用写入

### 想让其他设备访问

确认：

- 服务运行在 `0.0.0.0`
- 使用的是当前机器局域网 IP，而不是 `127.0.0.1`
- Windows 防火墙已允许对应端口

## 目录说明

- `app.py`: FastAPI 服务入口
- `client_demo.py`: 命令行客户端 Demo
- `token/`: 被监听的 JSON 文件目录
- `static/`: 前端页面资源
- `.env.example`: 配置示例文件