# Cook-Redis

Cook-Redis 是一个面向游戏测试/联调场景的 Redis 数据处理工具。项目当前包含三部分：

1. **Rust 后端服务**：提供 HTTP API，用于连接 Redis、备份数据库、删除 Key/Table、读取/写入 Hash 字段，以及对账号数据进行本地化处理。
2. **Chrome/Chromium 浏览器扩展**：提供轻量化操作面板，用于配置环境并触发常用 Redis 操作。
3. **Python 脚本工具**：早期/辅助版命令行工具，用于导入、备份、本地化、批处理和跨服数据处理。

> ⚠️ 本项目会执行 `FLUSHDB`、`DEL`、`RESTORE REPLACE` 等高风险 Redis 操作。请务必先确认目标库、备份数据和配置文件，避免误删生产或重要测试数据。

## 目录

- [功能概览](#功能概览)
- [项目结构](#项目结构)
- [运行环境](#运行环境)
- [快速开始：Rust 后端](#快速开始rust-后端)
- [浏览器扩展使用方式](#浏览器扩展使用方式)
- [Python 脚本使用方式](#python-脚本使用方式)
- [配置说明](#配置说明)
- [HTTP API 文档](#http-api-文档)
- [账号本地化规则](#账号本地化规则)
- [安全注意事项](#安全注意事项)
- [开发与测试](#开发与测试)
- [常见问题](#常见问题)

## 功能概览

### Redis 基础能力

- 测试 Redis 连接是否可用。
- 清空指定 Redis DB。
- 删除指定 Key 或指定表名。
- 将源 Redis DB 备份/复制到目标 Redis DB。
- 读取 Hash 字段，并自动尝试将二进制内容解析为：
  - MessagePack JSON；
  - UTF-8 文本；
  - 原始二进制 Base64。
- 以 Base64 形式写回 Hash 字段的原始字节数据。
- 列出指定 Hash 的所有字段。

### 游戏账号本地化能力

- 单账号本地化：从源 Redis 的指定 Hash Field 读取账号数据，改写平台、分组、服务器等字段后写入目标 Redis。
- 批量本地化：按指定 Field 列表批量处理。
- 全表本地化：使用 `HSCAN` 扫描整个 Hash 并批量写入，适合处理 `Account` 等账号表。
- 自动为目标账号 Field 添加登录前缀，例如 `local_`、`yhzr2_`。

### 辅助入口

- Rust 后端适合作为当前主服务入口。
- 浏览器扩展适合日常手动操作。
- Python 脚本保留了命令行菜单和批处理流程，适合离线或历史工作流。

## 项目结构

```text
cook-redis/
├── README.md
├── LICENSE
├── backend/                 # Rust HTTP 后端
│   ├── Cargo.toml
│   ├── Cargo.lock
│   ├── config/
│   │   └── app.json         # 后端监听地址配置
│   └── src/
│       ├── main.rs          # Axum 服务入口
│       ├── routes/          # HTTP 路由
│       ├── services/        # Redis 与本地化业务逻辑
│       ├── models/          # 请求/响应/配置模型
│       ├── utils/           # MessagePack 编解码工具
│       └── error.rs         # 统一错误响应
├── extension/               # Chrome/Chromium Manifest V3 扩展
│   ├── manifest.json
│   ├── popup.html/js
│   ├── options.html/js
│   ├── style.css
│   └── icons/
└── scripts/python/          # Python 版脚本工具
    ├── cfg/                 # 示例配置
    ├── src/                 # Python 源码
    ├── test/                # 简单脚本/测试样例
    └── temp/                # 配置模板与临时文件
```

## 运行环境

### 必需环境

- Redis Server：本地或远程 Redis 实例。
- Rust：建议安装稳定版 Rust 工具链。
- Chrome/Chromium：仅当使用浏览器扩展时需要。
- Python 3.10+：仅当使用 `scripts/python` 脚本时需要；脚本中使用了 `match/case` 语法。

### Rust 后端主要依赖

后端基于：

- `axum`：HTTP API 服务。
- `tokio`：异步运行时。
- `redis`：异步 Redis 客户端。
- `serde` / `serde_json`：序列化与 JSON 处理。
- `rmp-serde`：MessagePack 编解码。
- `base64`：二进制字段编码。
- `tower-http`：CORS 支持。

### Python 脚本主要依赖

Python 脚本使用：

- `redis`
- `msgpack`

安装示例：

```bash
python -m pip install redis msgpack
```

## 快速开始：Rust 后端

### 1. 配置监听地址

后端配置文件位于 `backend/config/app.json`：

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 8642
  }
}
```

如需允许局域网访问，可将 `host` 改为 `0.0.0.0`。请注意这会暴露危险操作接口，务必配合网络隔离或代理鉴权。

### 2. 启动服务

```bash
cd backend
cargo run
```

启动成功后会输出类似：

```text
CookRedis Rust server listening on http://127.0.0.1:8642
```

### 3. 健康检查

```bash
curl http://127.0.0.1:8642/api/health
```

预期响应：

```json
{
  "success": true,
  "data": "cookredis-rs is running",
  "message": "ok"
}
```

## 浏览器扩展使用方式

扩展位于 `extension/`，采用 Chrome Manifest V3。

### 安装步骤

1. 打开 Chrome/Chromium。
2. 访问 `chrome://extensions/`。
3. 打开右上角「开发者模式」。
4. 点击「加载已解压的扩展程序」。
5. 选择本项目的 `extension/` 目录。
6. 确保 Rust 后端已启动，默认地址为 `http://127.0.0.1:8642`。

### 扩展配置

扩展会将环境配置保存在浏览器本地存储中。常见配置包括：

- 环境名称，例如 `default`、`dev`、`qa`。
- 源 Redis：`host`、`port`、`db`、`password`。
- 目标 Redis：`host`、`port`、`db`、`password`。
- 本地化配置：
  - `pre_login`：目标账号前缀；
  - `platform`：目标平台；
  - `group`：目标分组；
  - `server`：目标服务器名或区服标识。

> 说明：扩展的 `manifest.json` 默认允许访问 `http://127.0.0.1:8642/*` 和 `http://localhost:8642/*`。如果后端监听地址或端口不同，需要同步修改扩展权限配置。

## Python 脚本使用方式

Python 工具位于 `scripts/python/`，包含配置、处理逻辑和测试脚本。

### 1. 安装依赖

```bash
python -m pip install redis msgpack
```

### 2. 修改配置

常用配置文件位于 `scripts/python/cfg/`：

- `local_db_cfg.json`：本服/目标库配置，以及账号本地化参数。
- `source_db_cfg.json`：源库配置。
- `backup_db_cfg.json`：备份库配置。
- `cross_db_cfg.json`：跨服库配置。
- `del_tb.json`：需要删除的表名列表。

### 3. 启动菜单工具

```bash
cd scripts/python/src
python cookredis.py
```

菜单提供以下工作流：

1. 将源库导入本服库。
2. 将本服库备份到备份库。
3. 本地化账号数据。
4. 删除指定本服 DB。
5. 重新初始化配置。
6. 批量处理多个配置目录。
7. 跨服数据处理。
0. 退出。

> 注意：Python 脚本中的 `clear()` 使用了 Windows 的 `cls` 命令；在 macOS/Linux 上可能需要改为 `clear`。

## 配置说明

### Rust RedisConfig

Rust API 中 Redis 连接配置统一使用如下结构：

```json
{
  "host": "127.0.0.1",
  "port": 6379,
  "password": null,
  "db": 0
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `host` | string | 是 | Redis 主机名或 IP。 |
| `port` | number | 是 | Redis 端口。 |
| `password` | string/null | 否 | Redis 密码；无密码可传 `null`。 |
| `db` | number | 是 | Redis DB 编号。 |

### Rust ServerConfig

账号本地化接口使用如下服务配置：

```json
{
  "platform": "1",
  "group": "2",
  "server": "S2",
  "pre_login": "local_"
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `platform` | string | 是 | 目标平台值。若能解析为整数，会优先写入数字字段。 |
| `group` | string | 是 | 目标分组值。若能解析为整数，会优先写入数字字段。 |
| `server` | string | 是 | 目标服务器/区服标识。 |
| `pre_login` | string | 否 | 写入目标账号 Field 时使用的前缀。 |

### Python local_db_cfg.json 示例

```json
{
  "redis": {
    "host": "localhost",
    "port": 6379,
    "db": 2,
    "password": null
  },
  "server": {
    "platform": 1,
    "group": 1
  },
  "account": "Account",
  "preLogin": "yhzr2_"
}
```

## HTTP API 文档

后端所有成功响应大致遵循：

```json
{
  "success": true,
  "data": {},
  "message": "ok"
}
```

错误响应大致遵循：

```json
{
  "success": false,
  "message": "错误信息"
}
```

### 健康检查

#### `GET /api/health`

检查服务是否运行。

```bash
curl http://127.0.0.1:8642/api/health
```

#### `GET /api/redis/ping`

返回 Redis 路由层的简单 `pong`。

```bash
curl http://127.0.0.1:8642/api/redis/ping
```

### Redis 连接测试

#### `POST /api/redis/test`

请求体为 RedisConfig：

```bash
curl -X POST http://127.0.0.1:8642/api/redis/test \
  -H 'Content-Type: application/json' \
  -d '{"host":"127.0.0.1","port":6379,"password":null,"db":0}'
```

成功后返回 `connected`。

### 清空 DB

#### `POST /api/redis/flushdb`

请求体：

```json
{
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "confirm_text": "FLUSHDB db=2 host=127.0.0.1"
}
```

`confirm_text` 必须严格等于：

```text
FLUSHDB db=<db> host=<host>
```

示例：

```bash
curl -X POST http://127.0.0.1:8642/api/redis/flushdb \
  -H 'Content-Type: application/json' \
  -d '{"target":{"host":"127.0.0.1","port":6379,"password":null,"db":2},"confirm_text":"FLUSHDB db=2 host=127.0.0.1"}'
```

### 删除 Keys

#### `POST /api/redis/delete-keys`

请求体：

```json
{
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "keys": ["key1", "key2"],
  "confirm_text": "DELETE 2 db=2"
}
```

`confirm_text` 必须严格等于：

```text
DELETE <keys.length> db=<db>
```

### 删除 Tables

#### `POST /api/redis/delete-tables`

该接口本质上也是删除 Redis Key，只是参数名为 `tables`，适合按业务表名删除。

请求体：

```json
{
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "tables": ["tb1", "tb2"],
  "confirm_text": "DELETE_TABLES 2 db=2"
}
```

`confirm_text` 必须严格等于：

```text
DELETE_TABLES <tables.length> db=<db>
```

### 备份数据库

#### `POST /api/redis/backup`

将源 DB 的所有 Key 复制到目标 DB。执行时会先清空目标 DB，然后对源库所有 Key 执行 `DUMP`，再在目标库执行 `RESTORE ... REPLACE`。

请求体：

```json
{
  "source": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 1
  },
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  }
}
```

示例：

```bash
curl -X POST http://127.0.0.1:8642/api/redis/backup \
  -H 'Content-Type: application/json' \
  -d '{"source":{"host":"127.0.0.1","port":6379,"password":null,"db":1},"target":{"host":"127.0.0.1","port":6379,"password":null,"db":2}}'
```

### 读取 Hash 字段

#### `POST /api/redis/hash/get`

读取指定 Hash Field，并返回可视化结果。

请求体：

```json
{
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "hash_name": "Account",
  "field": "test_user"
}
```

响应中的 `data` 包含：

| 字段 | 说明 |
| --- | --- |
| `hash_name` | Hash 名称。 |
| `field` | Field 名称。 |
| `raw_base64` | 原始字节的 Base64 表示。 |
| `raw_size` | 原始字节长度。 |
| `decoded_type` | `msgpack-json`、`utf8-text` 或 `binary`。 |
| `decoded_json` | MessagePack 可解析时的 JSON 值。 |
| `decoded_text` | UTF-8 可解析时的文本。 |

### 写入 Hash 字段

#### `POST /api/redis/hash/set`

以 Base64 形式写入原始字节。

请求体：

```json
{
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "hash_name": "Account",
  "field": "test_user",
  "base64_value": "SGVsbG8="
}
```

### 列出 Hash 字段

#### `POST /api/redis/hash/list`

请求体：

```json
{
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "hash_name": "Account"
}
```

### 单账号本地化

#### `POST /api/process/localize-account`

请求体：

```json
{
  "source": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 1
  },
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "hash_name": "Account",
  "source_field": "player_001",
  "target_field": null,
  "server": {
    "platform": "1",
    "group": "2",
    "server": "S2",
    "pre_login": "local_"
  }
}
```

行为说明：

- 从 `source` 的 `hash_name/source_field` 读取 MessagePack 数据。
- 将数据转成 JSON 值并按本地化规则替换。
- 重新编码为 MessagePack。
- 写入 `target` 的同名 Hash。
- 如果 `target_field` 为 `null`，则目标 Field 为 `pre_login + source_field`。

### 批量字段本地化

#### `POST /api/process/localize-batch`

请求体：

```json
{
  "source": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 1
  },
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "hash_name": "Account",
  "source_fields": ["player_001", "player_002"],
  "server": {
    "platform": "1",
    "group": "2",
    "server": "S2",
    "pre_login": "local_"
  }
}
```

响应 `data` 为处理摘要：

```json
{
  "hash_name": "Account",
  "scanned": 2,
  "localized": 2,
  "skipped": 0,
  "written": 2,
  "elapsed_ms": 12
}
```

### 全表账号本地化

#### `POST /api/process/localize-all-acc`

请求体与 `localize-batch` 相同，但 `source_fields` 当前不会作为扫描范围使用；服务会对 `hash_name` 执行 `HSCAN`，扫描所有 Field。

适合全量处理 `Account` Hash：

```json
{
  "source": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 1
  },
  "target": {
    "host": "127.0.0.1",
    "port": 6379,
    "password": null,
    "db": 2
  },
  "hash_name": "Account",
  "source_fields": [],
  "server": {
    "platform": "1",
    "group": "2",
    "server": "S2",
    "pre_login": "local_"
  }
}
```

## 账号本地化规则

Rust 后端会先将 Hash Field 的原始字节按 MessagePack 解码为 JSON 值，然后递归处理：

1. **根数组处理**
   - 如果根数组前两个元素看起来是数字，会分别替换为 `platform` 和 `group`。
   - 如果根数组第一个元素仍是数组，也会尝试处理该内层数组的前两个元素。

2. **对象字段处理**
   - 以下键会被视为平台字段：`platform`、`plat`、`platformId`。
   - 以下键会被视为分组字段：`group`、`groupId`、`gid`。
   - 以下键会被视为服务器字段：`server`、`sid`、`zone`。

3. **字符串内容处理**
   - 会替换字符串中的平台、分组、服务器键值片段，例如 `platform=...`、`gid=...`、`server=...`。
   - 会使用正则替换形如 `S数字` 的服务器标识为目标 `server`。

4. **目标 Field 命名**
   - 单账号接口：优先使用显式 `target_field`；没有传入时使用 `pre_login + source_field`。
   - 批量/全表接口：统一使用 `pre_login + 原 Field`。

> 如果某个 Field 不是合法 MessagePack，批量接口会跳过该 Field 并增加 `skipped` 计数。

## 安全注意事项

- **备份接口会清空目标库**：`/api/redis/backup` 会先对目标 Redis DB 执行 `FLUSHDB`。
- **删除接口不可恢复**：`delete-keys`、`delete-tables` 会直接删除目标 Redis 中的 Key。
- **清库接口不可恢复**：`flushdb` 会清空目标 DB。
- **不要将后端直接暴露到不可信网络**：当前后端启用了宽松 CORS，适合本地工具场景，不适合作为公网服务直接部署。
- **上线前请增加鉴权**：如果需要多人或远程使用，建议在反向代理或后端层增加认证、审计和白名单。
- **操作前先备份**：尤其是在处理 `Account`、跨服数据或大批量 Key 前，建议先复制到备份库验证。

## 开发与测试

### Rust 后端检查

```bash
cd backend
cargo fmt --check
cargo check
```

### Rust 后端运行

```bash
cd backend
cargo run
```

### Python 简单脚本

```bash
cd scripts/python/src
python cookredis.py
```

### Redis 测试数据

`scripts/python/test/input_test_data.py` 可向本地 Redis 写入示例 `Account` Hash 数据。运行前请确认它连接的是预期 Redis：

```bash
cd scripts/python/test
python input_test_data.py
```

## 常见问题

### 1. 后端启动后扩展显示无法连接怎么办？

- 确认后端实际监听地址和端口。
- 确认 `extension/manifest.json` 中的 `host_permissions` 包含后端地址。
- 确认 Redis 配置中的源库 `host` 不为空。
- 先用 `curl http://127.0.0.1:8642/api/health` 检查后端是否可用。

### 2. Redis 连接失败怎么办？

- 检查 Redis Server 是否启动。
- 检查 `host`、`port`、`db`、`password` 是否正确。
- 如果 Redis 没有密码，`password` 建议传 `null`。
- 如果使用 Docker 或远程 Redis，检查网络、防火墙和 Redis bind 配置。

### 3. 本地化接口返回 MessagePack 解析失败怎么办？

说明目标 Field 的值不是当前服务预期的 MessagePack 格式。可以先使用 `/api/redis/hash/get` 查看 `decoded_type` 和 `raw_base64`，确认数据类型后再处理。

### 4. 备份很慢怎么办？

当前 Rust 后端使用 `KEYS *` 枚举源库，并逐个 `DUMP`/`RESTORE`。对于非常大的库，建议在低峰期执行，或后续改造为基于 `SCAN` 和 Pipeline 的实现。

### 5. Python 脚本适合继续使用吗？

可以作为历史工作流或离线辅助工具使用。但如果需要 HTTP API、浏览器面板或异步批处理，建议优先使用 Rust 后端。
