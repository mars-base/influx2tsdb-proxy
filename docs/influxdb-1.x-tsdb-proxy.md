# InfluxDB 1.x → TimescaleDB 协议适配器

## 概述

influx2tsdb-proxy 是一个轻量 Go 代理服务，暴露 InfluxDB 1.x HTTP API，底层读写 TimescaleDB。Grafana 可用 InfluxDB 数据源语法直接查询 TimescaleDB 中的数据。

## 架构

```
Grafana (InfluxDB datasource)
    │
    ├── GET  /ping              → 204 + X-Influxdb-Version: 1.11.8
    ├── POST /write?db=xxx      → Line Protocol → INSERT INTO schema.hypertable
    ├── GET  /query?q=...       → InfluxQL → SQL → TimescaleDB → InfluxDB JSON
    └── GET  /debug/vars        → {}
         │
         ▼
    pgxpool → PostgreSQL + TimescaleDB
         │
         ├── game_monitor schema (InfluxDB database)
         │   └── server_online (hypertable)
         │
         └── app_metrics schema (InfluxDB database)
             └── requests (hypertable)
```

## 项目结构

```
influxdb-tsdb-proxy/
├── main.go                  # 入口，HTTP server + 路由 + CLI flags
├── Makefile                 # build / cross-compile
├── adapter/
│   ├── meta.go              # MetaStore — 数据库/表/元数据管理 + 自动建表
│   ├── retention.go         # RetentionStore — 保留策略 CRUD + 自动配置
│   ├── influxql_parser.go   # InfluxQL → SQL 转换器（手写递归，无外部依赖）
│   ├── line_protocol.go     # Line Protocol 解析器（支持带/不带时间戳）
│   └── response.go          # InfluxDB JSON 响应格式构造
├── ansible/                 # Ansible 部署角色
│   ├── playbooks/
│   └── roles/
└── docs/
    ├── influxdb-vs-proxy-test.md   # 对比测试指南
    └── grafana-*.json               # Grafana 面板 JSON
```

依赖：
- `github.com/jackc/pgx/v5` — PostgreSQL 驱动

## 多数据库架构

每个 InfluxDB 数据库映射为一个 PostgreSQL schema：

| InfluxDB 操作 | PostgreSQL 对应 |
|--------------|----------------|
| `CREATE DATABASE "mydb"` | `CREATE SCHEMA "mydb"` |
| `DROP DATABASE "mydb"` | `DROP SCHEMA "mydb" CASCADE` |
| `POST /write?db=mydb` | `INSERT INTO "mydb".measurement` |
| `GET /query?db=mydb` | `SELECT FROM "mydb".measurement` |

元数据表（public schema）：

| 表 | 用途 |
|---|------|
| `_influx_databases` | 数据库列表 |
| `_influx_meta` | measurement/tag/field 元数据（含 `db_name`） |
| `_retention_policy` | 保留策略（含 `db_name`，复合主键） |

数据库自动创建：首次写入时自动创建 schema 和默认 `autogen` 保留策略。

## Line Protocol 解析

支持带时间戳和不带时间戳两种格式：

```
# 带时间戳（纳秒级 Unix 时间戳）
server_online,server_id=s1,region=华东 online_count=3500i 1724140800000000000

# 不带时间戳（使用服务端当前时间）
server_online,server_id=s1,region=华东 online_count=3500i
```

→ tags: `{server_id: "s1", region: "华东"}`
→ fields: `{online_count: 3500}` (integer)
→ timestamp: nanoseconds → `time.Time`（或 `time.Now().UTC()`）

## InfluxQL → SQL 转换

### 聚合函数

| InfluxQL | SQL |
|----------|-----|
| `mean("field")` | `avg(field)` |
| `sum/max/min/count("field")` | `sum/max/min/count(field)` |
| `last("field")` | `DISTINCT ON + ORDER BY time DESC` |
| `first("field")` | `DISTINCT ON + ORDER BY time ASC` |

### 时间分组

| InfluxQL | SQL |
|----------|-----|
| `GROUP BY time(5s)` | `GROUP BY time_bucket('5s', time)` |
| `time > now() - 5s` | `time > now() - interval '5 seconds'` |
| `$timeFilter` | `time >= $from AND time <= $to` |

### 支持的查询类型

| InfluxQL | 说明 |
|----------|------|
| `SELECT ... FROM` | 时间序列查询（聚合 + GROUP BY） |
| `SHOW DATABASES` | 返回当前请求的 db |
| `SHOW MEASUREMENTS` | 查 `_influx_meta` |
| `SHOW TAG KEYS` | 查 tag 列 |
| `SHOW TAG VALUES` | `SELECT DISTINCT` tag 列 |
| `SHOW FIELD KEYS` | 查 field 列 |
| `CREATE DATABASE` | 创建 schema + 元数据 |
| `DROP DATABASE` | 删除 schema + 清理元数据 |
| `CREATE RETENTION POLICY` | 创建保留策略 + 自动配置 |
| `ALTER RETENTION POLICY` | 修改保留策略 + 自动配置 |
| `DROP RETENTION POLICY` | 删除保留策略 |
| `SHOW RETENTION POLICIES` | 列出所有策略 |

### 子查询示例

```
# InfluxQL:
SELECT sum("val") FROM (
  SELECT last("online_count") AS val
  FROM "server_online"
  WHERE time > now() - 5s
  GROUP BY "server_id"
)

# → SQL:
SELECT sum(val) FROM (
  SELECT DISTINCT ON (server_id) online_count AS val
  FROM "game_monitor"."server_online"
  WHERE time > now() - interval '5 seconds'
  ORDER BY server_id, time DESC
) t
```

## 保留策略（Retention Policy）

支持 InfluxDB 1.x 标准的 per-database 保留策略 CRUD。

### 最低保留时间

**15 分钟**。低于 15 分钟的 duration 会返回错误。`INF`（无限保留）不受限制。

### 自动配置

设置保留策略时，proxy 自动为所有 hypertable 配置 **chunk interval** 和 **compression policy**：

**规则：**
- Retention < 1d：按公式计算（chunk 最小 1h，compress 最小 15m）
- Retention 1d ~ 7d：chunk = retention / 24，compression = retention / 4
- Retention > 7d：两者均封顶为 1 day

| Retention | Chunk | Compress |
|-----------|-------|----------|
| 1h | 1 hour | 15 min |
| 1d | 1 hour | 6 hours |
| 3d | 3 hours | 18 hours |
| 7d | 7 hours | 42 hours |
| > 7d | 1 day | 1 day |
| INF | 1 day | 1 day |

### 数据清理策略

保留策略是清理时序数据的推荐方式：

| 方式 | 机制 | 性能 |
|------|------|------|
| **保留策略**（推荐） | 自动 `DROP chunk` | 极快 |
| 手动 `DELETE` | 逐行标记删除 | 慢 |
| `drop_chunks()` | 按表清理 | 快 |

### 列式压缩

默认启用 TimescaleDB 列式压缩：
- `compress_segmentby`：使用 tag 列
- `compress_orderby`：`time DESC`
- `compress_after`：按保留时长自动计算

## 构建和部署

```bash
# 构建
make build                        # 当前平台
make linux                        # Linux amd64
make cross                        # 全平台

# 运行
influx2tsdb-proxy \
  -pg "postgres://user:pass@host:5433/tsdb?sslmode=disable" \
  -db game_monitor \
  -port 8087 \
  -pool 20 \
  -verbose
```

### CLI 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-pg` | *(必需)* | PostgreSQL 连接字符串 |
| `-db` | *(自动)* | InfluxDB 数据库名（默认从 -pg 解析） |
| `-port` | `8087` | HTTP 监听端口 |
| `-pool` | `10` | 连接池大小 |
| `-verbose` | `false` | 详细日志（SQL 语句和查询详情） |

### Ansible 部署

```yaml
influx2tsdb_proxy_instances:
  - name: game
    pg_dsn: "postgres://dba:pass@10.241.21.97:5433/tsdb?sslmode=disable"
    db: game_monitor
    port: 8087
    pool_size: 20
```

```bash
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml -e "HOSTS=servers"
```

## 注意事项

- InfluxQL parser 为手写递归实现，无外部依赖（不使用 `influxdata/influxql`）
- `last()` 用 `DISTINCT ON` 实现
- 所有数值统一序列化为 `float64`，兼容 Grafana InfluxDB 插件
- 支持 epoch 毫秒和 RFC3339 两种时间格式输出
- 保留策略修改立即生效，同时触发 TimescaleDB 同步
- 后台每 5 分钟自动同步保留策略和压缩策略
