# InfluxDB 1.x → TimescaleDB 协议适配器（Go 版）

## Context

已有 Grafana + TimescaleDB 和 Grafana + InfluxDB 两套监控面板。本方案构建一个轻量 Go 代理服务，暴露 InfluxDB 1.x HTTP API，但底层读写 TimescaleDB。Grafana 可以用 InfluxDB 数据源语法直接查询 TimescaleDB 中的数据。

## 架构

```
Grafana (InfluxDB datasource, port 8087)
    │
    ├── GET  /ping         → return 204
    ├── POST /write?db=xxx → parse Line Protocol → INSERT INTO TimescaleDB
    ├── GET  /query?q=...  → parse InfluxQL → SQL → TimescaleDB → InfluxDB JSON response
    └── GET  /debug/vars   → minimal JSON
         │
         ▼
    pgx connection pool → pgbouncer:5433 → TimescaleDB (tsdb)
```

## 项目结构

```
tools/influxdb-adapter/
├── main.go              # 入口，HTTP server + 路由
├── config.go            # 环境变量配置
├── handler_ping.go      # /ping, /debug/vars
├── handler_write.go     # /write — Line Protocol 解析 + 写入
├── handler_query.go     # /query — InfluxQL 解析 + SQL 转换 + 响应
├── influxql_parser.go   # InfluxQL → SQL 转换器
├── line_protocol.go     # Line Protocol 解析器
├── meta.go              # measurement/tag/field 元数据管理
├── response.go          # InfluxDB JSON 响应格式构造
├── go.mod
└── go.sum
```

依赖：
- `github.com/jackc/pgx/v5` — PostgreSQL 驱动（高性能，原生支持 pgxpool）
- `github.com/influxdata/influxql` — InfluxQL 官方 AST parser（用于解析 InfluxQL → AST → SQL）

## 需要支持的 InfluxQL 查询

从 `docs/grafana-influxdb-monitor.json` 提取的 7 条真实查询：

| # | 面板 | InfluxQL |
|---|------|----------|
| 1 | 当前总在线 | `SELECT sum("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE time > now() - 5s GROUP BY "server_id")` |
| 2 | 活跃服务器 | `SELECT count("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE time > now() - 5s GROUP BY "server_id")` |
| 3 | 峰值在线 | `SELECT max("total") FROM (SELECT sum("online_count") AS total FROM "server_online" GROUP BY time(5s))` |
| 4 | 总在线趋势 | `SELECT "total" FROM (SELECT sum("online_count") AS total FROM "server_online" WHERE $timeFilter GROUP BY time(5s))` |
| 5 | 各服在线 | `SELECT mean("online_count") FROM "server_online" WHERE $timeFilter GROUP BY time(10s), "server_id" fill(null)` |
| 6 | 各服柱状图 | `SELECT last("online_count") FROM "server_online" WHERE time > now() - 3s GROUP BY "server_id"` |
| 7 | 大区占比饼图 | `SELECT sum("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE time > now() - 5s GROUP BY "server_id", "region") GROUP BY "region"` |

Grafana 还会发送元数据查询：
- `SHOW DATABASES`
- `SHOW MEASUREMENTS`
- `SHOW TAG VALUES FROM "server_online" WITH KEY = "server_id"`
- `SHOW FIELD KEYS FROM "server_online"`
- `CREATE DATABASE "xxx"` (no-op)

## InfluxQL → SQL 转换规则

### 使用 `influxdata/influxql` AST

利用官方 parser 将 InfluxQL 解析为 AST，然后遍历 AST 节点生成 SQL：

```go
stmt, err := influxql.ParseStatement(queryString)
// stmt 可能是 *influxql.SelectStatement, *influxql.ShowMeasurementsStatement 等
```

### 聚合函数

| InfluxQL | SQL |
|----------|-----|
| `mean("field")` | `avg(field)` |
| `sum("field")` | `sum(field)` |
| `max/min("field")` | `max/min(field)` |
| `count("field")` | `count(field)` |
| `last("field")` | 子查询: `DISTINCT ON + ORDER BY time DESC` |
| `first("field")` | 子查询: `DISTINCT ON + ORDER BY time ASC` |

### 时间分组

| InfluxQL | SQL |
|----------|-----|
| `GROUP BY time(5s)` | `GROUP BY time_bucket('5s', time)` |
| `GROUP BY time(10s), "tag"` | `GROUP BY time_bucket('10s', time), tag` |
| `$timeFilter` | `time >= $from AND time <= $to` (从 epoch 参数解析) |
| `time > now() - 5s` | `time > now() - interval '5 seconds'` |

### 子查询转换示例

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
  FROM server_online
  WHERE time > now() - interval '5 seconds'
  ORDER BY server_id, time DESC
) t
```

```
# InfluxQL (双层 GROUP BY):
SELECT sum("val") FROM (
  SELECT last("online_count") AS val
  FROM "server_online"
  WHERE time > now() - 5s
  GROUP BY "server_id", "region"
) GROUP BY "region"

# → SQL:
SELECT region, sum(val) FROM (
  SELECT DISTINCT ON (server_id, region) online_count AS val, region
  FROM server_online
  WHERE time > now() - interval '5 seconds'
  ORDER BY server_id, region, time DESC
) t GROUP BY region
```

### last() 实现

InfluxDB `last()` = 每组最新一条记录。用 PostgreSQL `DISTINCT ON`：
```sql
SELECT DISTINCT ON (server_id) online_count AS val
FROM server_online WHERE time > now() - interval '5 seconds'
ORDER BY server_id, time DESC
```

## Measurement → 表映射

### 元数据表

```sql
CREATE TABLE IF NOT EXISTS _influx_meta (
    measurement TEXT NOT NULL,
    column_name TEXT NOT NULL,
    column_type TEXT NOT NULL,  -- 'tag' | 'field_int' | 'field_float' | 'field_string' | 'field_bool'
    PRIMARY KEY (measurement, column_name)
);
```

### Line Protocol 解析

```
server_online,server_id=s1,region=华东 online_count=3500i 1724140800000000000
```

→ tags: `{server_id: "s1", region: "华东"}`
→ fields: `{online_count: 3500}` (integer)
→ timestamp: nanoseconds → `time.Time`
→ measurement: `server_online`

### 自动建表（首次写入时）

```sql
CREATE TABLE IF NOT EXISTS server_online (
    time TIMESTAMPTZ NOT NULL,
    server_id TEXT,   -- tag
    region TEXT,      -- tag
    online_count BIGINT  -- field (integer)
);
SELECT create_hypertable('server_online', by_range('time'), if_not_exists => true);
```

## InfluxDB JSON 响应格式

### 时间序列查询

```json
{
  "results": [{
    "statement_id": 0,
    "series": [{
      "name": "server_online",
      "tags": {"server_id": "s1"},
      "columns": ["time", "mean"],
      "values": [["2026-08-20T10:00:00Z", 3500]]
    }]
  }]
}
```

### SHOW 查询

```json
{
  "results": [{
    "series": [{
      "name": "server_online",
      "columns": ["tagKey"],
      "values": [["server_id"], ["region"]]
    }]
  }]
}
```

## 核心代码结构

### main.go

```go
func main() {
    cfg := LoadConfig()
    pool, err := pgxpool.New(ctx, cfg.PGConnString())
    // ...
    s := &Server{pool: pool, meta: NewMetaStore(pool)}
    http.HandleFunc("/ping", s.handlePing)
    http.HandleFunc("/write", s.handleWrite)
    http.HandleFunc("/query", s.handleQuery)
    http.HandleFunc("/debug/vars", s.handleDebugVars)
    log.Printf("InfluxDB adapter listening on :%d", cfg.Port)
    http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), nil)
}
```

### config.go

```go
type Config struct {
    Port     int    // ADAPTER_PORT, default 8087
    PGHost   string // PG_HOST, default 10.241.21.97
    PGPort   int    // PG_PORT, default 5433
    PGUser   string // PG_USER, default dba
    PGPass   string // PG_PASSWORD
    PGDB     string // PG_DATABASE, default tsdb
}
```

## 实现步骤

### Step 1: 项目初始化 + /ping + /debug/vars

- `go mod init`, 引入 pgx/v5 和 influxql
- 基础 HTTP server，标准库 `net/http`
- `/ping` 返回 204 + `X-Influxdb-Version: 1.11.8`
- `/debug/vars` 返回 `{}`

### Step 2: Line Protocol 写入 + /write

- 手写 Line Protocol parser（influxdb 官方 Go 库 `github.com/influxdata/line-protocol` 也可以用）
- 解析 measurement, tags, fields, timestamp
- 自动建表 + 元数据记录
- pgxpool 批量 INSERT
- 连接池管理（pgxpool 默认支持）

### Step 3: InfluxQL 查询解析 + /query

- 用 `influxdata/influxql` 解析为 AST
- 递归遍历 AST 节点，生成 SQL
  - `SelectStatement` → SELECT + WHERE + GROUP BY
  - 聚合函数 `Call` 节点 → SQL 聚合
  - `DurationLiteral` → `time_bucket` 间隔
  - `BinaryExpr` with `time > now() - 5s` → interval 表达式
  - 子查询 `SelectStatement.Source` 为 `*SelectStatement` 时递归处理
- `last()` → `DISTINCT ON` 重写
- SQL 执行 + 结果格式化为 InfluxDB JSON

### Step 4: SHOW 查询

- `ShowDatabasesStatement` → 返回 `["tsdb"]`
- `ShowMeasurementsStatement` → 查 `_influx_meta` 或 `information_schema.tables`
- `ShowTagValuesStatement` → `SELECT DISTINCT` 查 tag 列
- `ShowFieldKeysStatement` → 查 `_influx_meta` 的 field 列

### Step 5: 编译 + 测试验证

- `go build -o influxdb-adapter`
- 启动适配器（port 8087）
- 复用 `sample_online_influx.py` 写入（改端口为 8087）
- Grafana 添加 InfluxDB 数据源指向 8087
- 应用 InfluxDB 面板 JSON，验证所有 7 个面板数据正确

## 构建和启动

```bash
# 编译
cd tools/influxdb-adapter
go build -o influxdb-adapter

# 启动
nohup ./influxdb-adapter > /tmp/influxdb-adapter.log 2>&1 &

# 写入数据（复用 Python 采样脚本，改端口）
INFLUX_PORT=8087 PYTHONUNBUFFERED=1 nohup python3 scripts/sample_online_influx.py > /tmp/sample_influx_adapter.log 2>&1 &
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ADAPTER_PORT` | 8087 | HTTP 监听端口 |
| `PG_HOST` | 127.0.0.1 | TimescaleDB host |
| `PG_PORT` | 5432 | TimescaleDB port |
| `PG_USER` | dba | 数据库用户 |
| `PG_PASSWORD` | *(required)* | 数据库密码 |
| `PG_DATABASE` | tsdb | 数据库名 |
| `PG_POOL_SIZE` | 10 | pgxpool 最大连接数 |

## 验证步骤

```bash
# 1. 编译启动
cd tools/influxdb-adapter && go build && ./influxdb-adapter

# 2. 测试 /ping
curl -i http://localhost:8087/ping

# 3. 写入数据（复用采样脚本，改端口）
INFLUX_PORT=8087 python3 scripts/sample_online_influx.py

# 4. 测试查询
curl "http://localhost:8087/query?db=game_monitor&q=SELECT%20mean(%22online_count%22)%20FROM%20%22server_online%22%20WHERE%20time%20%3E%20now()%20-%201m%20GROUP%20BY%20time(10s)"

# 5. Grafana 添加 InfluxDB 数据源 → http://localhost:8087, db=game_monitor
# 6. 导入面板 JSON，验证数据
```

## 注意事项

- 使用 `influxdata/influxql` 官方 AST parser，语法覆盖完整且可靠
- `last()` 用 `DISTINCT ON` 实现，性能优于子查询 + LATERAL
- `$timeFilter` 需要从 Grafana 发送的 `epoch` 参数（ms 级时间戳）解析
- `fill(null)` 暂时忽略，Grafana 对空值有默认处理
- Line Protocol 的 timestamp 是纳秒，Go `time.Unix(sec, nsec)` 原生支持
- pgxpool 连接池天然支持高并发写入
- 单一静态二进制，部署简单，无运行时依赖
- 适配器是开发/测试用途，不做认证
