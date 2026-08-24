# influx2tsdb-proxy Stress Test Results

## 测试环境

| 组件 | 地址 | 说明 |
|------|------|------|
| InfluxDB 1.x | `localhost:8086` | 原生时序数据库 |
| influx2tsdb-proxy | `localhost:8087` | Proxy 代理 → TimescaleDB |
| TimescaleDB (PostgreSQL) | `10.241.21.97:5433` | 底层存储 |

## 测试参数

| 参数 | 值 |
|------|-----|
| 总数据量 | 100,000 points |
| 批次大小 | 1,000 pts/batch |
| 时间跨度 | 30 分钟 |
| 查询轮次 | 5 次取中位数 |
| SHARD DURATION | 1 分钟（proxy 压缩测试） |

## 写入性能

| 指标 | InfluxDB | Proxy | 比率 |
|------|----------|-------|------|
| 总耗时 | 332ms | 9.1s | 27x |
| 吞吐量 | 301,657 pts/s | 11,017 pts/s | 27x |
| 批次 p50 | 2.5ms | 89ms | 36x |
| 批次 p99 | 12ms | 102ms | 9x |

## 查询性能（8 类查询 × 5 次取中位数）

| 查询 | InfluxDB | Proxy 未压缩 | Proxy 压缩后 | 压缩/未压缩 |
|------|----------|-------------|-------------|------------|
| count | 142µs | 79ms | 75ms | 0.95x ✓ |
| mean GROUP BY time(1h) | 168µs | 80ms | 75ms | 0.94x ✓ |
| sum GROUP BY time(30m) | 142µs | 79ms | 76ms | 0.96x |
| last GROUP BY server_id | 141µs | 79ms | 78ms | 0.98x |
| mean+max+min time(10m) | 176µs | 79ms | 77ms | 0.97x |
| subquery sum(last) | 145µs | 80ms | 78ms | 0.98x |
| GROUP BY tag+time | 210µs | 79ms | 76ms | 0.96x |
| SHOW TAG VALUES | 114µs | 84ms | 83ms | 0.99x |
| **TOTAL** | **1.2ms** | **640ms** | **618ms** | **0.97x** |

## 压缩效果

- 通过 InfluxQL 语法设置保留策略触发压缩：
  ```
  CREATE RETENTION POLICY "compress_test" ON "stress_test" DURATION 1h REPLICATION 1 SHARD DURATION 1m DEFAULT
  ```
- `compress_after = 10s`，`schedule_interval = 1min`
- 90 秒等待后数据自动压缩
- 压缩后聚合查询略快 ~3%，TimescaleDB 的 decompress 开销很小

## 结论

1. **写入**：Proxy 约 11k pts/s，比原生 InfluxDB 慢 27 倍，满足日常监控场景
2. **查询**：Proxy 比 InfluxDB 慢约 500 倍（InfluxDB 内存缓存 vs PostgreSQL 磁盘查询 + 网络开销）
3. **压缩**：压缩后查询略快 ~3%，decompress 开销可忽略
4. **压缩触发**：短保留策略（1h）配合 SHARD DURATION 可在 90 秒内完成自动压缩

## 运行方式

```bash
go run ./test/stress.go
```
