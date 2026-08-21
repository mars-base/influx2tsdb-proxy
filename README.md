# influx2tsdb-proxy

InfluxDB 1.x protocol adapter for TimescaleDB. Exposes InfluxDB 1.x HTTP API (`/ping`, `/write`, `/query`, `/debug/vars`), translates Line Protocol and InfluxQL to PostgreSQL/TimescaleDB.

Grafana can use InfluxDB datasource to query TimescaleDB data transparently.

## Features

- InfluxDB 1.x HTTP API compatible (`/ping`, `/write`, `/query`, `/debug/vars`)
- Line Protocol parser (measurement, tags, fields, timestamp)
- InfluxQL to SQL translation (aggregations, `time_bucket`, subqueries, `last()` → `DISTINCT ON`)
- Auto table and hypertable creation with metadata tracking
- `SHOW DATABASES / MEASUREMENTS / TAG VALUES / FIELD KEYS` support
- TimescaleDB extension auto-detection and creation
- Cross-platform builds (Linux / macOS / Windows, amd64 + arm64)

## Requirements

- Go 1.21+
- PostgreSQL with TimescaleDB extension (auto-created if missing)

## Architecture

```
Grafana (InfluxDB datasource)
    │
    ├── GET  /ping         → 204 + X-Influxdb-Version
    ├── POST /write?db=xxx → Line Protocol → INSERT INTO TimescaleDB
    ├── GET  /query?q=...  → InfluxQL → SQL → TimescaleDB → InfluxDB JSON
    └── GET  /debug/vars   → {}
         │
         ▼
    pgxpool → TimescaleDB
```

## Usage

```bash
# Required: PostgreSQL/TimescaleDB connection string
influx2tsdb-proxy -pg "postgres://user:pass@host:port/db?sslmode=disable"

# Optional flags
influx2tsdb-proxy \
  -pg "postgres://user:pass@host:port/db?sslmode=disable" \
  -port 8087 \
  -pool 10
```

| Flag | Default | Description |
|------|---------|-------------|
| `-pg` | *(required)* | PostgreSQL connection string |
| `-port` | `8087` | HTTP listen port |
| `-pool` | `10` | Connection pool size |

### Development

```bash
# Build and run
PG_DSN="postgres://user:pass@host:port/db" make run
```

## InfluxQL Translation

| InfluxQL | SQL |
|----------|-----|
| `mean("f")` | `avg(f)` |
| `sum/max/min/count("f")` | `sum/max/min/count(f)` |
| `last("f")` | `DISTINCT ON + ORDER BY time DESC` |
| `first("f")` | `DISTINCT ON + ORDER BY time ASC` |
| `GROUP BY time(5s)` | `GROUP BY time_bucket('5s', time)` |
| `time > now() - 5s` | `time > now() - interval '5 seconds'` |
| `$timeFilter` | `time >= $from AND time <= $to` |

### Subquery Example

```sql
-- InfluxQL
SELECT sum("val") FROM (
  SELECT last("online_count") AS val
  FROM "server_online"
  WHERE time > now() - 5s
  GROUP BY "server_id"
)

-- Translated SQL
SELECT sum(val) FROM (
  SELECT DISTINCT ON (server_id) online_count AS val
  FROM server_online
  WHERE time > now() - interval '5 seconds'
  ORDER BY server_id, time DESC
) t
```

## Build

```bash
# Current platform
make build

# Linux amd64
make linux

# All platforms (linux/darwin/windows, amd64+arm64)
make cross
```

Binary output: `build/influx2tsdb-proxy`

## Testing

See [docs/influxdb-vs-proxy-test.md](docs/influxdb-vs-proxy-test.md) for the full comparison test guide.

### Quick Start

```bash
# 1. Start proxy
./influx2tsdb-proxy -pg "postgres://user:pass@host:port/db"

# 2. Run dual-write sampling (writes identical data to both InfluxDB and proxy)
DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=game_monitor python3 scripts/sample_online_influx.py
```

### influx CLI Compatibility (InfluxDB shell v1.11.8)

Tested with `influx -host localhost -port 8087 -database game_monitor -execute "..."` using dual-write sampling.
Both endpoints return identical data for all supported commands:

| # | Command | InfluxDB 8086 | Proxy 8087 | Match |
|---|---------|:---:|:---:|:---:|
| 1 | `SHOW DATABASES` | 4 databases | 1 (request db) | ✅ |
| 2 | `SHOW MEASUREMENTS` | `server_online` | `server_online` | ✅ |
| 3 | `SHOW TAG KEYS FROM server_online` | `region`, `server_id` | `region`, `server_id` | ✅ |
| 4 | `SHOW TAG VALUES ... WITH KEY=server_id` | s1-s8 | s1-s8 | ✅ |
| 5 | `SHOW FIELD KEYS FROM server_online` | `online_count integer` | `online_count integer` | ✅ |
| 6 | `SELECT mean(...) GROUP BY time(10s)` | identical values | identical values | ✅ |
| 7 | `SELECT sum(...) GROUP BY time(10s)` | identical values | identical values | ✅ |
| 8 | `SELECT last(...) GROUP BY server_id` | 8 servers, values match | 8 servers, values match | ✅ |
| 9 | `SELECT count(...) WHERE time > now()-1m` | 96 | 96 | ✅ |
| 10 | Subquery `SELECT sum(last()) GROUP BY server_id` | 21430 | 21430 | ✅ |

Known minor differences:
- **SHOW DATABASES**: InfluxDB lists all databases; proxy returns only the requested `db` parameter
- **Time precision in `last()`**: InfluxDB uses native nanoseconds; TimescaleDB stores milliseconds, converted to ns for epoch output
- **Count time column**: InfluxDB returns the latest write timestamp; proxy returns `0` for non-time-grouped aggregates (standard InfluxDB behavior)

## License

MIT
