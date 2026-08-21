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

See [docs/influxdb-vs-proxy-test.md](docs/influxdb-vs-proxy-test.md) for the full comparison test guide between InfluxDB 1.x and influx2tsdb-proxy.

The test covers three dimensions:

1. **Write interface** — single/multi-line Line Protocol, invalid input, concurrent dual-write
2. **Read interface** — `/ping`, `/debug/vars`, `SHOW` queries, aggregations (`mean`/`sum`/`max`/`min`/`count`/`last`), subqueries, epoch time format
3. **Grafana panel verification** — 7 panels (stat, timeseries, bargauge, piechart) rendered side-by-side from real InfluxDB and proxy datasources

Quick start:

```bash
# 1. Start proxy
./influx2tsdb-proxy -pg "postgres://user:pass@host:port/db"

# 2. Run dual-write sampling (writes identical data to both InfluxDB and proxy)
DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=tsdb python3 scripts/sample_online_influx.py

# 3. Compare responses
curl -s "http://localhost:8086/query?db=game_monitor&epoch=ms&q=SELECT%20mean(%22online_count%22)%20FROM%20%22server_online%22%20WHERE%20time%20%3E%20now()%20-%205m%20GROUP%20BY%20time(30s)"
curl -s "http://localhost:8087/query?db=tsdb&epoch=ms&q=SELECT%20mean(%22online_count%22)%20FROM%20%22server_online%22%20WHERE%20time%20%3E%20now()%20-%205m%20GROUP%20BY%20time(30s)"
```

## License

MIT
