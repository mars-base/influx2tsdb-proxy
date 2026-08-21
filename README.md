# influx2tsdb-proxy

InfluxDB 1.x protocol adapter for TimescaleDB. Exposes InfluxDB 1.x HTTP API (`/ping`, `/write`, `/query`, `/debug/vars`), translates Line Protocol and InfluxQL to PostgreSQL/TimescaleDB.

Grafana can use InfluxDB datasource to query TimescaleDB data transparently.

## Features

- InfluxDB 1.x HTTP API compatible (`/ping`, `/write`, `/query`, `/debug/vars`)
- Line Protocol parser (measurement, tags, fields, timestamp)
- InfluxQL to SQL translation (aggregations, `time_bucket`, subqueries, `last()` → `DISTINCT ON`)
- Retention policy CRUD (`CREATE / ALTER / DROP / SHOW RETENTION POLICY`) with automatic TimescaleDB sync
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
  -pool 10 \
  -verbose
```

| Flag | Default | Description |
|------|---------|-------------|
| `-pg` | *(required)* | PostgreSQL connection string |
| `-port` | `8087` | HTTP listen port |
| `-pool` | `10` | Connection pool size |
| `-verbose` | `false` | Enable SQL and query detail logging |

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

## Retention Policy

Supports InfluxDB 1.x retention policy CRUD, automatically syncing to TimescaleDB's native retention policy via `add_retention_policy`.

| InfluxQL | Description |
|----------|-------------|
| `CREATE RETENTION POLICY "rp_7d" ON "db" DURATION 7d REPLICATION 1 DEFAULT` | Create policy, set as default |
| `ALTER RETENTION POLICY "rp_7d" ON "db" DURATION 30d` | Modify duration |
| `DROP RETENTION POLICY "rp_7d" ON "db"` | Delete policy |
| `SHOW RETENTION POLICIES ON "db"` | List all policies |

### How It Works

- Policies are stored in the `_retention_policy` metadata table
- The **default** policy is applied to all hypertables (InfluxDB retention is per-database)
- `CREATE / ALTER / DROP` trigger immediate sync to TimescaleDB; a background sync runs every 5 minutes
- Duration formats: `7d`, `168h`, `30d`, `1w`, `INF` / `0s` (infinite = no retention)
- `DROP` removes TimescaleDB retention jobs when no default policy remains

## Subquery Example

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

## Deployment (Ansible)

Automated deployment via Ansible with supervisor process management. Supports multiple instances on a single host.

### Quick Setup

```bash
# Download ansible playbooks
curl -sL https://raw.githubusercontent.com/mars-base/influx2tsdb-proxy/main/ansible/install.sh | bash
cd /srv/influx2tsdb-proxy

# Create inventory
cat > hosts << 'EOF'
[tsdb_servers]
192.168.1.100
EOF

# Create instance config
cat > group_vars/tsdb_servers.yml << 'EOF'
influx2tsdb_proxy_instances:
  - name: default
    pg_dsn: "postgres://user:pass@host:5432/tsdb?sslmode=disable"
    port: 8087
EOF

# Deploy
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml
```

### Multi-Instance Deployment

```yaml
# group_vars/tsdb_servers.yml
influx2tsdb_proxy_instances:
  - name: game
    pg_dsn: "postgres://user:pass@db1:5432/game_tsdb"
    port: 8087
    pool_size: 20
    verbose: true
  - name: monitor
    pg_dsn: "postgres://user:pass@db2:5432/monitor_tsdb"
    port: 8088
    pool_size: 10
    dir: /opt/influx2tsdb-proxy-monitor
```

Each instance has:
- Independent directory: `/srv/influx2tsdb-proxy-<name>` (or custom `dir`)
- Independent port and PostgreSQL DSN
- Independent supervisor process: `influx2tsdb-proxy-<name>`
- Independent log files: `<dir>/logs/out_<name>.log`

### Instance Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `name` | *(required)* | Instance name (used in process name, directory, logs) |
| `pg_dsn` | *(required)* | PostgreSQL/TimescaleDB connection string |
| `port` | `8087` | HTTP listen port |
| `pool_size` | `10` | Connection pool size |
| `verbose` | `false` | Enable SQL and query detail logging |
| `dir` | `/srv/influx2tsdb-proxy-<name>` | Installation directory |

Global variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `influx2tsdb_proxy_version` | `latest` | Release version (e.g., `v1.0.0`) |

### Specify Version

```bash
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml \
  -e "influx2tsdb_proxy_version=v1.0.0"
```

### Management

```bash
# Check all instances
ansible tsdb_servers -m shell -a "supervisorctl status"

# Restart all instances
ansible tsdb_servers -m shell -a "supervisorctl restart 'influx2tsdb-proxy:*'"

# Restart specific instance
ansible tsdb_servers -m shell -a "supervisorctl restart influx2tsdb-proxy-game"

# View logs
ansible tsdb_servers -m shell -a "tail -50 /srv/influx2tsdb-proxy-game/logs/out_game.log"

# Update binary only (skip supervisor setup)
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml --tags "sync"
```

See [ansible/README.md](ansible/README.md) for detailed documentation.

## Testing

See [docs/influxdb-vs-proxy-test.md](docs/influxdb-vs-proxy-test.md) for the full comparison test guide.

### Quick Start

```bash
./influx2tsdb-proxy -pg "postgres://user:pass@host:port/db"
```

### influx CLI Compatibility (InfluxDB shell v1.11.8)

Tested with dual-write sampling: `DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=game_monitor python3 scripts/sample_online_influx.py`

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
| 11 | `CREATE RETENTION POLICY "rp_7d" ... DURATION 7d` | ✅ | ✅ | ✅ |
| 12 | `SHOW RETENTION POLICIES` | autogen + rp_7d, shardGroupDuration match | identical | ✅ |
| 13 | `ALTER RETENTION POLICY "rp_7d" ... DURATION 1d` | ✅ | ✅ | ✅ |
| 14 | `DROP RETENTION POLICY "rp_7d"` | ✅ | ✅ | ✅ |

Known minor differences:
- **SHOW DATABASES**: InfluxDB lists all databases; proxy returns only the requested `db` parameter
- **Time precision in `last()`**: InfluxDB uses native nanoseconds; TimescaleDB stores milliseconds, converted to ns for epoch output
- **Count time column**: InfluxDB returns the latest write timestamp; proxy returns `0` for non-time-grouped aggregates (standard InfluxDB behavior)

## License

MIT
