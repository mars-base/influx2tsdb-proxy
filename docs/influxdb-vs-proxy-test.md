# InfluxDB 1.x vs influx2tsdb-proxy Comparison Test

Verify the influx2tsdb-proxy is API-compatible with InfluxDB 1.x by comparing write/read responses and Grafana panel rendering side-by-side.

## Environment

| Component | Address | Database |
|-----------|---------|----------|
| InfluxDB 1.x | `localhost:8086` | `game_monitor` |
| influx2tsdb-proxy | `localhost:8087` | `tsdb` |
| TimescaleDB (PostgreSQL) | `192.168.1.100:5433` | `tsdb` |
| Grafana | `192.168.1.200:3000` | — |

> **Note**: `tsdb` 是 PostgreSQL 数据库名，需要预先安装 TimescaleDB 扩展。proxy 本身不管理数据库，只通过 PostgreSQL 协议读写 TimescaleDB 表。

### Prerequisites

- InfluxDB 1.x running on port 8086
- PostgreSQL + TimescaleDB extension:
  ```sql
  CREATE DATABASE tsdb;
  \c tsdb
  CREATE EXTENSION IF NOT EXISTS timescaledb;
  ```
- influx2tsdb-proxy binary built (`go build -o influx2tsdb-proxy`)
- Grafana with admin access

---

## 1. Setup

### 1.1 Clean test data

```bash
# Drop measurement from InfluxDB
curl -s -X POST "http://localhost:8086/query?db=game_monitor" \
  --data-urlencode 'q=DROP MEASUREMENT "server_online"'

# Drop table from TimescaleDB
PGPASSWORD=tsdbpass123 psql -h 192.168.1.100 -p 5433 -U dba -d tsdb \
  -c "DROP TABLE IF EXISTS server_online"
```

### 1.2 Start proxy

```bash
cd ~/bucket/influxdb-tsdb-proxy
go build -o influx2tsdb-proxy
nohup ./influx2tsdb-proxy -pg "postgres://dba:tsdbpass123@192.168.1.100:5433/tsdb" \
  > /tmp/influx2tsdb-proxy.log 2>&1 &
```

### 1.3 Start dual-write sampling

```bash
cd ~/bucket/influxdb-tsdb-proxy
DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=tsdb \
  nohup python3 scripts/sample_online_influx.py > /tmp/sample_dual.log 2>&1 &
```

This writes identical data to both InfluxDB (8086) and the proxy (8087) every 5 seconds, simulating 8 game servers across 4 regions.

---

## 2. Write Interface Comparison

### 2.1 Single line write

```bash
# InfluxDB
curl -i -X POST "http://localhost:8086/write?db=game_monitor" \
  --data-binary 'server_online,server_id=test_s1,region=test online_count=100i'

# Proxy
curl -i -X POST "http://localhost:8087/write?db=tsdb" \
  --data-binary 'server_online,server_id=test_s1,region=test online_count=100i'
```

**Expected**: Both return `HTTP/1.1 204 No Content`.

### 2.2 Multi-line write

```bash
curl -i -X POST "http://localhost:8086/write?db=game_monitor" \
  --data-binary 'server_online,server_id=test_s2,region=test online_count=200i
server_online,server_id=test_s3,region=test online_count=300i'

curl -i -X POST "http://localhost:8087/write?db=tsdb" \
  --data-binary 'server_online,server_id=test_s2,region=test online_count=200i
server_online,server_id=test_s3,region=test online_count=300i'
```

**Expected**: Both return `204`.

### 2.3 Verify data written

```bash
# InfluxDB
curl -s "http://localhost:8086/query?db=game_monitor&q=SELECT%20count(*)%20FROM%20%22server_online%22" | python3 -m json.tool

# Proxy
curl -s "http://localhost:8087/query?db=tsdb&q=SELECT%20count(*)%20FROM%20%22server_online%22" | python3 -m json.tool
```

**Expected**: Both return similar `count_online_count` values (proxy may include dual-write data).

### 2.4 Invalid Line Protocol

```bash
# Both should return 400
curl -i -X POST "http://localhost:8086/write?db=game_monitor" --data-binary 'invalid data'
curl -i -X POST "http://localhost:8087/write?db=tsdb" --data-binary 'invalid data'
```

---

## 3. Read Interface Comparison

### 3.1 /ping

```bash
echo "=== InfluxDB ==="
curl -sI http://localhost:8086/ping | head -5

echo "=== Proxy ==="
curl -sI http://localhost:8087/ping | head -5
```

**Expected**: Both return `204` with `X-Influxdb-Version: 1.x`.

### 3.2 /debug/vars

```bash
curl -s http://localhost:8086/debug/vars | python3 -m json.tool
curl -s http://localhost:8087/debug/vars | python3 -m json.tool
```

### 3.3 SHOW queries

```bash
queries=(
  "SHOW DATABASES"
  "SHOW MEASUREMENTS"
  "SHOW TAG KEYS FROM \"server_online\""
  "SHOW TAG VALUES FROM \"server_online\" WITH KEY = \"server_id\""
  "SHOW FIELD KEYS FROM \"server_online\""
  "SHOW RETENTION POLICIES ON \"game_monitor\""
)

for q in "${queries[@]}"; do
  encoded=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$q'))")
  echo "=== $q ==="
  echo "--- InfluxDB (8086) ---"
  curl -s "http://localhost:8086/query?db=game_monitor&q=$encoded" | python3 -m json.tool
  echo "--- Proxy (8087) ---"
  curl -s "http://localhost:8087/query?db=tsdb&q=$encoded" | python3 -m json.tool
  echo ""
done
```

### 3.4 Aggregation queries

```bash
queries=(
  "SELECT mean(\"online_count\") FROM \"server_online\" WHERE time > now() - 5m GROUP BY time(30s)"
  "SELECT sum(\"online_count\") FROM \"server_online\" WHERE time > now() - 5m GROUP BY time(30s)"
  "SELECT max(\"online_count\") FROM \"server_online\" WHERE time > now() - 5m GROUP BY time(30s)"
  "SELECT min(\"online_count\") FROM \"server_online\" WHERE time > now() - 5m GROUP BY time(30s)"
  "SELECT count(\"online_count\") FROM \"server_online\" WHERE time > now() - 5m GROUP BY time(30s)"
  "SELECT last(\"online_count\") FROM \"server_online\" WHERE time > now() - 5s GROUP BY \"server_id\""
  "SELECT mean(\"online_count\") FROM \"server_online\" WHERE time > now() - 5m GROUP BY time(30s), \"server_id\""
)

for q in "${queries[@]}"; do
  encoded=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$q'))")
  echo "=== $q ==="
  echo "--- InfluxDB (8086) ---"
  curl -s "http://localhost:8086/query?db=game_monitor&epoch=ms&q=$encoded" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d['results'][0].get('series',[{'values':[]}])[0].get('values',[])[:3], indent=2))"
  echo "--- Proxy (8087) ---"
  curl -s "http://localhost:8087/query?db=tsdb&epoch=ms&q=$encoded" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d['results'][0].get('series',[{'values':[]}])[0].get('values',[])[:3], indent=2))"
  echo ""
done
```

### 3.5 Subqueries (Grafana panel queries)

```bash
queries=(
  # Panel 1: current total online (stat)
  'SELECT sum("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE time > now() - 5s GROUP BY "server_id")'
  # Panel 2: active servers (stat)
  'SELECT count("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE time > now() - 5s GROUP BY "server_id")'
  # Panel 4: total online trend (timeseries)
  'SELECT "total" FROM (SELECT sum("online_count") AS total FROM "server_online" WHERE time > now() - 1m GROUP BY time(5s))'
  # Panel 5: per-server (timeseries)
  'SELECT mean("online_count") FROM "server_online" WHERE time > now() - 1m GROUP BY time(10s), "server_id" fill(null)'
  # Panel 7: region pie chart
  'SELECT sum("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE time > now() - 5s GROUP BY "server_id", "region") GROUP BY "region"'
)

for q in "${queries[@]}"; do
  encoded=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$q'''))")
  echo "=== $q ==="
  echo "--- InfluxDB (8086) ---"
  curl -s "http://localhost:8086/query?db=game_monitor&epoch=ms&q=$encoded" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d['results'][0].get('series',[{'values':[]}])[0].get('values',[])[:3], indent=2))"
  echo "--- Proxy (8087) ---"
  curl -s "http://localhost:8087/query?db=tsdb&epoch=ms&q=$encoded" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d['results'][0].get('series',[{'values':[]}])[0].get('values',[])[:3], indent=2))"
  echo ""
done
```

### 3.6 Response format validation

Both endpoints must return InfluxDB-compatible JSON:

```json
{
  "results": [{
    "statement_id": 0,
    "series": [{
      "name": "server_online",
      "tags": {"server_id": "s1"},
      "columns": ["time", "mean"],
      "values": [[1787223230000, 3500.5]]
    }]
  }]
}
```

Key format requirements (when `epoch=ms`):
- `time` column: epoch milliseconds (integer), NOT RFC3339 string
- Value columns: `float64` (not `int64`), to prevent Grafana parsing as string
- Tag grouping: separate `series[]` entries with `tags` map

---

## 4. Grafana Panel Verification

### 4.1 Create datasources

```bash
# Real InfluxDB datasource
curl -s -X POST "http://admin:<password>@192.168.1.200:3000/api/datasources" \
  -H "Content-Type: application/json" -d '{
    "name": "influxdb-real",
    "type": "influxdb",
    "uid": "ffvqbfm3e3a4ga",
    "url": "http://localhost:8086",
    "database": "game_monitor",
    "access": "proxy"
  }'

# Proxy datasource
curl -s -X POST "http://admin:<password>@192.168.1.200:3000/api/datasources" \
  -H "Content-Type: application/json" -d '{
    "name": "influxdb-tsdb-proxy",
    "type": "influxdb",
    "uid": "efvqp59hcterke",
    "url": "http://192.168.1.100:8087",
    "database": "tsdb",
    "access": "proxy"
  }'
```

### 4.2 Import dashboards

```bash
# Real InfluxDB dashboard (UID: influxdb-real)
curl -s -X POST "http://admin:<password>@192.168.1.200:3000/api/dashboards/db" \
  -H "Content-Type: application/json" \
  -d @docs/grafana-influxdb-monitor.json

# Proxy dashboard (UID: influx2tsdb-proxy)
curl -s -X POST "http://admin:<password>@192.168.1.200:3000/api/dashboards/db" \
  -H "Content-Type: application/json" \
  -d @docs/grafana-influxdb-proxy-monitor.json
```

### 4.3 Panel verification checklist

Open both dashboards side-by-side in Grafana:

| # | Panel | Type | Verify |
|---|-------|------|--------|
| 1 | Current Total Online | `stat` | Both show similar total (e.g. ~21k) |
| 2 | Active Servers | `stat` | Both show `8` |
| 3 | Peak Online | `stat` | Both show peak within time range |
| 4 | Total Online Trend | `timeseries` | Both show line chart with data points |
| 5 | Per-Server Online | `timeseries` | Both show 8 lines (s1-s8) |
| 6 | Current Per-Server | `bargauge` | Both show 8 bars with proportional scaling |
| 7 | Region Pie Chart | `piechart` | Both show 4 regions (华东/华南/华北/西南) |

**Visual comparison points:**
- Data values should be close (same input data via dual-write)
- Time axis alignment on timeseries panels
- Series count and tag labels match
- No "Data outside time range" or "No data" errors
- Color thresholds render consistently

---

## 5. Known Differences

### 5.1 Duplicate data handling

| Behavior | InfluxDB | Proxy (TimescaleDB) |
|----------|----------|---------------------|
| Same tag + timestamp | Overwrites previous point | Inserts new row (no dedup) |
| Impact | `count()` reflects unique points | `count()` may be higher with concurrent writes |

**Mitigation**: Avoid running multiple write processes targeting the same proxy simultaneously, or add `DISTINCT ON` dedup in queries.

### 5.2 `last()` / `first()` implementation

| Implementation | InfluxDB | Proxy |
|----------------|----------|-------|
| Mechanism | Native storage engine | `DISTINCT ON` + `ORDER BY time DESC/ASC` |
| Performance | O(1) per group | O(log n) per group with B-tree index |
| Optimization | — | LATERAL JOIN for per-tag index lookup |

### 5.3 Epoch millisecond WHERE clause

Grafana sends time filters as `time > 1787221389201ms`. The proxy converts this to RFC3339 timestamps in SQL:

```
time > 1787221389201ms  →  time > '2026-08-20T10:23:09Z'
```

### 5.4 Numeric type serialization

Go `int64` serializes as JSON integer, which Grafana's InfluxDB plugin may misinterpret as string. The proxy normalizes all numeric values to `float64` to match InfluxDB's behavior.

### 5.5 Response time format

| `epoch` parameter | InfluxDB | Proxy |
|-------------------|----------|-------|
| `epoch=ms` | Integer milliseconds | Integer milliseconds (`float64`) |
| No epoch | RFC3339 string | RFC3339Nano string |
| Stat queries (no time grouping) | `0` | `0` |

---

## 6. Cleanup

```bash
# Stop sampling
pkill -f sample_online_influx

# Stop proxy
pkill -f influx2tsdb-proxy

# Clean data
curl -s -X POST "http://localhost:8086/query?db=game_monitor" \
  --data-urlencode 'q=DROP MEASUREMENT "server_online"'
PGPASSWORD=tsdbpass123 psql -h 192.168.1.100 -p 5433 -U dba -d tsdb \
  -c "DROP TABLE IF EXISTS server_online"
```
