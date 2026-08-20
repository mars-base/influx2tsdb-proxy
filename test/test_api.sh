#!/bin/bash
# InfluxDB 1.x API Test Suite for influx2tsdb-proxy
# Usage: ./test/test_api.sh [HOST:PORT]

set -e

BASE="${1:-localhost:8087}"
URL="http://$BASE"
PASS=0
FAIL=0
SKIP=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test measurement name (unique to avoid conflicts)
TS=$(date +%s)
MEASUREMENT="test_api_${TS}"

pass() { echo -e "  ${GREEN}✓${NC} $1"; PASS=$((PASS+1)); }
fail() { echo -e "  ${RED}✗${NC} $1"; FAIL=$((FAIL+1)); }
skip() { echo -e "  ${YELLOW}○${NC} $1 (skipped)"; SKIP=$((SKIP+1)); }
section() { echo -e "\n${YELLOW}== $1 ==${NC}"; }

assert_status() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$actual" = "$expected" ]; then
        pass "$desc (HTTP $actual)"
    else
        fail "$desc (expected HTTP $expected, got $actual)"
    fi
}

assert_contains() {
    local desc="$1" body="$2" pattern="$3"
    if echo "$body" | grep -q "$pattern"; then
        pass "$desc"
    else
        fail "$desc (missing: $pattern)"
    fi
}

assert_json_value() {
    local desc="$1" body="$2" jqpath="$3" expected="$4"
    local actual
    actual=$(echo "$body" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    val = d
    for key in '$jqpath'.split('.'):
        if key.isdigit():
            val = val[int(key)]
        elif key.startswith('['):
            val = val[int(key.strip('[]'))]
        else:
            val = val[key]
    print(val)
except:
    print('__ERROR__')
" 2>/dev/null)
    if [ "$actual" = "$expected" ]; then
        pass "$desc"
    else
        fail "$desc (expected: $expected, got: $actual)"
    fi
}

# ============================================================
section "1. /ping"
# ============================================================

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$URL/ping")
assert_status "/ping returns 204" "204" "$STATUS"

HEADER=$(curl -s -I "$URL/ping" 2>/dev/null | grep -i "X-Influxdb-Version" || true)
if echo "$HEADER" | grep -q "1.11.8"; then
    pass "/ping X-Influxdb-Version: 1.11.8"
else
    fail "/ping missing X-Influxdb-Version header"
fi

# ============================================================
section "2. /debug/vars"
# ============================================================

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$URL/debug/vars")
assert_status "/debug/vars returns 200" "200" "$STATUS"

BODY=$(curl -s "$URL/debug/vars")
assert_contains "/debug/vars returns JSON object" "$BODY" "{"

# ============================================================
section "3. /write - Line Protocol"
# ============================================================

# 3a. Basic write with tags and timestamp
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" \
    -d "${MEASUREMENT},server_id=srv001,region=us-east online_count=3500i,cpu_usage=72.5 1755676800000000000")
assert_status "/write basic Line Protocol" "204" "$STATUS"

# 3b. Write multiple lines
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" \
    -d "${MEASUREMENT},server_id=srv002,region=us-west online_count=2800i,cpu_usage=65.3 1755676800000000000
${MEASUREMENT},server_id=srv003,region=eu-west online_count=1200i,cpu_usage=45.1 1755676800000000000
${MEASUREMENT},server_id=srv001,region=us-east online_count=3600i,cpu_usage=74.2 1755676860000000000")
assert_status "/write multiple lines" "204" "$STATUS"

# 3c. Write with different field types
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" \
    -d "${MEASUREMENT},server_id=srv004,region=ap-east online_count=500i,cpu_usage=12.3,is_active=true 1755676920000000000")
assert_status "/write with boolean field" "204" "$STATUS"

# 3d. Write without timestamp (server assigns)
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" \
    -d "${MEASUREMENT},server_id=srv005,region=ap-east online_count=100i,cpu_usage=5.0")
assert_status "/write without timestamp" "204" "$STATUS"

# 3e. Invalid Line Protocol
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" \
    -d "invalid_no_space_fields")
assert_status "/write invalid Line Protocol returns 400" "400" "$STATUS"

# 3f. Empty body
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" -d "")
assert_status "/write empty body returns 204" "204" "$STATUS"

# 3g. GET /write should fail
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$URL/write?db=testdb")
assert_status "GET /write returns 405" "405" "$STATUS"

# ============================================================
section "4. /query - SHOW queries"
# ============================================================

# Wait a moment for writes to be visible
sleep 1

# 4a. SHOW DATABASES
BODY=$(curl -s "$URL/query?q=SHOW%20DATABASES")
assert_contains "SHOW DATABASES has series" "$BODY" '"series"'
assert_contains "SHOW DATABASES returns db name" "$BODY" '"tsdb"'

# 4b. SHOW MEASUREMENTS
BODY=$(curl -s "$URL/query?q=SHOW%20MEASUREMENTS")
assert_contains "SHOW MEASUREMENTS has series" "$BODY" '"series"'
assert_contains "SHOW MEASUREMENTS returns test measurement" "$BODY" "$MEASUREMENT"

# 4c. SHOW TAG KEYS
BODY=$(curl -s "$URL/query?q=SHOW%20TAG%20KEYS%20FROM%20%22${MEASUREMENT}%22")
assert_contains "SHOW TAG KEYS has server_id" "$BODY" "server_id"
assert_contains "SHOW TAG KEYS has region" "$BODY" "region"

# 4d. SHOW TAG VALUES
BODY=$(curl -s "$URL/query?q=SHOW%20TAG%20VALUES%20FROM%20%22${MEASUREMENT}%22%20WITH%20KEY%20%3D%20%22server_id%22")
assert_contains "SHOW TAG VALUES has srv001" "$BODY" "srv001"
assert_contains "SHOW TAG VALUES has srv002" "$BODY" "srv002"

# 4e. SHOW FIELD KEYS
BODY=$(curl -s "$URL/query?q=SHOW%20FIELD%20KEYS%20FROM%20%22${MEASUREMENT}%22")
assert_contains "SHOW FIELD KEYS has online_count" "$BODY" "online_count"
assert_contains "SHOW FIELD KEYS has cpu_usage" "$BODY" "cpu_usage"

# 4f. SHOW RETENTION POLICIES
BODY=$(curl -s "$URL/query?q=SHOW%20RETENTION%20POLICIES")
assert_contains "SHOW RETENTION POLICIES has autogen" "$BODY" "autogen"

# 4g. CREATE DATABASE (no-op)
BODY=$(curl -s "$URL/query?q=CREATE%20DATABASE%20%22testdb%22")
assert_contains "CREATE DATABASE returns result" "$BODY" "results"

# ============================================================
section "5. /query - SELECT queries"
# ============================================================

# 5a. Basic SELECT with aggregation
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT mean(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY time(1h)")
assert_contains "SELECT mean() GROUP BY time returns series" "$BODY" '"series"'

# 5b. SELECT with tag GROUP BY
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT mean(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY \"server_id\"")
assert_contains "SELECT mean() GROUP BY tag returns tags" "$BODY" '"tags"'
assert_contains "SELECT mean() GROUP BY tag has server_id" "$BODY" "server_id"

# 5c. last() query
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT last(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY \"server_id\"")
assert_contains "SELECT last() returns values" "$BODY" '"values"'

# 5d. sum() query
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT sum(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z'")
assert_contains "SELECT sum() returns values" "$BODY" '"values"'

# 5e. count() query
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT count(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z'")
assert_contains "SELECT count() returns values" "$BODY" '"values"'

# 5f. max/min query
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT max(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z'")
assert_contains "SELECT max() returns values" "$BODY" '"values"'

BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT min(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z'")
assert_contains "SELECT min() returns values" "$BODY" '"values"'

# ============================================================
section "6. /query - Subqueries"
# ============================================================

# 6a. sum(last()) subquery
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT sum(\"val\") FROM (SELECT last(\"online_count\") AS val FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY \"server_id\")")
assert_contains "sum(last()) subquery returns values" "$BODY" '"values"'

# 6b. count(last()) subquery
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT count(\"val\") FROM (SELECT last(\"online_count\") AS val FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY \"server_id\")")
assert_contains "count(last()) subquery returns values" "$BODY" '"values"'

# 6c. max(sum()) with time bucket subquery
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT max(\"total\") FROM (SELECT sum(\"online_count\") AS total FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY time(1h))")
assert_contains "max(sum()) subquery returns values" "$BODY" '"values"'

# 6d. Subquery with outer GROUP BY tag
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT sum(\"val\") FROM (SELECT last(\"online_count\") AS val FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY \"server_id\",\"region\") GROUP BY \"region\"")
assert_contains "subquery with outer GROUP BY returns values" "$BODY" '"values"'

# ============================================================
section "7. /query - Edge cases"
# ============================================================

# 7a. Missing q parameter
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$URL/query")
assert_status "/query without q returns 400" "400" "$STATUS"

# 7b. POST /query
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/query" -d "q=SHOW+DATABASES")
assert_status "POST /query works" "200" "$STATUS"

# 7c. now() time comparison
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT last(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > now() - 1000000h")
assert_contains "now() time comparison works" "$BODY" '"values"'

# 7d. fill(null)
BODY=$(curl -s -G "$URL/query" --data-urlencode "q=SELECT mean(\"online_count\") FROM \"${MEASUREMENT}\" WHERE time > '2025-08-20T00:00:00Z' GROUP BY time(1h) fill(null)")
assert_contains "fill(null) does not error" "$BODY" "results"

# ============================================================
section "8. Response format validation"
# ============================================================

# Validate JSON structure
BODY=$(curl -s "$URL/query?q=SHOW%20DATABASES")
VALID=$(python3 -c "
import sys, json
try:
    d = json.loads('''$BODY''')
    assert 'results' in d
    assert isinstance(d['results'], list)
    r = d['results'][0]
    assert 'statement_id' in r
    assert 'series' in r
    s = r['series'][0]
    assert 'name' in s
    assert 'columns' in s
    assert 'values' in s
    print('VALID')
except Exception as e:
    print(f'INVALID: {e}')
" 2>/dev/null)
if [ "$VALID" = "VALID" ]; then
    pass "InfluxDB JSON response structure valid"
else
    fail "InfluxDB JSON response structure: $VALID"
fi

# ============================================================
# Cleanup test measurement
# ============================================================
section "9. Cleanup"

BODY=$(curl -s "$URL/query?q=DROP%20MEASUREMENT%20%22${MEASUREMENT}%22")
assert_contains "DROP MEASUREMENT accepted" "$BODY" "results"

# ============================================================
# Summary
# ============================================================
echo ""
echo "================================"
TOTAL=$((PASS+FAIL+SKIP))
echo -e "Total: $TOTAL | ${GREEN}Passed: $PASS${NC} | ${RED}Failed: $FAIL${NC} | ${YELLOW}Skipped: $SKIP${NC}"
echo "================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
