#!/bin/bash
# InfluxDB 1.x API Test Suite for influx2tsdb-proxy
# Usage: ./test/test_api.sh [HOST:PORT]
#
# Test data is loaded from:
#   test/data/writes.txt  - Line Protocol write cases
#   test/data/queries.txt - InfluxQL query cases

set -e

BASE="${1:-localhost:8087}"
URL="http://$BASE"
PASS=0
FAIL=0
SKIP=0

# Resolve data directory relative to this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test measurement name (unique to avoid conflicts)
TS=$(date +%s)
MEASUREMENT="test_api_${TS}"
TIME_GT="2025-08-20T00:00:00Z"

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

# Expand ${MEASUREMENT} and ${TIME_GT} placeholders in a string
expand_vars() {
    local s="$1"
    s="${s//\$\{MEASUREMENT\}/$MEASUREMENT}"
    s="${s//\$\{TIME_GT\}/$TIME_GT}"
    echo "$s"
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

if [ ! -f "$DATA_DIR/writes.txt" ]; then
    fail "writes.txt not found: $DATA_DIR/writes.txt"
else
    while IFS='|' read -r desc body expected_status; do
        # Skip comments and empty lines
        [[ "$desc" =~ ^[[:space:]]*# ]] && continue
        [[ -z "$desc" ]] && continue

        # Expand variables
        body=$(expand_vars "$body")
        expected_status=$(echo "$expected_status" | tr -d '[:space:]')

        # Special cases
        if [ "$desc" = "empty_body" ]; then
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" -d "")
        elif [ "$desc" = "get_write_method" ]; then
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$URL/write?db=testdb")
        else
            # Handle \n as actual newlines for multi-line writes
            body=$(echo -e "$body")
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/write?db=testdb" -d "$body")
        fi

        assert_status "/write $desc" "$expected_status" "$STATUS"
    done < "$DATA_DIR/writes.txt"
fi

# ============================================================
section "4. /query - from queries.txt"
# ============================================================

# Wait a moment for writes to be visible
sleep 1

if [ ! -f "$DATA_DIR/queries.txt" ]; then
    fail "queries.txt not found: $DATA_DIR/queries.txt"
else
    while IFS='|' read -r desc query pattern assert_type; do
        # Skip comments and empty lines
        [[ "$desc" =~ ^[[:space:]]*# ]] && continue
        [[ -z "$desc" ]] && continue

        # Expand variables
        query=$(expand_vars "$query")
        pattern=$(expand_vars "$pattern")
        assert_type=$(echo "$assert_type" | tr -d '[:space:]')

        if [ "$assert_type" = "status" ]; then
            # pattern is expected HTTP status code
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" -G "$URL/query" --data-urlencode "q=$query")
            assert_status "$desc" "$pattern" "$STATUS"
        else
            # default: contains
            BODY=$(curl -s -G "$URL/query" --data-urlencode "q=$query")
            assert_contains "$desc" "$BODY" "$pattern"
        fi
    done < "$DATA_DIR/queries.txt"
fi

# ============================================================
section "5. /query - Edge cases"
# ============================================================

# Missing q parameter
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$URL/query")
assert_status "/query without q returns 400" "400" "$STATUS"

# POST /query
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/query" -d "q=SHOW+DATABASES")
assert_status "POST /query works" "200" "$STATUS"

# ============================================================
section "6. Response format validation"
# ============================================================

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
section "7. Cleanup"

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
