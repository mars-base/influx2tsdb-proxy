package adapter

import (
	"fmt"
	"log"
	"regexp"
	"strings"
)

// Verbose controls whether SQL and query detail logs are printed.
// Set via -verbose CLI flag.
var Verbose bool

// Converter translates InfluxQL queries to SQL and executes them against TimescaleDB
type Converter struct {
	meta           *MetaStore
	retentionStore *RetentionStore
	dbName         string
	fromMs         int64
	toMs           int64
	epoch          string // "ms" for millisecond timestamps, empty for RFC3339
}

func NewConverter(meta *MetaStore, retentionStore *RetentionStore, dbName string, fromMs, toMs int64, epoch string) *Converter {
	return &Converter{meta: meta, retentionStore: retentionStore, dbName: dbName, fromMs: fromMs, toMs: toMs, epoch: epoch}
}

// qualifiedTable returns a schema-qualified table reference: "dbName"."measurement"
func (c *Converter) qualifiedTable(measurement string) string {
	return fmt.Sprintf(`"%s"."%s"`, escapeIdent(c.dbName), escapeIdent(measurement))
}

// Convert parses an InfluxQL query and returns an InfluxDB-compatible response
func (c *Converter) Convert(query string) (*InfluxDBResponse, error) {
	query = strings.TrimSpace(query)
	upper := strings.ToUpper(query)

	// Route to appropriate handler
	switch {
	case strings.HasPrefix(upper, "SHOW DATABASES"):
		return handleShowDatabases(c.meta), nil
	case strings.HasPrefix(upper, "SHOW MEASUREMENTS"):
		return handleShowMeasurements(c.meta, c.dbName), nil
	case strings.HasPrefix(upper, "SHOW RETENTION"):
		return c.handleShowRetentionPolicies(), nil
	case strings.HasPrefix(upper, "SHOW TAG VALUES"):
		return handleShowTagValues(c.meta, c.dbName, query), nil
	case strings.HasPrefix(upper, "SHOW TAG KEYS"):
		return handleShowTagKeys(c.meta, c.dbName, query), nil
	case strings.HasPrefix(upper, "SHOW FIELD KEYS"):
		return handleShowFieldKeys(c.meta, c.dbName, query), nil
	case strings.HasPrefix(upper, "CREATE RETENTION POLICY"):
		return c.handleCreateRetentionPolicy(query), nil
	case strings.HasPrefix(upper, "ALTER RETENTION POLICY"):
		return c.handleAlterRetentionPolicy(query), nil
	case strings.HasPrefix(upper, "DROP RETENTION POLICY"):
		return c.handleDropRetentionPolicy(query), nil
	case strings.HasPrefix(upper, "DROP MEASUREMENT"):
		return c.handleDropMeasurement(query)
	case strings.HasPrefix(upper, "CREATE DATABASE"):
		return c.handleCreateDatabase(query), nil
	case strings.HasPrefix(upper, "DROP DATABASE"):
		return c.handleDropDatabase(query), nil
	case strings.HasPrefix(upper, "DELETE"):
		return c.handleDelete(query), nil
	case strings.HasPrefix(upper, "SELECT"):
		return c.handleSelect(query)
	default:
		return &InfluxDBResponse{
			Results: []InfluxDBResult{{Error: fmt.Sprintf("unsupported query: %.100s", query)}},
		}, nil
	}
}

// handleSelect processes SELECT queries
func (c *Converter) handleSelect(query string) (*InfluxDBResponse, error) {
	// Check if this is a subquery (FROM contains a SELECT)
	upper := strings.ToUpper(query)

	if containsSubquery(upper) {
		return c.handleSubquery(query)
	}

	return c.handleSimpleSelect(query)
}

// containsSubquery checks if the query has a subquery in FROM clause
func containsSubquery(upper string) bool {
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return false
	}
	afterFrom := strings.TrimSpace(upper[fromIdx+4:])
	return strings.HasPrefix(afterFrom, "(")
}

// handleSimpleSelect handles queries like:
// SELECT mean("online_count") FROM "server_online" WHERE ... GROUP BY time(10s), "server_id" fill(null)
// SELECT last("online_count") FROM "server_online" WHERE time > now() - 3s GROUP BY "server_id"
func (c *Converter) handleSimpleSelect(query string) (*InfluxDBResponse, error) {
	q := c.parseSelectQuery(query)

	// Handle empty queries (Grafana sends "SELECT FROM ..." while editing)
	// Return empty result like native InfluxDB does
	if len(q.fields) == 0 || q.measurement == "" {
		return emptyResult(), nil
	}

	var sql string
	if (q.hasLast || q.hasFirst) && len(q.groupByTags) > 0 && q.groupByTime == "" {
		// UNION ALL with per-tag index lookup (avoids full sort)
		sql = c.buildLastFirstLateralSQL(q)
	} else {
		sql = c.buildSimpleSQL(q)
	}

	if Verbose {
		log.Printf("[SQL] %s", sql)
	}

	return c.executeAndFormat(sql, q.measurement, q.groupByTags, q.fieldAliases)
}

// handleSubquery handles queries like:
// SELECT sum("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE ... GROUP BY "server_id")
// SELECT max("total") FROM (SELECT sum("online_count") AS total FROM "server_online" GROUP BY time(5s))
func (c *Converter) handleSubquery(query string) (*InfluxDBResponse, error) {
	outer, inner, err := c.splitSubquery(query)
	if err != nil {
		return nil, err
	}

	// Build inner SQL first
	innerQ := c.parseSelectQuery(inner)
	innerSQL := c.buildSimpleSQL(innerQ)

	// Build outer SQL wrapping the inner as a subquery
	outerQ := c.parseSelectQuery(outer)
	outerSQL := c.buildOuterSQL(outerQ, innerSQL, innerQ)

	if Verbose {
		log.Printf("[SQL] %s", outerSQL)
	}

	// Use inner measurement name (outer query uses alias "t" which is not the real measurement)
	measurement := innerQ.measurement

	return c.executeAndFormat(outerSQL, measurement, outerQ.groupByTags, outerQ.fieldAliases)
}

type selectQuery struct {
	fields       []string // raw field expressions
	measurement  string
	where        string
	groupByTags  []string // tag names for GROUP BY (non-time)
	groupByTime  string   // time interval e.g. "10s"
	fill         string   // fill strategy
	hasLast      bool     // whether the query uses last()
	hasFirst     bool     // whether the query uses first()
	fieldAliases []string // column aliases for output
	limit        int
}

// Regex patterns for parsing
var (
	reSelect    = regexp.MustCompile(`(?i)^SELECT\s+(.+?)\s+FROM\s+`)
	reFrom      = regexp.MustCompile(`(?i)\bFROM\s+("[^"]+"|[\w]+)`)
	reWhere     = regexp.MustCompile(`(?i)\bWHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)`)
	reGroupBy   = regexp.MustCompile(`(?i)\bGROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+fill\s*\(|\s*$)`)
	reFill      = regexp.MustCompile(`(?i)\bfill\s*\(\s*(\w+)\s*\)`)
	reLimit     = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	reTimeGroup = regexp.MustCompile(`(?i)time\s*\(\s*([\w.]+)\s*\)`)
	reLast      = regexp.MustCompile(`(?i)\blast\s*\(`)
	reFirst     = regexp.MustCompile(`(?i)\bfirst\s*\(`)
)

// splitSubquery splits an outer+inner query
// Input: SELECT sum("val") FROM (SELECT last("online_count") AS val FROM "server_online" WHERE ... GROUP BY "server_id")
func (c *Converter) splitSubquery(query string) (outer, inner string, err error) {
	upper := strings.ToUpper(query)
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return "", "", fmt.Errorf("no FROM clause")
	}

	outerPart := query[:fromIdx+4] // "SELECT ... FROM"
	afterFrom := strings.TrimSpace(query[fromIdx+4:])

	// Find the matching closing parenthesis
	if !strings.HasPrefix(afterFrom, "(") {
		return "", "", fmt.Errorf("expected subquery after FROM")
	}

	depth := 0
	endIdx := -1
	for i, ch := range afterFrom {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}

	if endIdx < 0 {
		return "", "", fmt.Errorf("unmatched parenthesis in subquery")
	}

	inner = afterFrom[1:endIdx] // content between ( and )

	// The outer query continues after the closing )
	outerSuffix := ""
	if endIdx+1 < len(afterFrom) {
		outerSuffix = strings.TrimSpace(afterFrom[endIdx+1:])
	}

	// Build outer query: outerPart + "t" + suffix
	// We'll add the FROM subquery back in buildOuterSQL
	outer = outerPart + " t"
	if outerSuffix != "" {
		outer += " " + outerSuffix
	}

	return outer, inner, nil
}
