package adapter

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

// Converter translates InfluxQL queries to SQL and executes them against TimescaleDB
type Converter struct {
	meta   *MetaStore
	dbName string
	fromMs int64
	toMs   int64
}

func NewConverter(meta *MetaStore, dbName string, fromMs, toMs int64) *Converter {
	return &Converter{meta: meta, dbName: dbName, fromMs: fromMs, toMs: toMs}
}

// Convert parses an InfluxQL query and returns an InfluxDB-compatible response
func (c *Converter) Convert(query string) (*InfluxDBResponse, error) {
	query = strings.TrimSpace(query)
	upper := strings.ToUpper(query)

	// Route to appropriate handler
	switch {
	case strings.HasPrefix(upper, "SHOW DATABASES"):
		return handleShowDatabases(c.dbName), nil
	case strings.HasPrefix(upper, "SHOW MEASUREMENTS"):
		return handleShowMeasurements(c.meta), nil
	case strings.HasPrefix(upper, "SHOW RETENTION"):
		return handleShowRetentionPolicies(), nil
	case strings.HasPrefix(upper, "SHOW TAG VALUES"):
		return handleShowTagValues(c.meta, query), nil
	case strings.HasPrefix(upper, "SHOW TAG KEYS"):
		return handleShowTagKeys(c.meta, query), nil
	case strings.HasPrefix(upper, "SHOW FIELD KEYS"):
		return handleShowFieldKeys(c.meta, query), nil
	case strings.HasPrefix(upper, "CREATE DATABASE"):
		return emptyResult(), nil
	case strings.HasPrefix(upper, "DROP DATABASE"):
		return emptyResult(), nil
	case strings.HasPrefix(upper, "SELECT"):
		return c.handleSelect(query)
	default:
		return &InfluxDBResponse{
			Results: []InfluxDBResult{{Error: fmt.Sprintf("unsupported query: %.100s", query)}},
		}, nil
	}
}

func handleShowRetentionPolicies() *InfluxDBResponse {
	return &InfluxDBResponse{
		Results: []InfluxDBResult{{
			Series: []InfluxDBSeries{{
				Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"},
				Values:  [][]interface{}{{"autogen", "0s", "168h0m0s", 1, true}},
			}},
		}},
	}
}

func handleShowTagKeys(meta *MetaStore, query string) *InfluxDBResponse {
	upper := strings.ToUpper(query)
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing FROM clause"}}}
	}
	afterFrom := strings.TrimSpace(query[fromIdx+4:])
	measurement := extractFirstToken(afterFrom)

	// Get tag columns from _influx_meta
	rows, err := meta.pool.Query(meta.ctx,
		"SELECT column_name FROM _influx_meta WHERE measurement = $1 AND column_type = 'tag' ORDER BY column_name",
		measurement)
	if err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}
	defer rows.Close()

	var values [][]interface{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
		}
		values = append(values, []interface{}{name})
	}

	return &InfluxDBResponse{
		Results: []InfluxDBResult{{
			Series: []InfluxDBSeries{{
				Name:    measurement,
				Columns: []string{"tagKey"},
				Values:  values,
			}},
		}},
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

	sql := c.buildSimpleSQL(q)

	log.Printf("[SQL] %s", sql)

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

	log.Printf("[SQL] %s", outerSQL)

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

func (c *Converter) parseSelectQuery(query string) selectQuery {
	q := selectQuery{}

	// Extract SELECT fields
	if m := reSelect.FindStringSubmatch(query); len(m) > 1 {
		q.fields = splitFields(m[1])
	}

	// Extract measurement from FROM
	if m := reFrom.FindStringSubmatch(query); len(m) > 1 {
		q.measurement = strings.Trim(m[1], `"`)
	}

	// Extract WHERE clause
	if m := reWhere.FindStringSubmatch(query); len(m) > 1 {
		q.where = strings.TrimSpace(m[1])
	}

	// Extract GROUP BY
	if m := reGroupBy.FindStringSubmatch(query); len(m) > 1 {
		groupStr := m[1]
		// Extract time() interval
		if tm := reTimeGroup.FindStringSubmatch(groupStr); len(tm) > 1 {
			q.groupByTime = tm[1]
		}
		// Extract tag groupings
		parts := strings.Split(groupStr, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if strings.HasPrefix(strings.ToLower(p), "time(") {
				continue
			}
			tag := strings.Trim(p, `"`)
			if tag != "" {
				q.groupByTags = append(q.groupByTags, tag)
			}
		}
	}

	// Extract fill
	if m := reFill.FindStringSubmatch(query); len(m) > 1 {
		q.fill = m[1]
	}

	// Extract limit
	if m := reLimit.FindStringSubmatch(query); len(m) > 1 {
		fmt.Sscanf(m[1], "%d", &q.limit)
	}

	// Check for last/first
	q.hasLast = reLast.MatchString(query)
	q.hasFirst = reFirst.MatchString(query)

	// Extract field aliases
	for _, f := range q.fields {
		alias := extractAlias(f)
		q.fieldAliases = append(q.fieldAliases, alias)
	}

	return q
}

func (c *Converter) buildSimpleSQL(q selectQuery) string {
	var sql strings.Builder

	// SELECT clause
	sql.WriteString("SELECT ")
	if q.hasLast || q.hasFirst {
		// Use DISTINCT ON for last/first
		distinctCols := strings.Join(q.groupByTags, ", ")
		if distinctCols == "" {
			distinctCols = "" // no group by, just take the latest row
		}
		if distinctCols != "" {
			sql.WriteString(fmt.Sprintf("DISTINCT ON (%s) ", distinctCols))
		}
	}

	// Build field list
	selectFields := c.convertFields(q.fields, q.groupByTime)

	if q.groupByTime != "" {
		// Add time_bucket as first column
		sql.WriteString(fmt.Sprintf("time_bucket('%s', time) AS time, ", q.groupByTime))
	} else if q.hasLast || q.hasFirst {
		// For last/first, include time column
		sql.WriteString("time, ")
	} else if c.hasAggregation(q.fields) {
		// Pure aggregation (with or without tag GROUP BY): skip time column
	} else {
		sql.WriteString("time, ")
	}

	// Add tag columns for GROUP BY
	for _, tag := range q.groupByTags {
		selectFields = append(selectFields, fmt.Sprintf(`"%s"`, tag))
	}

	sql.WriteString(strings.Join(selectFields, ", "))

	// FROM clause
	sql.WriteString(fmt.Sprintf(` FROM "%s"`, q.measurement))

	// WHERE clause
	where := c.convertWhere(q.where)
	if where != "" {
		sql.WriteString(fmt.Sprintf(" WHERE %s", where))
	}

	// ORDER BY for last/first
	if q.hasLast {
		orderCols := make([]string, 0)
		for _, tag := range q.groupByTags {
			orderCols = append(orderCols, fmt.Sprintf(`"%s"`, tag))
		}
		orderCols = append(orderCols, "time DESC")
		sql.WriteString(fmt.Sprintf(" ORDER BY %s", strings.Join(orderCols, ", ")))
	} else if q.hasFirst {
		orderCols := make([]string, 0)
		for _, tag := range q.groupByTags {
			orderCols = append(orderCols, fmt.Sprintf(`"%s"`, tag))
		}
		orderCols = append(orderCols, "time ASC")
		sql.WriteString(fmt.Sprintf(" ORDER BY %s", strings.Join(orderCols, ", ")))
	} else if q.groupByTime != "" {
		// Group by time + optional tags
		groupCols := []string{fmt.Sprintf("time_bucket('%s', time)", q.groupByTime)}
		for _, tag := range q.groupByTags {
			groupCols = append(groupCols, fmt.Sprintf(`"%s"`, tag))
		}
		sql.WriteString(fmt.Sprintf(" GROUP BY %s", strings.Join(groupCols, ", ")))
		sql.WriteString(" ORDER BY time")
	} else if len(q.groupByTags) > 0 && c.hasAggregation(q.fields) {
		// Aggregation with tag GROUP BY but no time bucket
		tagCols := make([]string, len(q.groupByTags))
		for i, tag := range q.groupByTags {
			tagCols[i] = fmt.Sprintf(`"%s"`, tag)
		}
		sql.WriteString(fmt.Sprintf(" GROUP BY %s", strings.Join(tagCols, ", ")))
	}

	// LIMIT
	if q.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
	}

	return sql.String()
}

func (c *Converter) buildOuterSQL(outer selectQuery, innerSQL string, inner selectQuery) string {
	var sql strings.Builder

	sql.WriteString("SELECT ")

	// Add outer group by tag columns
	for _, tag := range outer.groupByTags {
		sql.WriteString(fmt.Sprintf(`"%s", `, tag))
	}

	// Convert outer fields
	outerFields := c.convertOuterFields(outer.fields, inner)
	sql.WriteString(strings.Join(outerFields, ", "))

	// FROM subquery
	sql.WriteString(fmt.Sprintf(" FROM (%s) t", innerSQL))

	// Outer GROUP BY
	if len(outer.groupByTags) > 0 {
		tagCols := make([]string, len(outer.groupByTags))
		for i, t := range outer.groupByTags {
			tagCols[i] = fmt.Sprintf(`"%s"`, t)
		}
		sql.WriteString(fmt.Sprintf(" GROUP BY %s", strings.Join(tagCols, ", ")))
	}

	if outer.groupByTime != "" {
		sql.WriteString(fmt.Sprintf(" ORDER BY time_bucket('%s', time)", outer.groupByTime))
	}

	return sql.String()
}

func (c *Converter) convertFields(fields []string, groupByTime string) []string {
	var result []string
	for _, f := range fields {
		converted := c.convertField(f, groupByTime)
		result = append(result, converted)
	}
	return result
}

// hasAggregation checks if any field contains an aggregation function
func (c *Converter) hasAggregation(fields []string) bool {
	reAgg := regexp.MustCompile(`(?i)\b(mean|avg|sum|count|max|min|last|first|median|stddev|spread)\s*\(`)
	for _, f := range fields {
		if reAgg.MatchString(f) {
			return true
		}
	}
	return false
}

func (c *Converter) convertField(field string, groupByTime string) string {
	f := field
	// Replace mean() with avg()
	f = regexp.MustCompile(`(?i)\bmean\s*\(`).ReplaceAllString(f, "avg(")
	// Remove quotes around identifiers
	f = strings.ReplaceAll(f, `"`, "")

	// For last/first, just return the field name (DISTINCT ON handles it)
	if reLast.MatchString(field) || reFirst.MatchString(field) {
		// Extract: last("field") AS alias -> field AS alias
		re := regexp.MustCompile(`(?i)(?:last|first)\s*\(\s*"?(\w+)"?\s*\)(?:\s+AS\s+(\w+))?`)
		if m := re.FindStringSubmatch(field); len(m) > 1 {
			col := m[1]
			alias := m[2]
			if alias != "" {
				return fmt.Sprintf(`"%s" AS "%s"`, col, alias)
			}
			return fmt.Sprintf(`"%s"`, col)
		}
	}

	// If there's a GROUP BY time, the field is likely used with an aggregation
	// avg, sum, max, min, count are already handled
	return f
}

func (c *Converter) convertOuterFields(fields []string, inner selectQuery) []string {
	var result []string
	for _, f := range fields {
		converted := f
		// Replace mean() with avg()
		converted = regexp.MustCompile(`(?i)\bmean\s*\(`).ReplaceAllString(converted, "avg(")
		// Remove quotes
		converted = strings.ReplaceAll(converted, `"`, "")

		// If the outer field references an inner alias, use it directly
		// e.g. sum("val") -> sum(val)
		result = append(result, converted)
	}
	return result
}

func (c *Converter) convertWhere(where string) string {
	if where == "" {
		return ""
	}

	w := where

	// Replace $timeFilter with actual time range
	if strings.Contains(w, "$timeFilter") {
		from := time.UnixMilli(c.fromMs).UTC().Format(time.RFC3339)
		to := time.UnixMilli(c.toMs).UTC().Format(time.RFC3339)
		timeFilter := fmt.Sprintf("time >= '%s' AND time <= '%s'", from, to)
		w = strings.ReplaceAll(w, "$timeFilter", timeFilter)
	}

	// Replace: time > now() - Xs -> time > NOW() - interval 'X seconds'
	w = replaceTimeComparisons(w)

	// Remove quotes around identifiers
	w = strings.ReplaceAll(w, `"`, "")

	return w
}

func (c *Converter) executeAndFormat(sql string, measurement string, groupByTags []string, fieldAliases []string) (*InfluxDBResponse, error) {
	rows, err := c.meta.pool.Query(c.meta.ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("SQL error: %w (sql: %s)", err, sql)
	}
	defer rows.Close()

	// Get column info
	descs := rows.FieldDescriptions()
	colNames := make([]string, len(descs))
	for i, d := range descs {
		colNames[i] = string(d.Name)
	}

	// Read all rows
	var allRows []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i, v := range values {
			row[colNames[i]] = v
		}
		allRows = append(allRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Determine value columns (non-tag, non-time columns)
	tagSet := make(map[string]bool)
	for _, t := range groupByTags {
		tagSet[t] = true
	}

	var valueColNames []string
	var valueColAliases []string
	for _, col := range colNames {
		if col == "time" || tagSet[col] {
			continue
		}
		valueColNames = append(valueColNames, col)
	}

	// Use field aliases if available, otherwise use column names
	if len(fieldAliases) > 0 && len(valueColNames) > 0 {
		// Try to match aliases to value columns
		for _, alias := range fieldAliases {
			if alias != "" && alias != "time" {
				valueColAliases = append(valueColAliases, alias)
			}
		}
	}
	if len(valueColAliases) == 0 {
		valueColAliases = valueColNames
	}

	result := buildInfluxResult(measurement, allRows, colNames, groupByTags, valueColNames, valueColAliases)

	return &InfluxDBResponse{
		Results: []InfluxDBResult{result},
	}, nil
}

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

// Helper: replace time > now() - Xs patterns
func replaceTimeComparisons(where string) string {
	// Pattern: now() - Xs/Xm/Xh/Xd
	re := regexp.MustCompile(`(?i)now\(\)\s*-\s*(\d+)([smhd])`)
	result := re.ReplaceAllStringFunc(where, func(match string) string {
		m := re.FindStringSubmatch(match)
		if len(m) < 3 {
			return match
		}
		num := m[1]
		unit := m[2]
		var interval string
		switch unit {
		case "s":
			interval = "seconds"
		case "m":
			interval = "minutes"
		case "h":
			interval = "hours"
		case "d":
			interval = "days"
		}
		return fmt.Sprintf("NOW() - interval '%s %s'", num, interval)
	})
	return result
}

// splitFields splits SELECT field list by commas, respecting parentheses
func splitFields(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for _, ch := range s {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
		if ch == ',' && depth == 0 {
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteRune(ch)
	}

	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}

	return parts
}

// extractAlias extracts the alias from a field expression
// e.g. "mean(\"online_count\")" -> "mean"
// e.g. "last(\"online_count\") AS val" -> "val"
// e.g. "\"online_count\"" -> "online_count"
func extractAlias(field string) string {
	// Check for AS alias
	upper := strings.ToUpper(field)
	asIdx := strings.LastIndex(upper, " AS ")
	if asIdx >= 0 {
		return strings.Trim(strings.TrimSpace(field[asIdx+4:]), `"`)
	}

	// Check for function call: func("field") -> func
	re := regexp.MustCompile(`(?i)(\w+)\s*\(`)
	if m := re.FindStringSubmatch(field); len(m) > 1 {
		return m[1]
	}

	// Plain column: "field" or field
	return strings.Trim(field, `"`)
}

