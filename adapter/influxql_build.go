package adapter

import (
	"fmt"
	"regexp"
	"strings"
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

// buildLastFirstLateralSQL generates optimized SQL for last()/first() using LATERAL JOIN.
// Instead of DISTINCT ON (full sort), each tag group gets an independent LIMIT 1 index lookup.
//
// Generated SQL:
//
//	SELECT tags."server_id", t.time, t.online_count AS "val"
//	FROM (SELECT DISTINCT "server_id" FROM "server_online" WHERE time > ...) AS tags
//	CROSS JOIN LATERAL (
//	  SELECT time, online_count AS "val"
//	  FROM "server_online"
//	  WHERE time > ... AND "server_id" = tags."server_id"
//	  ORDER BY time DESC LIMIT 1
//	) AS t
func (c *Converter) buildLastFirstLateralSQL(q selectQuery) string {
	var sql strings.Builder
	fullTable := c.qualifiedTable(q.measurement)
	where := c.convertWhere(q.where)

	// Build converted field list (last("f") AS alias → "f" AS "alias")
	convertedFields := c.convertFields(q.fields, "")

	// Build lateral SELECT: time + converted fields
	lateralSelect := fmt.Sprintf("time, %s", strings.Join(convertedFields, ", "))

	// Build tag filter and outer tag columns
	var tagFilters []string
	var tagCols []string
	for _, tag := range q.groupByTags {
		tagFilters = append(tagFilters, fmt.Sprintf(`"%s" = tags."%s"`, tag, tag))
		tagCols = append(tagCols, fmt.Sprintf(`tags."%s"`, tag))
	}

	// ORDER direction
	orderDir := "DESC"
	if q.hasFirst {
		orderDir = "ASC"
	}

	// Outer SELECT: tag columns + lateral columns
	sql.WriteString("SELECT ")
	sql.WriteString(strings.Join(tagCols, ", "))
	sql.WriteString(", ")
	sql.WriteString(fmt.Sprintf("t.time, %s", joinLateralFields(convertedFields)))

	// FROM: distinct tags subquery
	sql.WriteString(" FROM (")
	sql.WriteString(fmt.Sprintf(`SELECT DISTINCT %s FROM %s`,
		joinQuotedTags(q.groupByTags), fullTable))
	if where != "" {
		sql.WriteString(fmt.Sprintf(" WHERE %s", where))
	}
	sql.WriteString(") AS tags")

	// CROSS JOIN LATERAL
	sql.WriteString(" CROSS JOIN LATERAL (")
	sql.WriteString(fmt.Sprintf(`SELECT %s FROM %s`, lateralSelect, fullTable))
	if where != "" {
		sql.WriteString(fmt.Sprintf(" WHERE %s AND %s", where, strings.Join(tagFilters, " AND ")))
	} else {
		sql.WriteString(fmt.Sprintf(" WHERE %s", strings.Join(tagFilters, " AND ")))
	}
	sql.WriteString(fmt.Sprintf(" ORDER BY time %s LIMIT 1", orderDir))
	sql.WriteString(") AS t")

	return sql.String()
}

// joinQuotedTags joins tag names with quotes: "tag1", "tag2"
func joinQuotedTags(tags []string) string {
	quoted := make([]string, len(tags))
	for i, t := range tags {
		quoted[i] = fmt.Sprintf(`"%s"`, t)
	}
	return strings.Join(quoted, ", ")
}

// joinLateralFields prefixes converted fields with "t." for the outer SELECT
func joinLateralFields(fields []string) string {
	result := make([]string, len(fields))
	for i, f := range fields {
		// f is like `"online_count" AS "val"` or `"online_count"`
		// prefix with t.: `t."online_count" AS "val"` or `t."online_count"`
		result[i] = "t." + f
	}
	return strings.Join(result, ", ")
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

	// FROM clause with schema-qualified table
	sql.WriteString(fmt.Sprintf(` FROM %s`, c.qualifiedTable(q.measurement)))

	// WHERE clause
	where := c.convertWhere(q.where)
	if where != "" {
		sql.WriteString(fmt.Sprintf(" WHERE %s", where))
	}

	// ORDER BY / GROUP BY logic
	if q.groupByTime != "" {
		// Group by time + optional tags (last/first are converted to TimescaleDB aggregates)
		groupCols := []string{fmt.Sprintf("time_bucket('%s', time)", q.groupByTime)}
		for _, tag := range q.groupByTags {
			groupCols = append(groupCols, fmt.Sprintf(`"%s"`, tag))
		}
		sql.WriteString(fmt.Sprintf(" GROUP BY %s", strings.Join(groupCols, ", ")))
		sql.WriteString(" ORDER BY time")
	} else if q.hasLast {
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

	// When inner has GROUP BY time but outer doesn't aggregate, pass through time column
	if inner.groupByTime != "" && outer.groupByTime == "" && !c.hasAggregation(outer.fields) {
		sql.WriteString("time, ")
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

	// For last/first
	if reLast.MatchString(field) || reFirst.MatchString(field) {
		re := regexp.MustCompile(`(?i)(last|first)\s*\(\s*"?(\w+)"?\s*\)(?:\s+AS\s+(.+))?`)
		if m := re.FindStringSubmatch(field); len(m) > 1 {
			funcName := m[1] // "last" or "first"
			col := m[2]      // field name
			alias := strings.TrimSpace(m[3])

			if groupByTime != "" {
				// With GROUP BY time, use TimescaleDB's last(val, time) / first(val, time)
				sqlFunc := fmt.Sprintf(`%s("%s", time)`, funcName, col)
				if alias != "" {
					return fmt.Sprintf(`%s AS %s`, sqlFunc, alias)
				}
				return sqlFunc
			}

			// Without GROUP BY time, DISTINCT ON handles it
			if alias != "" {
				return fmt.Sprintf(`"%s" AS %s`, col, alias)
			}
			return fmt.Sprintf(`"%s"`, col)
		}
	}

	// If there's a GROUP BY time but no aggregation function, wrap in last()
	// InfluxDB allows raw fields with GROUP BY time, but PostgreSQL requires aggregation
	// Check against f (after mean→avg replacement) to avoid false negatives
	if groupByTime != "" {
		reAgg := regexp.MustCompile(`(?i)\b(avg|sum|count|max|min|last|first|median|stddev|spread)\s*\(`)
		if !reAgg.MatchString(f) {
			return fmt.Sprintf("last(%s, time)", f)
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
