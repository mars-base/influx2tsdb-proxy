package adapter

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

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

	// Replace epoch millisecond literals: 1787221389201ms -> timestamp
	// Grafana sends time comparisons like: time > 1787221389201ms
	reEpochMs := regexp.MustCompile(`(\d+)ms`)
	w = reEpochMs.ReplaceAllStringFunc(w, func(match string) string {
		m := reEpochMs.FindStringSubmatch(match)
		if len(m) < 2 {
			return match
		}
		var ms int64
		fmt.Sscanf(m[1], "%d", &ms)
		return fmt.Sprintf("'%s'", time.UnixMilli(ms).UTC().Format(time.RFC3339))
	})

	// Remove InfluxDB type annotations: "region"::tag -> "region", region::tag -> region
	reTypeAnnot := regexp.MustCompile(`("[\w]+"|\w+)::(?:tag|field)\b`)
	w = reTypeAnnot.ReplaceAllString(w, "$1")

	// Replace: time > now() - Xs -> time > NOW() - interval 'X seconds'
	w = replaceTimeComparisons(w)

	// Remove quotes around identifiers
	w = strings.ReplaceAll(w, `"`, "")

	return w
}

func (c *Converter) executeAndFormat(sql string, measurement string, groupByTags []string, fieldAliases []string) (*InfluxDBResponse, error) {
	rows, err := c.meta.pool.Query(c.meta.ctx, sql)
	if err != nil {
		errMsg := err.Error()
		// Return empty result for "column does not exist" (Grafana placeholder fields like "value")
		// and "relation does not exist" (querying before data is written)
		if strings.Contains(errMsg, "does not exist") {
			log.Printf("[QUERY] non-fatal error, returning empty result: %v", err)
			return emptyResult(), nil
		}
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

	result := buildInfluxResult(measurement, allRows, colNames, groupByTags, valueColNames, valueColAliases, c.epoch)

	return &InfluxDBResponse{
		Results: []InfluxDBResult{result},
	}, nil
}

// replaceTimeComparisons replaces time > now() - Xs patterns with PostgreSQL intervals
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
