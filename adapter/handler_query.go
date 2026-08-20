package adapter

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func HandleQuery(dbName string, meta *MetaStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		queryStr := r.FormValue("q")
		_ = r.FormValue("db")
		epoch := r.FormValue("epoch")

		// Parse time range from from/to parameters (milliseconds)
		var fromMs, toMs int64
		if v := r.FormValue("from"); v != "" {
			fromMs, _ = strconv.ParseInt(v, 10, 64)
		}
		if v := r.FormValue("to"); v != "" {
			toMs, _ = strconv.ParseInt(v, 10, 64)
		}
		// Default: last 1 hour if not specified
		if fromMs == 0 && toMs == 0 {
			toMs = time.Now().UnixMilli()
			fromMs = toMs - 3600*1000
		}

		if queryStr == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(InfluxDBResponse{
				Results: []InfluxDBResult{{Error: `missing required parameter "q"`}},
			})
			return
		}

		log.Printf("[QUERY] q=%s epoch=%s from=%d to=%d", queryStr, epoch, fromMs, toMs)

		conv := NewConverter(meta, dbName, fromMs, toMs, epoch)
		result, err := conv.Convert(queryStr)
		if err != nil {
			log.Printf("[QUERY ERROR] %v", err)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(InfluxDBResponse{
				Results: []InfluxDBResult{{Error: err.Error()}},
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

func emptyResult() *InfluxDBResponse {
	return &InfluxDBResponse{
		Results: []InfluxDBResult{{StatementID: 0}},
	}
}

// handleShowDatabases returns available databases
func handleShowDatabases(dbName string) *InfluxDBResponse {
	return &InfluxDBResponse{
		Results: []InfluxDBResult{{
			Series: []InfluxDBSeries{{
				Name:    "databases",
				Columns: []string{"name"},
				Values:  [][]interface{}{{dbName}},
			}},
		}},
	}
}

// handleShowMeasurements returns all measurement names
func handleShowMeasurements(meta *MetaStore) *InfluxDBResponse {
	measurements, err := meta.GetMeasurements()
	if err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}
	values := make([][]interface{}, len(measurements))
	for i, m := range measurements {
		values[i] = []interface{}{m}
	}
	return &InfluxDBResponse{
		Results: []InfluxDBResult{{
			Series: []InfluxDBSeries{{
				Name:    "measurements",
				Columns: []string{"name"},
				Values:  values,
			}},
		}},
	}
}

// handleShowTagValues returns distinct tag values
func handleShowTagValues(meta *MetaStore, query string) *InfluxDBResponse {
	upper := strings.ToUpper(query)

	// Extract measurement from FROM clause
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing FROM clause"}}}
	}
	afterFrom := strings.TrimSpace(query[fromIdx+4:])
	measurement := extractFirstToken(afterFrom)

	// Extract key from WITH KEY = clause
	keyIdx := strings.Index(upper, "WITH KEY")
	if keyIdx < 0 {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing WITH KEY clause"}}}
	}
	afterKey := strings.TrimSpace(query[keyIdx+8:])
	afterKey = strings.TrimLeft(afterKey, "= ")
	tagKey := extractFirstToken(afterKey)

	values, err := meta.GetTagValues(measurement, tagKey)
	if err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}

	result := make([][]interface{}, len(values))
	for i, v := range values {
		result[i] = []interface{}{tagKey, v}
	}

	return &InfluxDBResponse{
		Results: []InfluxDBResult{{
			Series: []InfluxDBSeries{{
				Name:    measurement,
				Columns: []string{"key", "value"},
				Values:  result,
			}},
		}},
	}
}

// handleShowFieldKeys returns field info for a measurement
func handleShowFieldKeys(meta *MetaStore, query string) *InfluxDBResponse {
	upper := strings.ToUpper(query)
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing FROM clause"}}}
	}
	afterFrom := strings.TrimSpace(query[fromIdx+4:])
	measurement := extractFirstToken(afterFrom)

	fields, err := meta.GetFields(measurement)
	if err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}

	values := make([][]interface{}, len(fields))
	for i, f := range fields {
		values[i] = []interface{}{f.Name, f.Type}
	}

	return &InfluxDBResponse{
		Results: []InfluxDBResult{{
			Series: []InfluxDBSeries{{
				Name:    measurement,
				Columns: []string{"fieldKey", "fieldType"},
				Values:  values,
			}},
		}},
	}
}

// extractFirstToken extracts the first word/token, removing quotes
func extractFirstToken(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"'`)
	// Find end of token
	for i, c := range s {
		if c == ' ' || c == ',' || c == ')' {
			return strings.Trim(s[:i], `"'`)
		}
	}
	return strings.Trim(s, `"'`)
}

// extractMeasurement extracts measurement name from a query string
func extractMeasurement(query string) string {
	upper := strings.ToUpper(query)
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return ""
	}
	afterFrom := strings.TrimSpace(query[fromIdx+4:])
	return extractFirstToken(afterFrom)
}

// buildInfluxResult converts SQL rows into InfluxDB response format,
// grouping by tag columns into separate series.
// epoch controls time format: "ms" returns epoch milliseconds, "" returns RFC3339.
func buildInfluxResult(measurement string, rows []map[string]interface{}, colNames []string, tagCols []string, valueColNames []string, valueColAliases []string, epoch string) InfluxDBResult {
	if len(rows) == 0 {
		return InfluxDBResult{StatementID: 0}
	}

	// Check if time column exists and has valid values
	hasTime := false
	if _, ok := rows[0]["time"]; ok {
		for _, row := range rows {
			if row["time"] != nil {
				hasTime = true
				break
			}
		}
	}

	// Fallback time when no time column: use epoch ms or RFC3339 depending on epoch param
	fallbackTime := formatTimeValue(time.Now().UTC(), epoch)

	if len(tagCols) == 0 {
		// No tag grouping: single series
		columns := []string{"time"}
		columns = append(columns, valueColAliases...)

		values := make([][]interface{}, 0, len(rows))
		for _, row := range rows {
			valRow := make([]interface{}, 0, 1+len(valueColNames))
			if hasTime {
				valRow = append(valRow, formatTimeValue(row["time"], epoch))
			} else {
				valRow = append(valRow, fallbackTime)
			}
			for _, col := range valueColNames {
				valRow = append(valRow, normalizeValue(row[col]))
			}
			values = append(values, valRow)
		}

		return InfluxDBResult{
			StatementID: 0,
			Series: []InfluxDBSeries{{
				Name:    measurement,
				Columns: columns,
				Values:  values,
			}},
		}
	}

	// Group by tag values
	seriesMap := make(map[string]*InfluxDBSeries)
	var seriesOrder []string

	for _, row := range rows {
		tagVals := make([]string, len(tagCols))
		for i, tc := range tagCols {
			tagVals[i] = fmt.Sprintf("%v", row[tc])
		}
		key := strings.Join(tagVals, "\x00")

		series, ok := seriesMap[key]
		if !ok {
			tags := make(map[string]string)
			for i, tc := range tagCols {
				tags[tc] = tagVals[i]
			}
			columns := []string{"time"}
			columns = append(columns, valueColAliases...)

			series = &InfluxDBSeries{
				Name:    measurement,
				Tags:    tags,
				Columns: columns,
			}
			seriesMap[key] = series
			seriesOrder = append(seriesOrder, key)
		}

		valRow := make([]interface{}, 0, 1+len(valueColNames))
		if hasTime {
			valRow = append(valRow, formatTimeValue(row["time"], epoch))
		} else {
			valRow = append(valRow, fallbackTime)
		}
		for _, col := range valueColNames {
			valRow = append(valRow, normalizeValue(row[col]))
		}
		series.Values = append(series.Values, valRow)
	}

	allSeries := make([]InfluxDBSeries, 0, len(seriesOrder))
	for _, key := range seriesOrder {
		allSeries = append(allSeries, *seriesMap[key])
	}

	return InfluxDBResult{
		StatementID: 0,
		Series:      allSeries,
	}
}

// formatTimeValue converts a time value to the appropriate format based on epoch parameter.
// When epoch="ms", returns epoch milliseconds as float64 (for JSON numeric serialization).
// Otherwise returns RFC3339Nano string.
func formatTimeValue(v interface{}, epoch string) interface{} {
	var t time.Time
	switch tv := v.(type) {
	case time.Time:
		t = tv
	case string:
		// Already formatted string, return as-is unless epoch ms requested
		if epoch == "ms" {
			if parsed, err := time.Parse(time.RFC3339Nano, tv); err == nil {
				return float64(parsed.UnixMilli())
			}
			return tv
		}
		return tv
	default:
		if epoch == "ms" {
			if parsed, err := time.Parse(time.RFC3339Nano, fmt.Sprintf("%v", v)); err == nil {
				return float64(parsed.UnixMilli())
			}
		}
		return fmt.Sprintf("%v", v)
	}

	if epoch == "ms" {
		return float64(t.UnixMilli())
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// formatTime converts a time value to ISO 8601 string (kept for backward compat)
func formatTime(v interface{}) string {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", v)
	}
}
