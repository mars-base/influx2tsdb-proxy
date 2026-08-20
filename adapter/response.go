package adapter

import "time"

type InfluxDBResponse struct {
	Results []InfluxDBResult `json:"results"`
}

type InfluxDBResult struct {
	StatementID int              `json:"statement_id"`
	Series      []InfluxDBSeries `json:"series,omitempty"`
	Error       string           `json:"error,omitempty"`
}

type InfluxDBSeries struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
}

// normalizeValue ensures numeric values are float64 for InfluxDB JSON compatibility.
// InfluxDB always returns numbers as float64; Go int64 serializes as JSON integer
// which Grafana's InfluxDB plugin may misinterpret as string.
func normalizeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float32:
		return float64(val)
	default:
		return v
	}
}

// normalizeRow normalizes all values in a row and optionally prepends a time column
func normalizeRow(row []interface{}, insertTime bool) []interface{} {
	result := make([]interface{}, 0, len(row)+1)
	if insertTime {
		result = append(result, time.Now().UTC().Format(time.RFC3339Nano))
	}
	for _, v := range row {
		result = append(result, normalizeValue(v))
	}
	return result
}
