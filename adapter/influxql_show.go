package adapter

import (
	"strings"
	"time"
)

// handleShowRetentionPolicies returns retention policies from _retention_policy table
func (c *Converter) handleShowRetentionPolicies() *InfluxDBResponse {
	if c.retentionStore == nil {
		return &InfluxDBResponse{
			Results: []InfluxDBResult{{
				Series: []InfluxDBSeries{{
					Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"},
					Values:  [][]interface{}{{"autogen", "0s", "168h0m0s", 1, true}},
				}},
			}},
		}
	}

	policies := c.retentionStore.ShowPolicies(c.dbName)

	values := make([][]interface{}, 0, len(policies))
	for _, rp := range policies {
		// Normalize duration to Go format (e.g., 7d → 168h0m0s)
		dur := time.Duration(rp.DurationNs).String()
		// Use stored shard duration if set, otherwise auto-calculate
		var sgd string
		if rp.ShardDurationNs > 0 {
			sgd = time.Duration(rp.ShardDurationNs).String()
		} else {
			sgd = shardGroupDuration(rp.DurationNs)
		}
		values = append(values, []interface{}{rp.Name, dur, sgd, 1, rp.IsDefault})
	}

	return &InfluxDBResponse{
		Results: []InfluxDBResult{{
			Series: []InfluxDBSeries{{
				Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"},
				Values:  values,
			}},
		}},
	}
}

func handleShowTagKeys(meta *MetaStore, dbName, query string) *InfluxDBResponse {
	upper := strings.ToUpper(query)
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing FROM clause"}}}
	}
	afterFrom := strings.TrimSpace(query[fromIdx+4:])
	measurement := extractFirstToken(afterFrom)

	// Get tag columns from _influx_meta
	rows, err := meta.pool.Query(meta.ctx,
		"SELECT column_name FROM _influx_meta WHERE db_name = $1 AND measurement = $2 AND column_type = 'tag' ORDER BY column_name",
		dbName, measurement)
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

// shardGroupDuration returns the InfluxDB-style shard group duration
// based on retention duration (matches InfluxDB's internal algorithm).
func shardGroupDuration(durationNs int64) string {
	hours := durationNs / int64(time.Hour)
	switch {
	case hours == 0: // infinite
		return "168h0m0s"
	case hours < 24: // < 1 day
		return "1h0m0s"
	case hours < 4320: // < 180 days
		return "24h0m0s"
	default:
		return "168h0m0s"
	}
}

// extractOnDB extracts the database name from the ON "db" clause in retention policy queries
// e.g., CREATE RETENTION POLICY "rp" ON "testdb" DURATION 7d ... → "testdb"
func extractOnDB(query string) string {
	upper := strings.ToUpper(query)
	onIdx := strings.Index(upper, " ON ")
	if onIdx < 0 {
		return ""
	}
	afterOn := strings.TrimSpace(query[onIdx+4:])
	return extractFirstToken(afterOn)
}
