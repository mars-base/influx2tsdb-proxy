package adapter

import (
	"fmt"
	"log"
	"strings"
)

// handleCreateRetentionPolicy handles CREATE RETENTION POLICY
// Syntax: CREATE RETENTION POLICY "name" ON "db" DURATION 7d REPLICATION 1 [SHARD DURATION 5m] DEFAULT
func (c *Converter) handleCreateRetentionPolicy(query string) *InfluxDBResponse {
	if c.retentionStore == nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "retention store not initialized"}}}
	}

	upper := strings.ToUpper(query)
	rest := query[len("CREATE RETENTION POLICY"):]
	upperRest := upper[len("CREATE RETENTION POLICY"):]

	// Extract database name from ON clause
	targetDB := extractOnDB(rest)
	if targetDB == "" {
		targetDB = c.dbName
	}

	// Extract policy name (first token, may be quoted)
	name := extractFirstToken(strings.TrimSpace(rest))
	if name == "" {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing retention policy name"}}}
	}

	// Extract DURATION (find first occurrence, which is the main DURATION, not SHARD DURATION)
	durIdx := strings.Index(upperRest, "DURATION")
	if durIdx < 0 {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing DURATION clause"}}}
	}
	afterDur := strings.TrimSpace(rest[durIdx+8:])
	duration := extractFirstToken(afterDur)
	if duration == "" {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing DURATION value"}}}
	}

	// Extract SHARD DURATION (optional)
	var shardDuration string
	shardIdx := strings.Index(upperRest, "SHARD DURATION")
	if shardIdx >= 0 {
		afterShard := strings.TrimSpace(rest[shardIdx+14:])
		shardDuration = extractFirstToken(afterShard)
	}

	// Check DEFAULT — only search after REPLICATION to avoid matching policy name
	replIdx := strings.LastIndex(upperRest, "REPLICATION")
	afterRepl := ""
	if replIdx >= 0 {
		afterRepl = upperRest[replIdx+11:]
	}
	isDefault := strings.Contains(afterRepl, "DEFAULT")

	if err := c.retentionStore.CreatePolicy(targetDB, name, duration, shardDuration, isDefault); err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}

	// Sync immediately to TimescaleDB
	if err := c.retentionStore.SyncToTimescaleDB(targetDB); err != nil {
		log.Printf("Warning: retention sync after CREATE failed: %v", err)
	}

	return emptyResult()
}

// handleAlterRetentionPolicy handles ALTER RETENTION POLICY
// Syntax: ALTER RETENTION POLICY "name" ON "db" [DURATION 30d] [SHARD DURATION 5m] DEFAULT
func (c *Converter) handleAlterRetentionPolicy(query string) *InfluxDBResponse {
	if c.retentionStore == nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "retention store not initialized"}}}
	}

	upper := strings.ToUpper(query)
	rest := query[len("ALTER RETENTION POLICY"):]
	upperRest := upper[len("ALTER RETENTION POLICY"):]

	// Extract database name from ON clause
	targetDB := extractOnDB(rest)
	if targetDB == "" {
		targetDB = c.dbName
	}

	// Extract policy name
	name := extractFirstToken(strings.TrimSpace(rest))
	if name == "" {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing retention policy name"}}}
	}

	// Extract DURATION (optional for ALTER) — find first DURATION but not SHARD DURATION
	var duration string
	durIdx := strings.Index(upperRest, "DURATION")
	if durIdx >= 0 {
		// Check if this is SHARD DURATION
		if durIdx >= 6 && upperRest[durIdx-6:durIdx] == "SHARD " {
			// This is SHARD DURATION, skip it; look for main DURATION elsewhere
			durIdx = -1
		}
	}
	if durIdx >= 0 {
		afterDur := strings.TrimSpace(rest[durIdx+8:])
		duration = extractFirstToken(afterDur)
		if duration == "" {
			return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing DURATION value"}}}
		}
	}

	// Extract SHARD DURATION (optional)
	var shardDuration string
	shardIdx := strings.Index(upperRest, "SHARD DURATION")
	if shardIdx >= 0 {
		afterShard := strings.TrimSpace(rest[shardIdx+14:])
		shardDuration = extractFirstToken(afterShard)
	}

	// Only call AlterPolicy if duration or shard duration changed
	if duration != "" || shardDuration != "" {
		if err := c.retentionStore.AlterPolicy(targetDB, name, duration, shardDuration); err != nil {
			return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
		}
	}

	// Handle DEFAULT (optional for ALTER)
	// Search only after DURATION or REPLICATION to avoid matching policy name
	alterTail := upperRest
	if durIdx >= 0 {
		alterTail = upperRest[durIdx+8:]
	} else if replIdx := strings.LastIndex(upperRest, "REPLICATION"); replIdx >= 0 {
		alterTail = upperRest[replIdx+11:]
	}
	hasAlterDefault := strings.Contains(alterTail, "DEFAULT")

	if hasAlterDefault {
		if err := c.retentionStore.SetDefault(targetDB, name); err != nil {
			return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
		}
	}

	// Sync to TimescaleDB if anything changed
	if duration != "" || shardDuration != "" || hasAlterDefault {
		if err := c.retentionStore.SyncToTimescaleDB(targetDB); err != nil {
			log.Printf("Warning: retention sync after ALTER failed: %v", err)
		}
	}

	return emptyResult()
}

// handleDropRetentionPolicy handles DROP RETENTION POLICY
// Syntax: DROP RETENTION POLICY "name" ON "db"
func (c *Converter) handleDropRetentionPolicy(query string) *InfluxDBResponse {
	if c.retentionStore == nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "retention store not initialized"}}}
	}

	rest := query[len("DROP RETENTION POLICY"):]

	// Extract database name from ON clause
	targetDB := extractOnDB(rest)
	if targetDB == "" {
		targetDB = c.dbName
	}

	name := extractFirstToken(strings.TrimSpace(rest))
	if name == "" {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing retention policy name"}}}
	}

	if err := c.retentionStore.DropPolicy(targetDB, name); err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}

	// Sync immediately to TimescaleDB (removes retention jobs since no default policy remains)
	if err := c.retentionStore.SyncToTimescaleDB(targetDB); err != nil {
		log.Printf("Warning: retention sync after DROP failed: %v", err)
	}

	return emptyResult()
}

// handleCreateDatabase handles CREATE DATABASE
// Syntax: CREATE DATABASE "name"
func (c *Converter) handleCreateDatabase(query string) *InfluxDBResponse {
	// Extract database name from query
	rest := strings.TrimSpace(query[len("CREATE DATABASE"):])
	name := extractFirstToken(rest)
	if name == "" {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing database name"}}}
	}

	if err := c.meta.CreateDatabase(name); err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}

	// Ensure retention policies are initialized for this database
	if c.retentionStore != nil {
		c.retentionStore.EnsureDatabasePolicies(name)
	}

	return emptyResult()
}

// handleDropDatabase handles DROP DATABASE
// Syntax: DROP DATABASE "name"
func (c *Converter) handleDropDatabase(query string) *InfluxDBResponse {
	// Extract database name from query
	rest := strings.TrimSpace(query[len("DROP DATABASE"):])
	name := extractFirstToken(rest)
	if name == "" {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing database name"}}}
	}

	if err := c.meta.DropDatabase(name); err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}
	}

	// Clean up retention policies for this database
	if c.retentionStore != nil {
		c.retentionStore.DropDatabasePolicies(name)
	}

	return emptyResult()
}

// handleDropMeasurement processes DROP MEASUREMENT queries
// Syntax: DROP MEASUREMENT "measurement_name"
func (c *Converter) handleDropMeasurement(query string) (*InfluxDBResponse, error) {
	// Extract measurement name after "DROP MEASUREMENT"
	prefix := "DROP MEASUREMENT"
	after := strings.TrimSpace(query[len(prefix):])
	measurement := extractFirstToken(after)

	if measurement == "" {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: "missing measurement name"}}}, nil
	}

	// Drop the table with schema qualification
	dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`,
		escapeIdent(c.dbName), escapeIdent(measurement))
	_, err := c.meta.pool.Exec(c.meta.ctx, dropSQL)
	if err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}, nil
	}

	// Remove metadata with db_name filter
	_, err = c.meta.pool.Exec(c.meta.ctx,
		"DELETE FROM _influx_meta WHERE db_name = $1 AND measurement = $2",
		c.dbName, measurement)
	if err != nil {
		return &InfluxDBResponse{Results: []InfluxDBResult{{Error: err.Error()}}}, nil
	}

	return emptyResult(), nil
}
