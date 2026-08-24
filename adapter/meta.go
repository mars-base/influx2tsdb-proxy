package adapter

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MetaStore manages measurement/tag/field metadata and table auto-creation
type MetaStore struct {
	ctx            context.Context
	pool           *pgxpool.Pool
	mu             sync.RWMutex
	// Cache: "db/measurement" -> table schema
	tables         map[string]*TableSchema
	// Cache of known database names
	databases      map[string]bool
	retentionStore *RetentionStore // optional, for chunk_time_interval
}

type TableSchema struct {
	Tags   []string // tag column names (TEXT)
	Fields []string // field column names (DOUBLE PRECISION)
}

type FieldInfo struct {
	Name string
	Type string // "integer", "float", "string", "boolean"
}

func NewMetaStore(ctx context.Context, pool *pgxpool.Pool) *MetaStore {
	return &MetaStore{
		ctx:       ctx,
		pool:      pool,
		tables:    make(map[string]*TableSchema),
		databases: make(map[string]bool),
	}
}

// SetRetentionStore links the retention store so EnsureTable can set chunk_time_interval
func (m *MetaStore) SetRetentionStore(rs *RetentionStore) {
	m.retentionStore = rs
}

// Initialize creates the metadata tables and loads existing schemas
func (m *MetaStore) Initialize() error {
	// Create _influx_databases table
	sql := `CREATE TABLE IF NOT EXISTS _influx_databases (
		name TEXT PRIMARY KEY,
		created_at TIMESTAMPTZ DEFAULT NOW()
	)`
	if _, err := m.pool.Exec(m.ctx, sql); err != nil {
		return fmt.Errorf("create _influx_databases: %w", err)
	}

	// Create _influx_meta table
	sql = `CREATE TABLE IF NOT EXISTS _influx_meta (
		measurement TEXT NOT NULL,
		column_name TEXT NOT NULL,
		column_type TEXT NOT NULL,
		PRIMARY KEY (measurement, column_name)
	)`
	if _, err := m.pool.Exec(m.ctx, sql); err != nil {
		return fmt.Errorf("create _influx_meta: %w", err)
	}

	// Migrate: add db_name column to _influx_meta if missing
	var hasDBName bool
	m.pool.QueryRow(m.ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = '_influx_meta' AND column_name = 'db_name')").Scan(&hasDBName)
	if !hasDBName {
		if _, err := m.pool.Exec(m.ctx, "ALTER TABLE _influx_meta ADD COLUMN db_name TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("migrate _influx_meta add db_name: %w", err)
		}
		// Update primary key to include db_name
		m.pool.Exec(m.ctx, "ALTER TABLE _influx_meta DROP CONSTRAINT IF EXISTS _influx_meta_pkey")
		m.pool.Exec(m.ctx, "ALTER TABLE _influx_meta ADD PRIMARY KEY (db_name, measurement, column_name)")
		log.Println("Migrated _influx_meta: added db_name column")
	}

	log.Println("Initialized metadata tables")
	return m.loadSchemas()
}

func (m *MetaStore) loadSchemas() error {
	// Reset caches before reloading to avoid duplicates on repeated calls
	m.tables = make(map[string]*TableSchema)
	m.databases = make(map[string]bool)

	rows, err := m.pool.Query(m.ctx, "SELECT db_name, measurement, column_name, column_type FROM _influx_meta ORDER BY db_name, measurement, column_name")
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName, measurement, colName, colType string
		if err := rows.Scan(&dbName, &measurement, &colName, &colType); err != nil {
			return err
		}
		key := dbMeasurementKey(dbName, measurement)
		schema, ok := m.tables[key]
		if !ok {
			schema = &TableSchema{}
			m.tables[key] = schema
		}
		if colType == "tag" {
			schema.Tags = append(schema.Tags, colName)
		} else {
			schema.Fields = append(schema.Fields, colName)
		}
	}

	// Load databases list
	dbRows, err := m.pool.Query(m.ctx, "SELECT name FROM _influx_databases")
	if err == nil {
		defer dbRows.Close()
		for dbRows.Next() {
			var name string
			if err := dbRows.Scan(&name); err == nil {
				m.databases[name] = true
			}
		}
	}

	log.Printf("Loaded %d measurement schemas, %d databases", len(m.tables), len(m.databases))
	return nil
}

// --- Database management ---

// CreateDatabase creates a new InfluxDB database (PostgreSQL schema + metadata)
func (m *MetaStore) CreateDatabase(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.databases[name] {
		return nil // already exists
	}

	// Create PostgreSQL schema
	schemaSQL := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, escapeIdent(name))
	if _, err := m.pool.Exec(m.ctx, schemaSQL); err != nil {
		return fmt.Errorf("create schema %s: %w", name, err)
	}

	// Insert into _influx_databases
	if _, err := m.pool.Exec(m.ctx,
		"INSERT INTO _influx_databases (name) VALUES ($1) ON CONFLICT DO NOTHING", name); err != nil {
		return fmt.Errorf("insert _influx_databases: %w", err)
	}

	m.databases[name] = true
	log.Printf("Created database: %s", name)
	return nil
}

// DropDatabase drops an InfluxDB database (drops schema + cleanup metadata)
func (m *MetaStore) DropDatabase(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.databases[name] {
		return fmt.Errorf("database %q not found", name)
	}

	// Delete from _influx_databases
	m.pool.Exec(m.ctx, "DELETE FROM _influx_databases WHERE name = $1", name)

	// Drop PostgreSQL schema and all objects within
	dropSQL := fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, escapeIdent(name))
	if _, err := m.pool.Exec(m.ctx, dropSQL); err != nil {
		return fmt.Errorf("drop schema %s: %w", name, err)
	}

	// Cleanup metadata
	m.pool.Exec(m.ctx, "DELETE FROM _influx_meta WHERE db_name = $1", name)

	// Remove from cache
	delete(m.databases, name)
	for key := range m.tables {
		if strings.HasPrefix(key, name+"/") {
			delete(m.tables, key)
		}
	}

	log.Printf("Dropped database: %s", name)
	return nil
}

// ListDatabases returns all known database names sorted alphabetically
func (m *MetaStore) ListDatabases() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.databases))
	for name := range m.databases {
		names = append(names, name)
	}
	// Sort
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[i] > names[j] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

// DatabaseExists checks if a database exists
func (m *MetaStore) DatabaseExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.databases[name]
}

// EnsureDatabase ensures a database exists (auto-create on write)
func (m *MetaStore) EnsureDatabase(name string) error {
	if m.DatabaseExists(name) {
		return nil
	}

	// Create PostgreSQL schema
	schemaSQL := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, escapeIdent(name))
	if _, err := m.pool.Exec(m.ctx, schemaSQL); err != nil {
		return fmt.Errorf("create schema %s: %w", name, err)
	}

	// Insert into _influx_databases
	m.pool.Exec(m.ctx,
		"INSERT INTO _influx_databases (name) VALUES ($1) ON CONFLICT DO NOTHING", name)

	m.mu.Lock()
	m.databases[name] = true
	m.mu.Unlock()

	log.Printf("Auto-created database: %s", name)
	return nil
}

// MigrateExistingData moves existing public-schema tables to the default database schema.
// Called on startup for backward compatibility with single-database deployments.
func (m *MetaStore) MigrateExistingData(defaultDB string) error {
	if defaultDB == "" {
		return nil
	}

	// Ensure the default database exists
	if err := m.EnsureDatabase(defaultDB); err != nil {
		return err
	}

	// Find user tables in public schema (non-internal tables)
	rows, err := m.pool.Query(m.ctx, `
		SELECT tablename FROM pg_tables
		WHERE schemaname = 'public'
		AND tablename NOT LIKE '\_%'
	`)
	if err != nil {
		return fmt.Errorf("query public tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		tables = append(tables, name)
	}

	if len(tables) == 0 {
		return nil
	}

	log.Printf("Migrating %d tables from public schema to %s...", len(tables), defaultDB)

	for _, table := range tables {
		// Move table to the database schema
		moveSQL := fmt.Sprintf(`ALTER TABLE public."%s" SET SCHEMA "%s"`,
			escapeIdent(table), escapeIdent(defaultDB))
		if _, err := m.pool.Exec(m.ctx, moveSQL); err != nil {
			log.Printf("Warning: could not move table %s to schema %s: %v", table, defaultDB, err)
		} else {
			log.Printf("Moved table %s to schema %s", table, defaultDB)
		}
	}

	// Update _influx_meta: set db_name for rows with empty db_name
	m.pool.Exec(m.ctx, "UPDATE _influx_meta SET db_name = $1 WHERE db_name = ''", defaultDB)

	// Reload schemas
	m.loadSchemas()

	return nil
}

// --- Table and data operations ---

// EnsureTable creates the measurement table and hypertable in the specified database schema
func (m *MetaStore) EnsureTable(dbName, measurement string, tags map[string]string, fields map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := dbMeasurementKey(dbName, measurement)
	schema, exists := m.tables[key]
	if !exists {
		schema = &TableSchema{}
		m.tables[key] = schema
	}

	schemaName := escapeIdent(dbName)

	// Check for new tag columns
	for tagName := range tags {
		if !containsStr(schema.Tags, tagName) {
			schema.Tags = append(schema.Tags, tagName)
			m.pool.Exec(m.ctx,
				"INSERT INTO _influx_meta (db_name, measurement, column_name, column_type) VALUES ($1, $2, $3, 'tag') ON CONFLICT DO NOTHING",
				dbName, measurement, tagName)
			// Add column to existing table
			if exists {
				m.pool.Exec(m.ctx,
					fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS "%s" TEXT`,
						schemaName, escapeIdent(measurement), escapeIdent(tagName)))
			}
		}
	}

	// Check for new field columns
	for fieldName, val := range fields {
		if !containsStr(schema.Fields, fieldName) {
			schema.Fields = append(schema.Fields, fieldName)
			colType := inferColumnType(val)
			m.pool.Exec(m.ctx,
				"INSERT INTO _influx_meta (db_name, measurement, column_name, column_type) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
				dbName, measurement, fieldName, colType)
			// Add column to existing table
			if exists {
				m.pool.Exec(m.ctx,
					fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS "%s" DOUBLE PRECISION`,
						schemaName, escapeIdent(measurement), escapeIdent(fieldName)))
			}
		}
	}

	// Build CREATE TABLE with all known columns
	var cols []string
	cols = append(cols, `"time" TIMESTAMPTZ NOT NULL`)
	for _, tag := range schema.Tags {
		cols = append(cols, fmt.Sprintf(`"%s" TEXT`, escapeIdent(tag)))
	}
	for _, field := range schema.Fields {
		cols = append(cols, fmt.Sprintf(`"%s" DOUBLE PRECISION`, escapeIdent(field)))
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (%s)`,
		schemaName, escapeIdent(measurement), strings.Join(cols, ", "))
	if _, err := m.pool.Exec(m.ctx, createSQL); err != nil {
		return fmt.Errorf("create table %s.%s: %w", dbName, measurement, err)
	}

	// Create hypertable
	hyperSQL := fmt.Sprintf(`SELECT create_hypertable('"%s"."%s"', by_range('time'), if_not_exists => true)`,
		schemaName, escapeIdent(measurement))
	if _, err := m.pool.Exec(m.ctx, hyperSQL); err != nil {
		return fmt.Errorf("create hypertable %s.%s: %w", dbName, measurement, err)
	}

	// Apply chunk_time_interval from retention policy (if available)
	if m.retentionStore != nil {
		chunkInterval := m.retentionStore.DefaultChunkInterval(dbName)
		_, _ = m.pool.Exec(m.ctx,
			fmt.Sprintf(`SELECT set_chunk_time_interval('"%s"."%s"', INTERVAL '%s')`,
				schemaName, escapeIdent(measurement), chunkInterval))
	}

	return nil
}

// InsertBatch inserts records into a measurement table using a transaction
func (m *MetaStore) InsertBatch(dbName, measurement string, records []LineRecord) error {
	if len(records) == 0 {
		return nil
	}

	m.mu.RLock()
	key := dbMeasurementKey(dbName, measurement)
	schema := m.tables[key]
	m.mu.RUnlock()

	if schema == nil {
		return fmt.Errorf("no schema for measurement %s.%s", dbName, measurement)
	}

	schemaName := escapeIdent(dbName)

	// Build column list: time + tags + fields
	allCols := make([]string, 0, 1+len(schema.Tags)+len(schema.Fields))
	allCols = append(allCols, "time")
	allCols = append(allCols, schema.Tags...)
	allCols = append(allCols, schema.Fields...)

	quotedCols := make([]string, len(allCols))
	for i, c := range allCols {
		quotedCols[i] = fmt.Sprintf(`"%s"`, escapeIdent(c))
	}
	placeholders := make([]string, len(allCols))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertSQL := fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
		schemaName, escapeIdent(measurement),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	tx, err := m.pool.Begin(m.ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(m.ctx)

	for _, rec := range records {
		args := make([]interface{}, len(allCols))
		args[0] = rec.Timestamp

		tagOffset := 1
		for i, tag := range schema.Tags {
			if val, ok := rec.Tags[tag]; ok {
				args[tagOffset+i] = val
			}
		}

		fieldOffset := 1 + len(schema.Tags)
		for i, field := range schema.Fields {
			if val, ok := rec.Fields[field]; ok {
				args[fieldOffset+i] = toFloat64(val)
			}
		}

		if _, err := tx.Exec(m.ctx, insertSQL, args...); err != nil {
			return fmt.Errorf("insert row: %w", err)
		}
	}

	return tx.Commit(m.ctx)
}

// GetMeasurements returns all known measurement names for a database
func (m *MetaStore) GetMeasurements(dbName string) ([]string, error) {
	rows, err := m.pool.Query(m.ctx,
		"SELECT DISTINCT measurement FROM _influx_meta WHERE db_name = $1 ORDER BY measurement", dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// GetTagValues returns distinct values for a tag column
func (m *MetaStore) GetTagValues(dbName, measurement, tagKey string) ([]string, error) {
	sql := fmt.Sprintf(`SELECT DISTINCT "%s" FROM "%s"."%s" WHERE "%s" IS NOT NULL ORDER BY 1 LIMIT 1000`,
		escapeIdent(tagKey), escapeIdent(dbName), escapeIdent(measurement), escapeIdent(tagKey))

	rows, err := m.pool.Query(m.ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, rows.Err()
}

// GetFields returns field info for a measurement
func (m *MetaStore) GetFields(dbName, measurement string) ([]FieldInfo, error) {
	rows, err := m.pool.Query(m.ctx,
		"SELECT column_name, column_type FROM _influx_meta WHERE db_name = $1 AND measurement = $2 AND column_type != 'tag' ORDER BY column_name",
		dbName, measurement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []FieldInfo
	for rows.Next() {
		var name, colType string
		if err := rows.Scan(&name, &colType); err != nil {
			return nil, err
		}
		fields = append(fields, FieldInfo{Name: name, Type: colType})
	}
	return fields, rows.Err()
}

// helper functions

func dbMeasurementKey(dbName, measurement string) string {
	return dbName + "/" + measurement
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func inferColumnType(val interface{}) string {
	switch val.(type) {
	case int64:
		return "integer"
	case float64:
		return "float"
	case string:
		return "string"
	case bool:
		return "boolean"
	default:
		return "float"
	}
}

func toFloat64(val interface{}) interface{} {
	switch v := val.(type) {
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case float64:
		return v
	case bool:
		if v {
			return 1.0
		}
		return 0.0
	default:
		return nil
	}
}

func escapeIdent(s string) string {
	return strings.ReplaceAll(s, `"`, `""`)
}

var _ = time.Now // ensure time import
