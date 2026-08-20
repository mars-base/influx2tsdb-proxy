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
	ctx  context.Context
	pool *pgxpool.Pool
	mu   sync.RWMutex
	// Cache: measurement -> table schema
	tables map[string]*TableSchema
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
		ctx:    ctx,
		pool:   pool,
		tables: make(map[string]*TableSchema),
	}
}

// Initialize creates the _influx_meta table and loads existing schemas
func (m *MetaStore) Initialize() error {
	sql := `CREATE TABLE IF NOT EXISTS _influx_meta (
		measurement TEXT NOT NULL,
		column_name TEXT NOT NULL,
		column_type TEXT NOT NULL,
		PRIMARY KEY (measurement, column_name)
	)`
	if _, err := m.pool.Exec(m.ctx, sql); err != nil {
		return fmt.Errorf("create _influx_meta: %w", err)
	}
	log.Println("Initialized _influx_meta table")
	return m.loadSchemas()
}

func (m *MetaStore) loadSchemas() error {
	rows, err := m.pool.Query(m.ctx, "SELECT measurement, column_name, column_type FROM _influx_meta ORDER BY measurement, column_name")
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var measurement, colName, colType string
		if err := rows.Scan(&measurement, &colName, &colType); err != nil {
			return err
		}
		schema, ok := m.tables[measurement]
		if !ok {
			schema = &TableSchema{}
			m.tables[measurement] = schema
		}
		if colType == "tag" {
			schema.Tags = append(schema.Tags, colName)
		} else {
			schema.Fields = append(schema.Fields, colName)
		}
	}
	return rows.Err()
}

// EnsureTable creates the measurement table and hypertable if needed
func (m *MetaStore) EnsureTable(measurement string, tags map[string]string, fields map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema, exists := m.tables[measurement]
	if !exists {
		schema = &TableSchema{}
		m.tables[measurement] = schema
	}

	// Check for new tag columns
	for tagName := range tags {
		if !containsStr(schema.Tags, tagName) {
			schema.Tags = append(schema.Tags, tagName)
			m.pool.Exec(m.ctx,
				"INSERT INTO _influx_meta (measurement, column_name, column_type) VALUES ($1, $2, 'tag') ON CONFLICT DO NOTHING",
				measurement, tagName)
		}
	}

	// Check for new field columns
	for fieldName, val := range fields {
		if !containsStr(schema.Fields, fieldName) {
			schema.Fields = append(schema.Fields, fieldName)
			colType := inferColumnType(val)
			m.pool.Exec(m.ctx,
				"INSERT INTO _influx_meta (measurement, column_name, column_type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
				measurement, fieldName, colType)
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

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)",
		escapeIdent(measurement), strings.Join(cols, ", "))
	if _, err := m.pool.Exec(m.ctx, createSQL); err != nil {
		return fmt.Errorf("create table %s: %w", measurement, err)
	}

	// Create hypertable
	hyperSQL := fmt.Sprintf("SELECT create_hypertable('%s', by_range('time'), if_not_exists => true)",
		escapeIdent(measurement))
	if _, err := m.pool.Exec(m.ctx, hyperSQL); err != nil {
		return fmt.Errorf("create hypertable %s: %w", measurement, err)
	}

	return nil
}

// InsertBatch inserts records into a measurement table using a transaction
func (m *MetaStore) InsertBatch(measurement string, records []LineRecord) error {
	if len(records) == 0 {
		return nil
	}

	m.mu.RLock()
	schema := m.tables[measurement]
	m.mu.RUnlock()

	if schema == nil {
		return fmt.Errorf("no schema for measurement %s", measurement)
	}

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

	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)",
		escapeIdent(measurement),
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

// GetMeasurements returns all known measurement names
func (m *MetaStore) GetMeasurements() ([]string, error) {
	rows, err := m.pool.Query(m.ctx, "SELECT DISTINCT measurement FROM _influx_meta ORDER BY measurement")
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
func (m *MetaStore) GetTagValues(measurement, tagKey string) ([]string, error) {
	sql := fmt.Sprintf(`SELECT DISTINCT "%s" FROM "%s" WHERE "%s" IS NOT NULL ORDER BY 1 LIMIT 1000`,
		escapeIdent(tagKey), escapeIdent(measurement), escapeIdent(tagKey))

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
func (m *MetaStore) GetFields(measurement string) ([]FieldInfo, error) {
	rows, err := m.pool.Query(m.ctx,
		"SELECT column_name, column_type FROM _influx_meta WHERE measurement = $1 AND column_type != 'tag' ORDER BY column_name",
		measurement)
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
