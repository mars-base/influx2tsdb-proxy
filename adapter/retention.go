package adapter

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RetentionPolicy represents an InfluxDB retention policy
type RetentionPolicy struct {
	Name       string
	DBName     string
	Duration   string // original duration string (e.g., "7d", "168h", "INF")
	DurationNs int64  // duration in nanoseconds (0 = infinite)
	IsDefault  bool
	CreatedAt  time.Time
}

// RetentionStore manages retention policies in _retention_policy table
type RetentionStore struct {
	ctx       context.Context
	pool      *pgxpool.Pool
	mu        sync.RWMutex
	policies  map[string]*RetentionPolicy // "dbName/name" -> policy
	metaStore *MetaStore                  // reference to MetaStore for tag column info
	lastSync  time.Time
	syncEvery time.Duration
}

// policyKey generates a cache key for a policy
func policyKey(dbName, name string) string {
	return dbName + "/" + name
}

// NewRetentionStore creates and initialized the retention store
func NewRetentionStore(ctx context.Context, pool *pgxpool.Pool) (*RetentionStore, error) {
	rs := &RetentionStore{
		ctx:       ctx,
		pool:      pool,
		policies:  make(map[string]*RetentionPolicy),
		syncEvery: 5 * time.Minute,
	}

	if err := rs.initialize(); err != nil {
		return nil, err
	}

	return rs, nil
}

// initialize creates the _retention_policy table and loads existing policies
func (rs *RetentionStore) initialize() error {
	sql := `CREATE TABLE IF NOT EXISTS _retention_policy (
		name TEXT PRIMARY KEY,
		duration TEXT NOT NULL,
		duration_ns BIGINT NOT NULL,
		is_default BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMPTZ DEFAULT NOW()
	)`
	if _, err := rs.pool.Exec(rs.ctx, sql); err != nil {
		return fmt.Errorf("create _retention_policy: %w", err)
	}

	// Migrate: rename old 'measurement' column to 'name' if it exists
	var oldColExists bool
	rs.pool.QueryRow(rs.ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = '_retention_policy' AND column_name = 'measurement')").Scan(&oldColExists)
	if oldColExists {
		_, err := rs.pool.Exec(rs.ctx, "ALTER TABLE _retention_policy RENAME COLUMN measurement TO name")
		if err != nil {
			return fmt.Errorf("migrate _retention_policy column: %w", err)
		}
		log.Println("Migrated _retention_policy: measurement -> name")
	}

	// Migrate: add db_name column if missing
	var hasDBName bool
	rs.pool.QueryRow(rs.ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = '_retention_policy' AND column_name = 'db_name')").Scan(&hasDBName)
	if !hasDBName {
		if _, err := rs.pool.Exec(rs.ctx, "ALTER TABLE _retention_policy ADD COLUMN db_name TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("migrate _retention_policy add db_name: %w", err)
		}
		// Update primary key to include db_name
		rs.pool.Exec(rs.ctx, "ALTER TABLE _retention_policy DROP CONSTRAINT IF EXISTS _retention_policy_pkey")
		rs.pool.Exec(rs.ctx, "ALTER TABLE _retention_policy ADD PRIMARY KEY (db_name, name)")
		log.Println("Migrated _retention_policy: added db_name column")
	}

	log.Println("Initialized _retention_policy table")

	// Load existing policies
	rows, err := rs.pool.Query(rs.ctx,
		"SELECT db_name, name, duration, duration_ns, is_default, created_at FROM _retention_policy ORDER BY db_name, name")
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rp RetentionPolicy
		if err := rows.Scan(&rp.DBName, &rp.Name, &rp.Duration, &rp.DurationNs, &rp.IsDefault, &rp.CreatedAt); err != nil {
			return err
		}
		rs.policies[policyKey(rp.DBName, rp.Name)] = &rp
	}
	log.Printf("Loaded %d retention policies", len(rs.policies))
	return rows.Err()
}

// SetMetaStore links the MetaStore so we can access tag column information
func (rs *RetentionStore) SetMetaStore(m *MetaStore) {
	rs.metaStore = m
}

// applyCompression enables columnar compression on a hypertable
// segmentby uses tag columns, orderby uses time DESC
func (rs *RetentionStore) applyCompression(dbName, tableName string, tagColumns []string) error {
	schemaName := escapeIdent(dbName)
	fullName := fmt.Sprintf(`"%s"."%s"`, schemaName, escapeIdent(tableName))

	// Build segmentby clause from tag columns
	segmentby := ""
	if len(tagColumns) > 0 {
		quotedTags := make([]string, len(tagColumns))
		for i, tag := range tagColumns {
			quotedTags[i] = fmt.Sprintf(`"%s"`, escapeIdent(tag))
		}
		segmentby = strings.Join(quotedTags, ",")
	}

	// Enable compression with segmentby and orderby
	compressSQL := fmt.Sprintf(`
		ALTER TABLE %s SET (
			timescaledb.compress,
			timescaledb.compress_segmentby = '%s',
			timescaledb.compress_orderby = 'time DESC'
		)`, fullName, segmentby)

	if _, err := rs.pool.Exec(rs.ctx, compressSQL); err != nil {
		// Ignore if already enabled
		if !strings.Contains(err.Error(), "already enabled") {
			log.Printf("Warning: enable compression on %s: %v", fullName, err)
		}
	}

	// Add automatic compression policy (compress data older than compress_after)
	compressAfter := rs.DefaultCompressAfter(dbName)
	policySQL := fmt.Sprintf(`SELECT add_compression_policy('%s', INTERVAL '%s', if_not_exists => true)`, fullName, compressAfter)
	if _, err := rs.pool.Exec(rs.ctx, policySQL); err != nil {
		log.Printf("Warning: add compression policy on %s: %v", fullName, err)
	} else {
		log.Printf("Applied compression policy: %s -> compress_after = %s", fullName, compressAfter)
	}

	return nil
}

// RunSyncLoop runs the background retention policy sync loop
func (rs *RetentionStore) RunSyncLoop(intervalMinutes int) {
	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
	defer ticker.Stop()

	log.Printf("Retention sync loop started (every %d minutes)", intervalMinutes)

	for {
		select {
		case <-ticker.C:
			if err := rs.SyncAllDatabases(); err != nil {
				log.Printf("Retention sync error: %v", err)
			}
		case <-rs.ctx.Done():
			log.Println("Retention sync loop stopped")
			return
		}
	}
}

// EnsureDatabasePolicies ensures the default autogen policy exists for a database
func (rs *RetentionStore) EnsureDatabasePolicies(dbName string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	key := policyKey(dbName, "autogen")
	if _, exists := rs.policies[key]; exists {
		return nil
	}

	// Check if any default exists for this database
	var defaultCount int
	rs.pool.QueryRow(rs.ctx,
		"SELECT COUNT(*) FROM _retention_policy WHERE db_name = $1 AND is_default = TRUE", dbName).Scan(&defaultCount)

	// Insert autogen as default if no default exists
	isDefault := defaultCount == 0
	_, err := rs.pool.Exec(rs.ctx,
		`INSERT INTO _retention_policy (db_name, name, duration, duration_ns, is_default)
		VALUES ($1, 'autogen', '0s', 0, $2)
		ON CONFLICT (db_name, name) DO NOTHING`, dbName, isDefault)
	if err != nil {
		return fmt.Errorf("insert autogen policy for %s: %w", dbName, err)
	}

	rs.policies[key] = &RetentionPolicy{
		Name:       "autogen",
		DBName:     dbName,
		Duration:   "0s",
		DurationNs: 0,
		IsDefault:  isDefault,
		CreatedAt:  time.Now(),
	}

	log.Printf("Created autogen retention policy for database %s (default=%v)", dbName, isDefault)
	return nil
}

// DropDatabasePolicies removes all retention policies for a database
func (rs *RetentionStore) DropDatabasePolicies(dbName string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.pool.Exec(rs.ctx, "DELETE FROM _retention_policy WHERE db_name = $1", dbName)

	// Remove from cache
	for key, rp := range rs.policies {
		if rp.DBName == dbName {
			delete(rs.policies, key)
		}
	}

	log.Printf("Dropped all retention policies for database %s", dbName)
}

// CreatePolicy creates or updates a retention policy for a database
func (rs *RetentionStore) CreatePolicy(dbName, name, duration string, isDefault bool) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Parse duration
	durationNs, err := parseDuration(duration)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", duration, err)
	}

	// If is_default is true, clear other defaults for this database
	if isDefault {
		if _, err := rs.pool.Exec(rs.ctx,
			"UPDATE _retention_policy SET is_default = FALSE WHERE db_name = $1 AND is_default = TRUE", dbName); err != nil {
			return fmt.Errorf("clear default: %w", err)
		}
		// Update cache: clear all other defaults for this database
		for _, rp := range rs.policies {
			if rp.DBName == dbName {
				rp.IsDefault = false
			}
		}
	}

	// Upsert policy
	sql := `INSERT INTO _retention_policy (db_name, name, duration, duration_ns, is_default)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (db_name, name) DO UPDATE
		SET duration = EXCLUDED.duration,
		    duration_ns = EXCLUDED.duration_ns,
		    is_default = EXCLUDED.is_default`
	if _, err := rs.pool.Exec(rs.ctx, sql, dbName, name, duration, durationNs, isDefault); err != nil {
		return fmt.Errorf("upsert policy: %w", err)
	}

	// Update cache (preserve CreatedAt if policy already exists)
	key := policyKey(dbName, name)
	if existing, ok := rs.policies[key]; ok {
		existing.Duration = duration
		existing.DurationNs = durationNs
		existing.IsDefault = isDefault
	} else {
		rs.policies[key] = &RetentionPolicy{
			Name:       name,
			DBName:     dbName,
			Duration:   duration,
			DurationNs: durationNs,
			IsDefault:  isDefault,
			CreatedAt:  time.Now(),
		}
	}

	// Reload all policies from DB to sync is_default changes
	rows, err := rs.pool.Query(rs.ctx,
		"SELECT name, is_default FROM _retention_policy WHERE db_name = $1", dbName)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var pName string
			var pDefault bool
			if err := rows.Scan(&pName, &pDefault); err == nil {
				if p, ok := rs.policies[policyKey(dbName, pName)]; ok {
					p.IsDefault = pDefault
				}
			}
		}
	}

	log.Printf("Created/updated retention policy: %s.%s duration=%s default=%v", dbName, name, duration, isDefault)
	return nil
}

// AlterPolicy updates an existing retention policy
func (rs *RetentionStore) AlterPolicy(dbName, name, duration string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Check if exists
	key := policyKey(dbName, name)
	existing, ok := rs.policies[key]
	if !ok {
		return fmt.Errorf("retention policy %q not found in database %q", name, dbName)
	}

	// Parse duration
	durationNs, err := parseDuration(duration)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", duration, err)
	}

	// Update
	sql := `UPDATE _retention_policy SET duration = $3, duration_ns = $4 WHERE db_name = $1 AND name = $2`
	if _, err := rs.pool.Exec(rs.ctx, sql, dbName, name, duration, durationNs); err != nil {
		return fmt.Errorf("update policy: %w", err)
	}

	// Update cache
	existing.Duration = duration
	existing.DurationNs = durationNs

	log.Printf("Altered retention policy: %s.%s duration=%s", dbName, name, duration)
	return nil
}

// SetDefault marks the given policy as default and clears all others in the same database
func (rs *RetentionStore) SetDefault(dbName, name string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	key := policyKey(dbName, name)
	if _, ok := rs.policies[key]; !ok {
		return fmt.Errorf("retention policy %q not found in database %q", name, dbName)
	}

	// Clear all defaults in this database
	if _, err := rs.pool.Exec(rs.ctx,
		"UPDATE _retention_policy SET is_default = FALSE WHERE db_name = $1 AND is_default = TRUE", dbName); err != nil {
		return fmt.Errorf("clear defaults: %w", err)
	}

	// Set new default
	if _, err := rs.pool.Exec(rs.ctx,
		"UPDATE _retention_policy SET is_default = TRUE WHERE db_name = $1 AND name = $2", dbName, name); err != nil {
		return fmt.Errorf("set default: %w", err)
	}

	// Update cache
	for _, rp := range rs.policies {
		if rp.DBName == dbName {
			rp.IsDefault = rp.Name == name
		}
	}

	log.Printf("Set default retention policy: %s.%s", dbName, name)
	return nil
}

// DropPolicy removes a retention policy
func (rs *RetentionStore) DropPolicy(dbName, name string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Check if exists
	key := policyKey(dbName, name)
	policy, ok := rs.policies[key]
	if !ok {
		return fmt.Errorf("retention policy %q not found in database %q", name, dbName)
	}

	// InfluxDB does not allow dropping the default retention policy
	if policy.IsDefault {
		return fmt.Errorf("cannot drop default retention policy %q; use ALTER to set another policy as DEFAULT first", name)
	}

	// Delete from table
	if _, err := rs.pool.Exec(rs.ctx,
		"DELETE FROM _retention_policy WHERE db_name = $1 AND name = $2", dbName, name); err != nil {
		return fmt.Errorf("delete policy: %w", err)
	}

	// Update cache
	delete(rs.policies, key)

	log.Printf("Dropped retention policy: %s.%s", dbName, name)
	return nil
}

// ShowPolicies returns all retention policies for a database, sorted by name
func (rs *RetentionStore) ShowPolicies(dbName string) []RetentionPolicy {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var names []string
	for key, rp := range rs.policies {
		if rp.DBName == dbName {
			names = append(names, rp.Name)
			_ = key
		}
	}
	sort.Strings(names)

	result := make([]RetentionPolicy, 0, len(names))
	for _, name := range names {
		result = append(result, *rs.policies[policyKey(dbName, name)])
	}
	return result
}

// SyncToTimescaleDB applies the default retention policy to all hypertables in a database schema.
func (rs *RetentionStore) SyncToTimescaleDB(dbName string) error {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	// Find the default policy for this database
	var defaultPolicy *RetentionPolicy
	for _, rp := range rs.policies {
		if rp.DBName == dbName && rp.IsDefault {
			defaultPolicy = rp
			break
		}
	}
	if defaultPolicy == nil {
		for _, rp := range rs.policies {
			if rp.DBName == dbName && rp.DurationNs > 0 {
				defaultPolicy = rp
				break
			}
		}
	}

	// Get all hypertables in the specified schema
	rows, err := rs.pool.Query(rs.ctx,
		"SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_schema = $1", dbName)
	if err != nil {
		return fmt.Errorf("query hypertables: %w", err)
	}
	defer rows.Close()

	var hypertables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		// Skip internal tables (starting with _)
		if strings.HasPrefix(name, "_") {
			continue
		}
		hypertables = append(hypertables, name)
	}
	rows.Close()

	log.Printf("Syncing retention policies to %d hypertables in schema %s...", len(hypertables), dbName)

	for _, htName := range hypertables {
		fullName := fmt.Sprintf(`"%s"."%s"`, escapeIdent(dbName), escapeIdent(htName))

		// Remove existing retention policy (if any)
		_, _ = rs.pool.Exec(rs.ctx,
			fmt.Sprintf("SELECT remove_retention_policy('%s')", fullName))

		// Apply default policy if it exists and has a valid duration
		if defaultPolicy != nil && defaultPolicy.DurationNs > 0 {
			durationSec := defaultPolicy.DurationNs / int64(time.Second)
			interval := fmt.Sprintf("%d seconds", durationSec)

			_, err = rs.pool.Exec(rs.ctx,
				fmt.Sprintf("SELECT add_retention_policy('%s', INTERVAL '%s', if_not_exists => true)", fullName, interval))
			if err != nil {
				log.Printf("Add retention policy for %s: %v", fullName, err)
				continue
			}
			log.Printf("Applied retention policy: %s -> %s (from %s)", fullName, interval, defaultPolicy.Name)
		}

		// Sync chunk_time_interval to match shardGroupDuration
		chunkInterval := rs.DefaultChunkInterval(dbName)
		_, err = rs.pool.Exec(rs.ctx,
			fmt.Sprintf("SELECT set_chunk_time_interval('%s', INTERVAL '%s')", fullName, chunkInterval))
		if err != nil {
			log.Printf("Set chunk_time_interval for %s: %v", fullName, err)
		} else {
			log.Printf("Applied chunk_time_interval: %s -> %s", fullName, chunkInterval)
		}

		// Apply columnar compression
		if rs.metaStore != nil {
			key := dbMeasurementKey(dbName, htName)
			rs.metaStore.mu.RLock()
			schema := rs.metaStore.tables[key]
			var tagCols []string
			if schema != nil {
				tagCols = schema.Tags
			}
			rs.metaStore.mu.RUnlock()
			rs.applyCompression(dbName, htName, tagCols)
		}
	}

	rs.lastSync = time.Now()
	return nil
}

// SyncAllDatabases syncs retention policies for all known databases
func (rs *RetentionStore) SyncAllDatabases() error {
	rs.mu.RLock()
	// Collect unique database names
	dbSet := make(map[string]bool)
	for _, rp := range rs.policies {
		dbSet[rp.DBName] = true
	}
	rs.mu.RUnlock()

	for dbName := range dbSet {
		if err := rs.SyncToTimescaleDB(dbName); err != nil {
			log.Printf("Retention sync error for %s: %v", dbName, err)
		}
	}
	return nil
}

// DefaultChunkInterval returns the chunk_time_interval based on the default
// retention policy duration for a specific database.
// Returns "1 week" if no policy is set.
func (rs *RetentionStore) DefaultChunkInterval(dbName string) string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var defaultPolicy *RetentionPolicy
	for _, rp := range rs.policies {
		if rp.DBName == dbName && rp.IsDefault {
			defaultPolicy = rp
			break
		}
	}
	if defaultPolicy == nil {
		for _, rp := range rs.policies {
			if rp.DBName == dbName && rp.DurationNs > 0 {
				defaultPolicy = rp
				break
			}
		}
	}

	if defaultPolicy == nil || defaultPolicy.DurationNs == 0 {
		return "1 week"
	}

	hours := defaultPolicy.DurationNs / int64(time.Hour)
	switch {
	case hours < 24: // < 1 day
		return "1 hour"
	case hours < 4320: // < 180 days
		return "1 day"
	default:
		return "1 week"
	}
}

// DefaultCompressAfter returns the compress_after interval based on the default
// retention policy duration for a specific database.
// Uses retention_duration / 4 as the compression delay.
// Returns "7 days" if no policy is set.
func (rs *RetentionStore) DefaultCompressAfter(dbName string) string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var defaultPolicy *RetentionPolicy
	for _, rp := range rs.policies {
		if rp.DBName == dbName && rp.IsDefault {
			defaultPolicy = rp
			break
		}
	}
	if defaultPolicy == nil {
		for _, rp := range rs.policies {
			if rp.DBName == dbName && rp.DurationNs > 0 {
				defaultPolicy = rp
				break
			}
		}
	}

	if defaultPolicy == nil || defaultPolicy.DurationNs == 0 {
		return "7 days"
	}

	// compress_after = retention_duration / 4
	hours := defaultPolicy.DurationNs / int64(time.Hour)
	compressHours := hours / 4

	switch {
	case compressHours < 1:
		return "15 minutes"
	case compressHours < 24:
		return fmt.Sprintf("%d hours", compressHours)
	case compressHours < 168: // < 7 days
		days := compressHours / 24
		return fmt.Sprintf("%d days", days)
	default:
		weeks := compressHours / 168
		return fmt.Sprintf("%d weeks", weeks)
	}
}

// parseDuration parses InfluxDB duration format to nanoseconds
// Supported: 1h, 24h, 7d, 30d, 168h0m0s, INF, 0s (infinite)
func parseDuration(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	// Handle infinite duration
	if strings.ToUpper(s) == "INF" || s == "0s" || s == "0" {
		return 0, nil
	}

	// Try standard Go duration first
	if d, err := time.ParseDuration(s); err == nil {
		return d.Nanoseconds(), nil
	}

	// Try InfluxDB-specific formats
	// Pattern: number + unit (h=hour, d=day, w=week)
	re := regexp.MustCompile(`^(\d+)([hdw])$`)
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return 0, fmt.Errorf("unsupported format")
	}

	num, _ := strconv.ParseInt(matches[1], 10, 64)
	unit := matches[2]

	var multiplier int64
	switch unit {
	case "h":
		multiplier = int64(time.Hour)
	case "d":
		multiplier = 24 * int64(time.Hour)
	case "w":
		multiplier = 7 * 24 * int64(time.Hour)
	default:
		return 0, fmt.Errorf("unknown unit %q", unit)
	}

	return num * multiplier, nil
}
