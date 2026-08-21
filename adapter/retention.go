package adapter

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RetentionPolicy represents an InfluxDB retention policy
type RetentionPolicy struct {
	Name       string
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
	policies  map[string]*RetentionPolicy // measurement -> policy
	lastSync  time.Time
	syncEvery time.Duration
}

// NewRetentionStore creates and initializes the retention store
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
		measurement TEXT PRIMARY KEY,
		duration TEXT NOT NULL,
		duration_ns BIGINT NOT NULL,
		is_default BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMPTZ DEFAULT NOW()
	)`
	if _, err := rs.pool.Exec(rs.ctx, sql); err != nil {
		return fmt.Errorf("create _retention_policy: %w", err)
	}
	log.Println("Initialized _retention_policy table")

	// Load existing policies
	rows, err := rs.pool.Query(rs.ctx,
		"SELECT measurement, duration, duration_ns, is_default, created_at FROM _retention_policy ORDER BY measurement")
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rp RetentionPolicy
		if err := rows.Scan(&rp.Name, &rp.Duration, &rp.DurationNs, &rp.IsDefault, &rp.CreatedAt); err != nil {
			return err
		}
		rs.policies[rp.Name] = &rp
	}
	log.Printf("Loaded %d retention policies", len(rs.policies))
	return rows.Err()
}

// RunSyncLoop runs the background retention policy sync loop
func (rs *RetentionStore) RunSyncLoop(intervalMinutes int) {
	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
	defer ticker.Stop()

	log.Printf("Retention sync loop started (every %d minutes)", intervalMinutes)

	for {
		select {
		case <-ticker.C:
			if err := rs.SyncToTimescaleDB(); err != nil {
				log.Printf("Retention sync error: %v", err)
			}
		case <-rs.ctx.Done():
			log.Println("Retention sync loop stopped")
			return
		}
	}
}

// CreatePolicy creates or updates a retention policy
func (rs *RetentionStore) CreatePolicy(name string, duration string, isDefault bool) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Parse duration
	durationNs, err := parseDuration(duration)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", duration, err)
	}

	// If is_default is true, clear other defaults first
	if isDefault {
		if _, err := rs.pool.Exec(rs.ctx,
			"UPDATE _retention_policy SET is_default = FALSE WHERE is_default = TRUE"); err != nil {
			return fmt.Errorf("clear default: %w", err)
		}
	}

	// Upsert policy
	sql := `INSERT INTO _retention_policy (measurement, duration, duration_ns, is_default)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (measurement) DO UPDATE
		SET duration = EXCLUDED.duration,
		    duration_ns = EXCLUDED.duration_ns,
		    is_default = EXCLUDED.is_default`
	if _, err := rs.pool.Exec(rs.ctx, sql, name, duration, durationNs, isDefault); err != nil {
		return fmt.Errorf("upsert policy: %w", err)
	}

	// Update cache
	rs.policies[name] = &RetentionPolicy{
		Name:       name,
		Duration:   duration,
		DurationNs: durationNs,
		IsDefault:  isDefault,
		CreatedAt:  time.Now(),
	}

	log.Printf("Created/updated retention policy: %s duration=%s default=%v", name, duration, isDefault)
	return nil
}

// AlterPolicy updates an existing retention policy
func (rs *RetentionStore) AlterPolicy(name string, duration string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Check if exists
	existing, ok := rs.policies[name]
	if !ok {
		return fmt.Errorf("retention policy %q not found", name)
	}

	// Parse duration
	durationNs, err := parseDuration(duration)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", duration, err)
	}

	// Update
	sql := `UPDATE _retention_policy SET duration = $2, duration_ns = $3 WHERE measurement = $1`
	if _, err := rs.pool.Exec(rs.ctx, sql, name, duration, durationNs); err != nil {
		return fmt.Errorf("update policy: %w", err)
	}

	// Update cache
	existing.Duration = duration
	existing.DurationNs = durationNs

	log.Printf("Altered retention policy: %s duration=%s", name, duration)
	return nil
}

// DropPolicy removes a retention policy
func (rs *RetentionStore) DropPolicy(name string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Check if exists
	if _, ok := rs.policies[name]; !ok {
		return fmt.Errorf("retention policy %q not found", name)
	}

	// Delete from table
	if _, err := rs.pool.Exec(rs.ctx,
		"DELETE FROM _retention_policy WHERE measurement = $1", name); err != nil {
		return fmt.Errorf("delete policy: %w", err)
	}

	// Update cache
	delete(rs.policies, name)

	log.Printf("Dropped retention policy: %s", name)
	return nil
}

// ShowPolicies returns all retention policies
func (rs *RetentionStore) ShowPolicies() []RetentionPolicy {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	result := make([]RetentionPolicy, 0, len(rs.policies))
	for _, rp := range rs.policies {
		result = append(result, *rp)
	}
	return result
}

// SyncToTimescaleDB applies retention policies to TimescaleDB hypertables
func (rs *RetentionStore) SyncToTimescaleDB() error {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if len(rs.policies) == 0 {
		return nil
	}

	log.Printf("Syncing %d retention policies to TimescaleDB...", len(rs.policies))

	for name, rp := range rs.policies {
		// Skip infinite duration
		if rp.DurationNs == 0 {
			continue
		}

		// Convert nanoseconds to interval
		durationSec := rp.DurationNs / int64(time.Second)
		interval := fmt.Sprintf("%d seconds", durationSec)

		// Check if hypertable exists
		var hypertableExists bool
		err := rs.pool.QueryRow(rs.ctx,
			"SELECT EXISTS(SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = $1)",
			name).Scan(&hypertableExists)
		if err != nil {
			log.Printf("Check hypertable %s: %v", name, err)
			continue
		}
		if !hypertableExists {
			log.Printf("Hypertable %s not found, skipping retention policy", name)
			continue
		}

		// Remove existing policy (if any)
		_, _ = rs.pool.Exec(rs.ctx,
			fmt.Sprintf("SELECT remove_retention_policy('%s')", name))

		// Add new policy
		_, err = rs.pool.Exec(rs.ctx,
			fmt.Sprintf("SELECT add_retention_policy('%s', INTERVAL '%s', if_not_exists => true)", name, interval))
		if err != nil {
			log.Printf("Add retention policy for %s: %v", name, err)
			continue
		}

		log.Printf("Applied retention policy: %s -> %s", name, interval)
	}

	rs.lastSync = time.Now()
	return nil
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
