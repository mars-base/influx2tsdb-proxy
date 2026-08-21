package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"influxdb-tsdb-proxy/adapter"

	"github.com/jackc/pgx/v5/pgxpool"
)

var version = "dev"

func main() {
	pgConn := flag.String("pg", "", "PostgreSQL connection string (e.g. postgres://user:pass@host:port/db?sslmode=disable)")
	port := flag.String("port", "8087", "HTTP listen port")
	dbName := flag.String("db", "", "InfluxDB database name to expose (optional, defaults to PostgreSQL database name)")
	poolSize := flag.Int("pool", 10, "Connection pool size")
	verbose := flag.Bool("verbose", false, "Enable verbose logging (SQL statements and query details)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("influx2tsdb-proxy version:", version)
		os.Exit(0)
	}

	dsn := *pgConn
	if dsn == "" {
		log.Fatal("Required flag: -pg <connection-string>\n  Example: -pg postgres://user:pass@host:port/db?sslmode=disable")
	}

	// Use specified db name or extract from PostgreSQL connection string
	influxDBName := *dbName
	if influxDBName == "" {
		influxDBName = parseDBName(dsn)
	}
	if influxDBName == "" {
		log.Fatalf("Cannot parse database name from connection string")
	}

	ctx := context.Background()

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("Failed to parse pool config: %v", err)
	}
	poolConfig.MaxConns = int32(*poolSize)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to database: %v\n  DSN: %s\n  Please check:\n    - Connection string format\n    - Host/port accessibility\n    - Database name exists\n    - User/password correct", err, dsn)
	}
	log.Printf("Connected to PostgreSQL database, exposing as InfluxDB '%s'", influxDBName)

	// Check PostgreSQL version and required extensions
	var pgVersion string
	err = pool.QueryRow(ctx, "SELECT version()").Scan(&pgVersion)
	if err != nil {
		log.Fatalf("Failed to query PostgreSQL version: %v", err)
	}
	log.Printf("PostgreSQL: %s", pgVersion)

	// Ensure TimescaleDB extension
	var tsdbVersion string
	err = pool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&tsdbVersion)
	if err != nil {
		// Extension not found, try to create
		log.Printf("TimescaleDB extension not found, attempting to create...")
		if _, cerr := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS timescaledb"); cerr != nil {
			log.Fatalf("Failed to create TimescaleDB extension: %v\n"+
				"  This adapter requires TimescaleDB.\n"+
				"  Please install it manually: https://docs.timescale.com/latest/getting-started", cerr)
		}
		// Re-check after creation
		err = pool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&tsdbVersion)
		if err != nil {
			log.Fatalf("TimescaleDB extension created but version query failed: %v", err)
		}
		log.Printf("TimescaleDB extension created: v%s", tsdbVersion)
	} else {
		log.Printf("TimescaleDB extension: v%s", tsdbVersion)
	}

	// Check for pg_stat_statements (optional but useful)
	var hasStatStatements bool
	pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')").Scan(&hasStatStatements)
	if hasStatStatements {
		log.Printf("pg_stat_statements: enabled")
	}

	// Initialize metadata store
	meta := adapter.NewMetaStore(ctx, pool)
	if err := meta.Initialize(); err != nil {
		log.Fatalf("Failed to initialize metadata: %v", err)
	}

	// Initialize retention policy store
	retentionStore, err := adapter.NewRetentionStore(ctx, pool)
	if err != nil {
		log.Fatalf("Failed to initialize retention store: %v", err)
	}
	// Link retention store to meta store for chunk_time_interval
	meta.SetRetentionStore(retentionStore)
	// Initial sync of retention policies to TimescaleDB
	if err := retentionStore.SyncToTimescaleDB(); err != nil {
		log.Printf("Warning: initial retention sync failed: %v", err)
	}
	// Start background retention sync (every 5 minutes)
	go retentionStore.RunSyncLoop(5)

	// Set verbose logging
	adapter.Verbose = *verbose

	// HTTP routes
	http.HandleFunc("/ping", adapter.HandlePing)
	http.HandleFunc("/debug/vars", adapter.HandleDebugVars)
	http.HandleFunc("/write", adapter.HandleWrite(meta))
	http.HandleFunc("/query", adapter.HandleQuery(influxDBName, meta, retentionStore))

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		pool.Close()
		os.Exit(0)
	}()

	addr := fmt.Sprintf(":%s", *port)
	log.Printf("InfluxDB -> TimescaleDB proxy listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// parseDBName extracts the database name from a PostgreSQL connection string
func parseDBName(dsn string) string {
	// Try URL format: postgres://user:pass@host:port/dbname?params
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err == nil {
			name := strings.TrimPrefix(u.Path, "/")
			return name
		}
	}

	// Try key=value format: host=... dbname=mydb ...
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "dbname=") {
			return strings.TrimPrefix(part, "dbname=")
		}
	}

	return ""
}
