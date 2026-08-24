package main

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	influxHost = "http://localhost:8086"
	proxyHost  = "http://localhost:8087"
	db         = "stress_test"
	total      = 100000
	batchSize  = 1000
	runs       = 5
)

var (
	servers = []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}
	regions = []string{"华东", "华南", "华北", "西南"}
	client  = &http.Client{Timeout: 30 * time.Second}

	queryList = []struct{ name, q string }{
		{"count", `SELECT count("online_count") FROM "server_online"`},
		{"mean GROUP BY time(1h)", `SELECT mean("online_count") FROM "server_online" GROUP BY time(1h)`},
		{"sum GROUP BY time(30m)", `SELECT sum("online_count") FROM "server_online" GROUP BY time(30m)`},
		{"last GROUP BY server_id", `SELECT last("online_count") FROM "server_online" GROUP BY "server_id"`},
		{"mean+max+min time(10m)", `SELECT mean("online_count"), max("online_count"), min("online_count") FROM "server_online" GROUP BY time(10m)`},
		{"subquery sum(last)", `SELECT sum("val") FROM (SELECT last("online_count") AS val FROM "server_online" GROUP BY "server_id")`},
		{"GROUP BY tag+time", `SELECT mean("online_count") FROM "server_online" GROUP BY "server_id", time(1h)`},
		{"SHOW TAG VALUES", `SHOW TAG VALUES FROM "server_online" WITH KEY = "server_id"`},
	}
)

type WriteResult struct {
	TotalTime time.Duration
	Rate      float64
	BatchP50  time.Duration
	BatchP99  time.Duration
}

type ReadResult struct {
	Queries   []QueryResult
	TotalTime time.Duration
}

type QueryResult struct {
	Name string
	Time time.Duration
}

func writeReq(host, path, body string) (int, time.Duration) {
	u := host + path
	t0 := time.Now()
	resp, err := client.Post(u, "text/plain", strings.NewReader(body))
	elapsed := time.Since(t0)
	if err != nil {
		return 0, elapsed
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, elapsed
}

func execQuery(host, database, q string) (int, string, time.Duration) {
	data := url.Values{"db": {database}, "q": {q}, "epoch": {"ms"}}
	t0 := time.Now()
	resp, err := client.Post(host+"/query", "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
	elapsed := time.Since(t0)
	if err != nil {
		return 0, "", elapsed
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), elapsed
}

// generateBatch generates line protocol data.
// timeSpanMinutes controls the total time span (0 = auto: 5s interval → ~5.8 days).
// When timeSpanMinutes > 0, intervals are compressed to fit within that window.
func generateBatch(start, count int, timeSpanMinutes int) string {
	var b strings.Builder
	var intervalNs int64
	if timeSpanMinutes > 0 {
		// Spread data evenly across the specified time span
		intervalNs = int64(timeSpanMinutes) * 60_000_000_000 / int64(count)
	} else {
		intervalNs = 5_000_000_000 // 5s interval → 100k points = ~5.8 days
	}
	baseTs := time.Now().Add(-time.Duration(int64(count)*intervalNs) - time.Minute).UnixNano()
	for i := 0; i < count; i++ {
		idx := start + i
		server := servers[idx%len(servers)]
		region := regions[idx%len(regions)]
		ts := baseTs + int64(i)*intervalNs
		online := rand.Intn(4900) + 100
		cpu := 5.0 + rand.Float64()*90.0
		fmt.Fprintf(&b, "server_online,server_id=%s,region=%s online_count=%di,cpu_usage=%.1f %d\n",
			server, region, online, cpu, ts)
	}
	return b.String()
}

func setupDB(host, database string) {
	// Drop and recreate the entire database to clear ALL old chunks/hypertables
	data := url.Values{"q": {fmt.Sprintf(`DROP DATABASE "%s"`, database)}}
	resp, _ := client.Post(host+"/query", "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
	if resp != nil {
		resp.Body.Close()
	}
	time.Sleep(500 * time.Millisecond)
	data = url.Values{"q": {fmt.Sprintf(`CREATE DATABASE "%s"`, database)}}
	resp, _ = client.Post(host+"/query", "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
	if resp != nil {
		resp.Body.Close()
	}
	time.Sleep(500 * time.Millisecond)
}

// ─── Write ───────────────────────────────────────────────────────────

func testWrite(host, label string) WriteResult {
	fmt.Printf("\n%s\n", strings.Repeat("=", 56))
	fmt.Printf("WRITE: %s\n", label)
	fmt.Printf("%s\n", strings.Repeat("=", 56))

	setupDB(host, db)

	batches := total / batchSize
	batchTimes := make([]time.Duration, 0, batches)
	totalStart := time.Now()

	for b := 0; b < batches; b++ {
		data := generateBatch(b*batchSize, batchSize, 30) // 30min span → ~30 chunks with 1m shard
		status, elapsed := writeReq(host, "/write?db="+db, data)
		batchTimes = append(batchTimes, elapsed)

		if status != 204 {
			fmt.Printf("  Batch %d/%d: HTTP %d FAIL\n", b+1, batches, status)
		} else if (b+1)%20 == 0 || b == batches-1 {
			rate := float64(batchSize) / elapsed.Seconds()
			fmt.Printf("  Batch %d/%d: %v (%.0f pts/s)\n", b+1, batches, elapsed.Round(time.Millisecond), rate)
		}
	}

	totalElapsed := time.Since(totalStart)
	sort.Slice(batchTimes, func(i, j int) bool { return batchTimes[i] < batchTimes[j] })

	p50 := batchTimes[len(batchTimes)/2]
	p99 := batchTimes[len(batchTimes)*99/100]

	fmt.Printf("  Total: %v  Rate: %.0f pts/s  p50: %v  p99: %v\n",
		totalElapsed.Round(time.Millisecond), float64(total)/totalElapsed.Seconds(),
		p50.Round(time.Microsecond), p99.Round(time.Microsecond))

	return WriteResult{
		TotalTime: totalElapsed,
		Rate:      float64(total) / totalElapsed.Seconds(),
		BatchP50:  p50,
		BatchP99:  p99,
	}
}

// ─── Read ────────────────────────────────────────────────────────────

func testRead(host, label string) ReadResult {
	fmt.Printf("\n%s\n", strings.Repeat("=", 56))
	fmt.Printf("READ: %s\n", label)
	fmt.Printf("%s\n", strings.Repeat("=", 56))

	var results []QueryResult
	for _, qc := range queryList {
		// warmup
		execQuery(host, db, qc.q)

		times := make([]time.Duration, runs)
		for i := 0; i < runs; i++ {
			_, _, elapsed := execQuery(host, db, qc.q)
			times[i] = elapsed
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		median := times[runs/2]

		results = append(results, QueryResult{Name: qc.name, Time: median})
		fmt.Printf("  %-36s %8v\n", qc.name, median.Round(time.Microsecond))
	}

	var totalTime time.Duration
	for _, r := range results {
		totalTime += r.Time
	}
	fmt.Printf("  %-36s %8v\n", "TOTAL", totalTime.Round(time.Microsecond))

	return ReadResult{Queries: results, TotalTime: totalTime}
}

// ─── Proxy Compression Setup ─────────────────────────────────────────

// setupCompressionRP creates a retention policy with short compress_after before writing.
// RP: 1h duration, 1m SHARD DURATION → chunk=1m, compress_after=10s, schedule=1min.
func setupCompressionRP() {
	fmt.Printf("\n%s\n", strings.Repeat("=", 56))
	fmt.Println("SETUP: Compression RP (1h + SHARD 1m) before writing")
	fmt.Printf("%s\n", strings.Repeat("=", 56))

	q := `CREATE RETENTION POLICY "compress_test" ON "stress_test" DURATION 1h REPLICATION 1 SHARD DURATION 1m DEFAULT`
	_, body, _ := execQuery(proxyHost, db, q)
	fmt.Printf("  CREATE RP: %s\n", strings.TrimSpace(body))

	// Verify
	_, body, _ = execQuery(proxyHost, db, `SHOW RETENTION POLICIES`)
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "compress_test") || strings.Contains(line, "name") {
			fmt.Printf("    %s\n", line)
		}
	}
	time.Sleep(1 * time.Second)
}

// waitForCompression waits for TimescaleDB background job to compress old chunks.
// With compress_after=10s and schedule_interval=1min, ~90s is enough for most chunks.
func waitForCompression(seconds int) {
	fmt.Printf("\n%s\n", strings.Repeat("=", 56))
	fmt.Printf("WAITING: %ds for TimescaleDB compression job...\n", seconds)
	fmt.Printf("%s\n", strings.Repeat("=", 56))

	for i := 0; i < seconds; i += 10 {
		time.Sleep(10 * time.Second)
		elapsed := i + 10
		if elapsed > seconds {
			elapsed = seconds
		}
		fmt.Printf("  [%d/%ds] ...\n", elapsed, seconds)
	}
}

// ─── Compare ─────────────────────────────────────────────────────────

func compare(wInflux, wProxy WriteResult, rUncompressed, rCompressed ReadResult, rInflux ReadResult) {
	fmt.Printf("\n%s\n", strings.Repeat("=", 72))
	fmt.Println("COMPARISON SUMMARY")
	fmt.Printf("%s\n", strings.Repeat("=", 72))

	// Write
	fmt.Printf("\n%-36s %10s %10s %8s\n", "Write Metric", "InfluxDB", "Proxy", "Ratio")
	fmt.Println(strings.Repeat("-", 72))
	fmt.Printf("%-36s %10v %10v %7.2fx\n", "Total time",
		wInflux.TotalTime.Round(time.Millisecond), wProxy.TotalTime.Round(time.Millisecond),
		float64(wProxy.TotalTime)/float64(wInflux.TotalTime))
	fmt.Printf("%-36s %10.0f %10.0f %7.2fx\n", "Rate (pts/s)",
		wInflux.Rate, wProxy.Rate, wInflux.Rate/wProxy.Rate)
	fmt.Printf("%-36s %10v %10v %7.2fx\n", "Batch p50",
		wInflux.BatchP50.Round(time.Microsecond), wProxy.BatchP50.Round(time.Microsecond),
		float64(wProxy.BatchP50)/float64(wInflux.BatchP50))

	// Read: InfluxDB vs Proxy (uncompressed vs compressed)
	fmt.Printf("\n%-36s %10s %10s %10s %8s\n", "Query", "InfluxDB", "Uncompr.", "Compr.", "Compr/Uncompr")
	fmt.Println(strings.Repeat("-", 72))

	influxMap := make(map[string]time.Duration)
	for _, q := range rInflux.Queries {
		influxMap[q.Name] = q.Time
	}
	uncompMap := make(map[string]time.Duration)
	for _, q := range rUncompressed.Queries {
		uncompMap[q.Name] = q.Time
	}
	compMap := make(map[string]time.Duration)
	for _, q := range rCompressed.Queries {
		compMap[q.Name] = q.Time
	}

	for _, qc := range queryList {
		iT := influxMap[qc.name]
		uT := uncompMap[qc.name]
		cT := compMap[qc.name]
		ratio := float64(cT) / float64(uT)
		if uT == 0 {
			ratio = 0
		}
		marker := ""
		if ratio < 0.95 {
			marker = " ← faster"
		} else if ratio > 1.05 {
			marker = " ← slower"
		}
		fmt.Printf("%-36s %10v %10v %10v %7.2fx%s\n", qc.name,
			iT.Round(time.Microsecond), uT.Round(time.Microsecond), cT.Round(time.Microsecond), ratio, marker)
	}

	fmt.Println(strings.Repeat("-", 72))
	ratio := float64(rCompressed.TotalTime) / float64(rUncompressed.TotalTime)
	fmt.Printf("%-36s %10v %10v %10v %7.2fx\n", "TOTAL",
		rInflux.TotalTime.Round(time.Microsecond),
		rUncompressed.TotalTime.Round(time.Microsecond),
		rCompressed.TotalTime.Round(time.Microsecond), ratio)
}

func main() {
	fmt.Println("influx2tsdb-proxy Stress Test (Go)")
	fmt.Printf("Data: %d points, Batch: %d, Runs: %d\n", total, batchSize, runs)
	fmt.Printf("InfluxDB: %s, Proxy: %s\n", influxHost, proxyHost)

	// 1. Write to InfluxDB (baseline, no compression)
	wInflux := testWrite(influxHost, "InfluxDB 8086")

	// 2. Write to proxy (creates database and hypertable)
	wProxy := testWrite(proxyHost, "Proxy 8087")

	// 3. Setup compression RP on proxy AFTER writing
	//    RP: 1h, SHARD DURATION 1m → chunk=1m, compress_after=10s, schedule=1min
	setupCompressionRP()

	// 4. Read — uncompressed (just written, chunks not yet compressed)
	rInflux := testRead(influxHost, "InfluxDB 8086")
	rUncompressed := testRead(proxyHost, "Proxy 8087 (uncompressed)")

	// 5. Wait for TimescaleDB compression job to compress old chunks
	//    compress_after=10s, schedule=1min → ~90s is enough for most chunks
	waitForCompression(90)

	// 6. Read — compressed
	rCompressed := testRead(proxyHost, "Proxy 8087 (compressed)")

	// 7. Compare
	compare(wInflux, wProxy, rUncompressed, rCompressed, rInflux)

	// 8. Cleanup
	fmt.Println("\n--- Cleanup ---")
	data := url.Values{"q": {fmt.Sprintf(`DROP DATABASE "%s"`, db)}}
	for _, h := range []string{influxHost, proxyHost} {
		resp, _ := client.Post(h+"/query", "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
		if resp != nil {
			resp.Body.Close()
		}
	}
	fmt.Println("Done.")
}
