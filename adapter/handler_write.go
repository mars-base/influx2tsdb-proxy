package adapter

import (
	"io"
	"log"
	"net/http"
)

func HandleWrite(meta *MetaStore, dbName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Use request db parameter, fall back to startup dbName
		effectiveDB := dbName
		if reqDB := r.URL.Query().Get("db"); reqDB != "" {
			effectiveDB = reqDB
		}

		if effectiveDB == "" {
			http.Error(w, "missing db parameter", http.StatusBadRequest)
			return
		}

		// Ensure database exists (auto-create on write)
		if err := meta.EnsureDatabase(effectiveDB); err != nil {
			log.Printf("[WRITE ERROR] ensure database %s: %v", effectiveDB, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		records, err := ParseLineProtocol(string(body))
		if err != nil {
			log.Printf("[WRITE ERROR] parse: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(records) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Group records by measurement
		byMeasurement := make(map[string][]LineRecord)
		for _, rec := range records {
			byMeasurement[rec.Measurement] = append(byMeasurement[rec.Measurement], rec)
		}

		for measurement, recs := range byMeasurement {
			// Use first record to determine schema
			if err := meta.EnsureTable(effectiveDB, measurement, recs[0].Tags, recs[0].Fields); err != nil {
				log.Printf("[WRITE ERROR] ensure table %s.%s: %v", effectiveDB, measurement, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := meta.InsertBatch(effectiveDB, measurement, recs); err != nil {
				log.Printf("[WRITE ERROR] insert %s.%s: %v", effectiveDB, measurement, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			log.Printf("[WRITE] %s.%s: %d records", effectiveDB, measurement, len(recs))
		}

		w.WriteHeader(http.StatusNoContent)
	}
}
