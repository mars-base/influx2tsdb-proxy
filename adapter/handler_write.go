package adapter

import (
	"io"
	"log"
	"net/http"
)

func HandleWrite(meta *MetaStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// db parameter (for compatibility, we ignore it and use the configured database)
		_ = r.URL.Query().Get("db")

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
			if err := meta.EnsureTable(measurement, recs[0].Tags, recs[0].Fields); err != nil {
				log.Printf("[WRITE ERROR] ensure table %s: %v", measurement, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := meta.InsertBatch(measurement, recs); err != nil {
				log.Printf("[WRITE ERROR] insert %s: %v", measurement, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			log.Printf("[WRITE] %s: %d records", measurement, len(recs))
		}

		w.WriteHeader(http.StatusNoContent)
	}
}
