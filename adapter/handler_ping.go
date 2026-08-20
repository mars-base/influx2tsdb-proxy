package adapter

import (
	"net/http"
)

func HandlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Influxdb-Version", "1.11.8")
	w.WriteHeader(http.StatusNoContent)
}

func HandleDebugVars(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{}`))
}
