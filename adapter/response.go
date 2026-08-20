package adapter

type InfluxDBResponse struct {
	Results []InfluxDBResult `json:"results"`
}

type InfluxDBResult struct {
	StatementID int              `json:"statement_id"`
	Series      []InfluxDBSeries `json:"series,omitempty"`
	Error       string           `json:"error,omitempty"`
}

type InfluxDBSeries struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
}
