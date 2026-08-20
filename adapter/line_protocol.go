package adapter

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LineRecord represents a single parsed Line Protocol record
type LineRecord struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   time.Time
}

// ParseLineProtocol parses InfluxDB Line Protocol text into records.
// Format: measurement,tag=val,tag=val field=val,field=val timestamp
func ParseLineProtocol(body string) ([]LineRecord, error) {
	lines := strings.Split(strings.TrimSpace(body), "\n")
	records := make([]LineRecord, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		rec, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("parse line %q: %w", line, err)
		}
		records = append(records, rec)
	}

	return records, nil
}

func parseLine(line string) (LineRecord, error) {
	// Split by unescaped spaces: measurement+tags SP fields [SP timestamp]
	parts := splitLineParts(line)
	if len(parts) < 2 {
		return LineRecord{}, fmt.Errorf("need at least measurement and fields")
	}

	// Parse measurement and tags
	measurement, tags, err := parseMeasurementAndTags(parts[0])
	if err != nil {
		return LineRecord{}, err
	}

	// Parse fields
	fields, err := parseFields(parts[1])
	if err != nil {
		return LineRecord{}, err
	}

	// Parse optional timestamp
	ts := time.Now().UTC()
	if len(parts) >= 3 {
		nsec, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return LineRecord{}, fmt.Errorf("invalid timestamp: %w", err)
		}
		ts = time.Unix(0, nsec).UTC()
	}

	return LineRecord{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Timestamp:   ts,
	}, nil
}

// splitLineParts splits a line protocol line by unescaped spaces
func splitLineParts(line string) []string {
	var parts []string
	var current strings.Builder
	escaped := false

	for i := 0; i < len(line); i++ {
		c := line[i]
		if escaped {
			current.WriteByte(c)
			escaped = false
			continue
		}
		if c == '\\' {
			current.WriteByte(c)
			escaped = true
			continue
		}
		if c == ' ' {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteByte(c)
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

func parseMeasurementAndTags(s string) (string, map[string]string, error) {
	parts := strings.Split(s, ",")
	measurement := parts[0]
	if measurement == "" {
		return "", nil, fmt.Errorf("empty measurement name")
	}

	tags := make(map[string]string)
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return "", nil, fmt.Errorf("invalid tag: %s", part)
		}
		tags[kv[0]] = kv[1]
	}

	return measurement, tags, nil
}

func parseFields(s string) (map[string]interface{}, error) {
	fields := make(map[string]interface{})

	// Split by unescaped commas
	fieldParts := splitLineFields(s)

	for _, part := range fieldParts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid field: %s", part)
		}

		key := kv[0]
		value, err := parseFieldValue(kv[1])
		if err != nil {
			return nil, fmt.Errorf("invalid field value for %s: %w", key, err)
		}
		fields[key] = value
	}

	return fields, nil
}

func splitLineFields(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' && (i == 0 || s[i-1] != '\\') {
			inQuote = !inQuote
			current.WriteByte(c)
			continue
		}
		if c == ',' && !inQuote {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

func parseFieldValue(s string) (interface{}, error) {
	if len(s) == 0 {
		return nil, fmt.Errorf("empty value")
	}

	// Integer: ends with 'i'
	if strings.HasSuffix(s, "i") {
		n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	}

	// Unsigned integer: ends with 'u'
	if strings.HasSuffix(s, "u") {
		n, err := strconv.ParseUint(s[:len(s)-1], 10, 64)
		if err != nil {
			return nil, err
		}
		return int64(n), nil
	}

	// String: quoted
	if s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1], nil
	}

	// Boolean
	lower := strings.ToLower(s)
	if lower == "true" || lower == "t" {
		return true, nil
	}
	if lower == "false" || lower == "f" {
		return false, nil
	}

	// Float (default)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, err
	}
	return f, nil
}
