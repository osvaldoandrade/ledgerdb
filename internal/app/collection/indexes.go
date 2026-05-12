package collection

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

// ParseIndexSpecs accepts either a comma-separated list of single-column
// index fields (legacy) or a JSON array. The JSON form may contain bare
// strings (treated as single-column indexes named after the field) or
// objects with {name, fields[], unique?}.
func ParseIndexSpecs(value string) ([]domain.IndexSpec, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}
	if trimmed[0] == '[' {
		return DecodeIndexesJSON([]byte(trimmed))
	}
	return parseCSVIndexes(trimmed), nil
}

// DecodeIndexesJSON parses a JSON array of index specs. Each element may
// be a bare string (single-column index named after the field) or an
// object with {name, fields[], unique?}.
func DecodeIndexesJSON(data []byte) ([]domain.IndexSpec, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode indexes: %w", err)
	}
	specs := make([]domain.IndexSpec, 0, len(raw))
	for i, item := range raw {
		spec, err := decodeIndexItem(item)
		if err != nil {
			return nil, fmt.Errorf("index[%d]: %w", i, err)
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

func decodeIndexItem(item json.RawMessage) (domain.IndexSpec, error) {
	trimmed := strings.TrimSpace(string(item))
	if len(trimmed) == 0 {
		return domain.IndexSpec{}, fmt.Errorf("empty entry")
	}
	if trimmed[0] == '"' {
		var field string
		if err := json.Unmarshal(item, &field); err != nil {
			return domain.IndexSpec{}, err
		}
		field = strings.TrimSpace(field)
		if field == "" {
			return domain.IndexSpec{}, fmt.Errorf("empty field")
		}
		return domain.IndexSpec{Name: field, Fields: []string{field}}, nil
	}
	var spec domain.IndexSpec
	if err := json.Unmarshal(item, &spec); err != nil {
		return domain.IndexSpec{}, err
	}
	return spec, nil
}

func parseCSVIndexes(value string) []domain.IndexSpec {
	parts := strings.Split(value, ",")
	specs := make([]domain.IndexSpec, 0, len(parts))
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field == "" {
			continue
		}
		specs = append(specs, domain.IndexSpec{Name: field, Fields: []string{field}})
	}
	return specs
}

// NormalizeIndexSpecs trims fields, derives names when omitted, dedups by
// name, validates each spec, and returns the result sorted by name. It is
// safe to call with nil/empty input.
func NormalizeIndexSpecs(specs []domain.IndexSpec) ([]domain.IndexSpec, error) {
	seen := make(map[string]struct{}, len(specs))
	out := make([]domain.IndexSpec, 0, len(specs))
	for i, raw := range specs {
		spec, err := normalizeIndexSpec(raw)
		if err != nil {
			return nil, fmt.Errorf("index[%d]: %w", i, err)
		}
		if _, dup := seen[spec.Name]; dup {
			continue
		}
		seen[spec.Name] = struct{}{}
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func normalizeIndexSpec(spec domain.IndexSpec) (domain.IndexSpec, error) {
	fields := make([]string, 0, len(spec.Fields))
	for _, f := range spec.Fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		fields = append(fields, f)
	}
	if len(fields) == 0 {
		return domain.IndexSpec{}, fmt.Errorf("at least one field is required")
	}
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		name = strings.Join(fields, "_")
	}
	return domain.IndexSpec{Name: name, Fields: fields, Unique: spec.Unique}, nil
}
