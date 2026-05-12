package domain

// IndexSpec describes a secondary index materialized by external indexers.
// Indexes may be single-column or composite (multiple fields), and may be
// unique. The wire format on disk supports both legacy strings (single
// column, name derived from the field) and richer objects.
type IndexSpec struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique,omitempty"`
}
