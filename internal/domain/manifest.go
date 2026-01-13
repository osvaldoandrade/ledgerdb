package domain

import "time"

const ManifestVersion = 2

type Manifest struct {
	Version      int
	Name         string
	CreatedAt    time.Time
	StreamLayout StreamLayout
	HistoryMode  HistoryMode
}

func NewManifest(name string, createdAt time.Time) Manifest {
	return Manifest{
		Version:      ManifestVersion,
		Name:         name,
		CreatedAt:    createdAt.UTC(),
		StreamLayout: DefaultStreamLayout,
		HistoryMode:  DefaultHistoryMode,
	}
}

func (m Manifest) WithDefaults() Manifest {
	if m.Version == 0 {
		m.Version = 1
	}
	if m.StreamLayout == "" {
		if m.Version >= 2 {
			m.StreamLayout = DefaultStreamLayout
		} else {
			m.StreamLayout = StreamLayoutFlat
		}
	}
	if m.HistoryMode == "" {
		m.HistoryMode = DefaultHistoryMode
	}
	return m
}
