package domain

import (
	"fmt"
	"strings"
)

type StreamLayout string

const (
	StreamLayoutFlat    StreamLayout = "flat"
	StreamLayoutSharded StreamLayout = "sharded"
)

const DefaultStreamLayout = StreamLayoutSharded

func (layout StreamLayout) IsValid() bool {
	return layout == StreamLayoutFlat || layout == StreamLayoutSharded
}

func ParseStreamLayout(value string) (StreamLayout, error) {
	parsed := StreamLayout(strings.TrimSpace(value))
	if parsed == "" {
		return "", fmt.Errorf("stream layout is required")
	}
	if !parsed.IsValid() {
		return "", fmt.Errorf("invalid stream layout: %s", value)
	}
	return parsed, nil
}

func NormalizeStreamLayout(layout StreamLayout) StreamLayout {
	if layout.IsValid() {
		return layout
	}
	return DefaultStreamLayout
}

type HistoryMode string

const (
	HistoryModeAppend HistoryMode = "append"
	HistoryModeAmend  HistoryMode = "amend"
)

const DefaultHistoryMode = HistoryModeAppend

func (mode HistoryMode) IsValid() bool {
	return mode == HistoryModeAppend || mode == HistoryModeAmend
}

func ParseHistoryMode(value string) (HistoryMode, error) {
	parsed := HistoryMode(strings.TrimSpace(value))
	if parsed == "" {
		return "", fmt.Errorf("history mode is required")
	}
	if !parsed.IsValid() {
		return "", fmt.Errorf("invalid history mode: %s", value)
	}
	return parsed, nil
}

func NormalizeHistoryMode(mode HistoryMode) HistoryMode {
	if mode.IsValid() {
		return mode
	}
	return DefaultHistoryMode
}
