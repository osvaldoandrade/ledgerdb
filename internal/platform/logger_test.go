package platform

import (
	"testing"

	"log/slog"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input   string
		want    slog.Level
		wantErr bool
	}{
		{input: "", want: slog.LevelInfo},
		{input: "info", want: slog.LevelInfo},
		{input: "debug", want: slog.LevelDebug},
		{input: "warn", want: slog.LevelWarn},
		{input: "warning", want: slog.LevelWarn},
		{input: "error", want: slog.LevelError},
		{input: "bad", wantErr: true},
	}

	for _, tt := range tests {
		got, err := ParseLogLevel(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("expected error for %q", tt.input)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tt.input, err)
		}
		if got != tt.want {
			t.Fatalf("expected %v, got %v for %q", tt.want, got, tt.input)
		}
	}
}

func TestParseLogFormat(t *testing.T) {
	tests := []struct {
		input   string
		want    LogFormat
		wantErr bool
	}{
		{input: "", want: LogFormatText},
		{input: "text", want: LogFormatText},
		{input: "json", want: LogFormatJSON},
		{input: "bad", wantErr: true},
	}

	for _, tt := range tests {
		got, err := ParseLogFormat(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("expected error for %q", tt.input)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tt.input, err)
		}
		if got != tt.want {
			t.Fatalf("expected %v, got %v for %q", tt.want, got, tt.input)
		}
	}
}
