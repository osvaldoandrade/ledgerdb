package platform

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

type LogFormat string

const (
	LogFormatText LogFormat = "text"
	LogFormatJSON LogFormat = "json"
)

func ConfigureLogger(levelValue, formatValue string, out io.Writer) (*slog.Logger, error) {
	level, err := ParseLogLevel(levelValue)
	if err != nil {
		return nil, err
	}

	format, err := ParseLogFormat(formatValue)
	if err != nil {
		return nil, err
	}

	handlerOpts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch format {
	case LogFormatJSON:
		handler = slog.NewJSONHandler(out, handlerOpts)
	case LogFormatText:
		handler = slog.NewTextHandler(out, handlerOpts)
	default:
		return nil, fmt.Errorf("unsupported log format %q", formatValue)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger, nil
}

func ParseLogLevel(value string) (slog.Level, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid log level %q", value)
	}
}

func ParseLogFormat(value string) (LogFormat, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "", string(LogFormatText):
		return LogFormatText, nil
	case string(LogFormatJSON):
		return LogFormatJSON, nil
	default:
		return LogFormatText, fmt.Errorf("invalid log format %q", value)
	}
}
