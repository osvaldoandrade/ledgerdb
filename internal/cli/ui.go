package cli

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

const (
	ansiReset   = "\x1b[0m"
	ansiBold    = "\x1b[1m"
	ansiDim     = "\x1b[2m"
	ansiRed     = "\x1b[38;5;196m"
	ansiGreen   = "\x1b[38;5;82m"
	ansiYellow  = "\x1b[38;5;214m"
	ansiMagenta = "\x1b[38;5;201m"
	ansiCyan    = "\x1b[38;5;51m"
)

type renderer struct {
	color bool
}

func newRenderer(out io.Writer, asJSON bool) renderer {
	return renderer{color: colorEnabled(out, asJSON)}
}

func colorEnabled(out io.Writer, asJSON bool) bool {
	if asJSON {
		return false
	}
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return false
	}
	return isTerminal(out)
}

func spinnerEnabled(out io.Writer, asJSON bool) bool {
	if asJSON {
		return false
	}
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return false
	}
	return isTerminal(out)
}

func isTerminal(out io.Writer) bool {
	file, ok := out.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	term := strings.TrimSpace(os.Getenv("TERM"))
	return term != "" && term != "dumb"
}

func (r renderer) wrap(code, value string) string {
	if !r.color || value == "" {
		return value
	}
	return code + value + ansiReset
}

func (r renderer) key(value string) string {
	return r.wrap(ansiBold+ansiCyan, value)
}

func (r renderer) ok(value string) string {
	return r.wrap(ansiBold+ansiGreen, value)
}

func (r renderer) warn(value string) string {
	return r.wrap(ansiBold+ansiYellow, value)
}

func (r renderer) err(value string) string {
	return r.wrap(ansiBold+ansiRed, value)
}

func (r renderer) accent(value string) string {
	return r.wrap(ansiBold+ansiMagenta, value)
}

func (r renderer) dim(value string) string {
	return r.wrap(ansiDim, value)
}

func (r renderer) bar(width int, ratio float64) string {
	if width <= 0 {
		width = 20
	}
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	filled := int(math.Round(ratio * float64(width)))
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("=", filled) + strings.Repeat("-", width-filled) + "]"
}

func withSpinner(ctx context.Context, out io.Writer, enabled bool, label string, fn func() error) error {
	if !enabled {
		return fn()
	}
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	frames := []string{"|", "/", "-", "\\"}
	ticker := time.NewTicker(120 * time.Millisecond)
	defer ticker.Stop()

	frame := 0
	for {
		select {
		case err := <-done:
			clearLine(out)
			return err
		case <-ticker.C:
			fmt.Fprintf(out, "\r%s %s", frames[frame%len(frames)], label)
			frame++
		case <-ctx.Done():
			// Continue until fn returns to avoid breaking output mid-flight.
		}
	}
}

func clearLine(out io.Writer) {
	fmt.Fprint(out, "\r\x1b[2K")
}
