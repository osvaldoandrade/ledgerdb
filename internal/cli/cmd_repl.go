package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newReplCmd(opts *RootOptions) *cobra.Command {
	var scriptPath string
	cmd := &cobra.Command{
		Use:   "repl",
		Short: "Start an interactive REPL",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(scriptPath) == "" {
				return runRepl(cmd, opts, os.Stdin, true)
			}
			file, err := os.Open(scriptPath)
			if err != nil {
				return fmt.Errorf("open script: %w", err)
			}
			defer func() {
				_ = file.Close()
			}()
			return runRepl(cmd, opts, file, false)
		},
	}
	cmd.Flags().StringVar(&scriptPath, "script", "", "Read commands from a file non-interactively")
	return cmd
}

func runRepl(cmd *cobra.Command, opts *RootOptions, input io.Reader, interactive bool) error {
	out := cmd.OutOrStdout()
	errOut := cmd.ErrOrStderr()
	prompt := replPrompt(opts.RepoPath)

	if interactive {
		if _, err := fmt.Fprintln(out, "LedgerDB REPL. Type \\h for help, \\q or exit to quit."); err != nil {
			return err
		}
	}

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for {
		if interactive {
			if _, err := fmt.Fprint(out, prompt); err != nil {
				return err
			}
		}
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if isReplExitCommand(line) {
			break
		}
		if line == "\\h" || line == "help" {
			printReplHelp(out)
			continue
		}

		args, err := splitReplLine(line)
		if err != nil {
			fmt.Fprintf(errOut, "parse error: %v\n", err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		if err := dispatchReplCommand(cmd, opts, args); err != nil {
			_ = writeCLIError(errOut, NormalizeError(err), opts.JSONOutput)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	return nil
}

func replPrompt(repoPath string) string {
	repoPath = strings.TrimSpace(repoPath)
	if repoPath == "" || repoPath == "." {
		return "ledgerdb> "
	}
	return repoPath + "> "
}

func isReplExitCommand(line string) bool {
	switch line {
	case "\\q", "exit", "quit":
		return true
	}
	return false
}

func printReplHelp(out io.Writer) {
	fmt.Fprintln(out, "Commands:")
	fmt.Fprintln(out, "  Any ledgerdb subcommand, e.g. 'doc put users u1 --payload {...}'")
	fmt.Fprintln(out, "  \\h, help   Show this help")
	fmt.Fprintln(out, "  \\q, exit   Quit the REPL")
}

func dispatchReplCommand(parent *cobra.Command, opts *RootOptions, args []string) error {
	root := newRootCmd()
	root.SetOut(parent.OutOrStdout())
	root.SetErr(parent.ErrOrStderr())
	root.SetArgs(replInheritedArgs(opts, args))
	root.SilenceErrors = true
	root.SilenceUsage = true
	return root.ExecuteContext(parent.Context())
}

// replInheritedArgs prepends the parent REPL's persistent flags to the
// per-iteration command args unless the user already supplied them.
func replInheritedArgs(opts *RootOptions, args []string) []string {
	prepended := make([]string, 0, len(args)+4)
	if !replArgsContainFlag(args, "--repo") && strings.TrimSpace(opts.RepoPath) != "" {
		prepended = append(prepended, "--repo", opts.RepoPath)
	}
	if opts.JSONOutput && !replArgsContainFlag(args, "--json") {
		prepended = append(prepended, "--json")
	}
	prepended = append(prepended, args...)
	return prepended
}

func replArgsContainFlag(args []string, name string) bool {
	for _, a := range args {
		if a == name || strings.HasPrefix(a, name+"=") {
			return true
		}
	}
	return false
}

// splitReplLine splits a single REPL line into argv-style tokens.
// Supports double-quoted and single-quoted strings (with no escape handling
// inside single quotes) and backslash-escapes outside quotes and inside double
// quotes.
func splitReplLine(line string) ([]string, error) {
	var args []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	hasToken := false

	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
				continue
			}
			current.WriteByte(c)
			hasToken = true
		case inDouble:
			if c == '\\' && i+1 < len(line) {
				next := line[i+1]
				if next == '"' || next == '\\' {
					current.WriteByte(next)
					i++
					hasToken = true
					continue
				}
				current.WriteByte(c)
				hasToken = true
				continue
			}
			if c == '"' {
				inDouble = false
				continue
			}
			current.WriteByte(c)
			hasToken = true
		default:
			switch c {
			case '\'':
				inSingle = true
				hasToken = true
			case '"':
				inDouble = true
				hasToken = true
			case '\\':
				if i+1 < len(line) {
					current.WriteByte(line[i+1])
					i++
					hasToken = true
				}
			case ' ', '\t':
				if hasToken {
					args = append(args, current.String())
					current.Reset()
					hasToken = false
				}
			default:
				current.WriteByte(c)
				hasToken = true
			}
		}
	}
	if inSingle || inDouble {
		return nil, fmt.Errorf("unterminated quoted string")
	}
	if hasToken {
		args = append(args, current.String())
	}
	return args, nil
}
