package cli

import (
	"errors"

	"github.com/spf13/pflag"
)

func Execute() int {
	cmd := newRootCmd()
	if err := cmd.Execute(); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return 0
		}
		asJSON := flagBool(cmd, "json")
		exitErr := NormalizeError(err)
		_ = writeCLIError(cmd.ErrOrStderr(), exitErr, asJSON)
		return exitErr.Code
	}
	return 0
}

func flagBool(cmd interface {
	Flags() *pflag.FlagSet
	PersistentFlags() *pflag.FlagSet
	InheritedFlags() *pflag.FlagSet
}, name string) bool {
	if value, ok := getBool(cmd.Flags(), name); ok {
		return value
	}
	if value, ok := getBool(cmd.PersistentFlags(), name); ok {
		return value
	}
	if value, ok := getBool(cmd.InheritedFlags(), name); ok {
		return value
	}
	return false
}

func getBool(flags *pflag.FlagSet, name string) (bool, bool) {
	if flags == nil {
		return false, false
	}
	if flags.Lookup(name) == nil {
		return false, false
	}
	value, err := flags.GetBool(name)
	if err != nil {
		return false, false
	}
	return value, true
}
