package cmd

import (
	"context"
	"flag"
	"fmt"

	"github.com/AviaOne/tenderseed/internal/tenderseed"

	"github.com/google/subcommands"
)

// VersionArgs for the version command
type VersionArgs struct{}

// Name returns the command name
func (*VersionArgs) Name() string { return "version" }

// Synopsis returns a summary for the command
func (*VersionArgs) Synopsis() string { return "show the version announced to peers" }

// Usage returns full usage for the command
func (*VersionArgs) Usage() string {
	return `version

Show the version this binary announces to peers during the handshake.
`
}

// SetFlags initializes any command flags
func (args *VersionArgs) SetFlags(flagSet *flag.FlagSet) {
}

// Execute runs the command
func (args *VersionArgs) Execute(_ context.Context, flagSet *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fmt.Println(tenderseed.Version)
	return subcommands.ExitSuccess
}
