package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/google/subcommands"

	"github.com/AviaOne/tenderseed/internal/tenderseed"
)

// StartArgs for the start command
type StartArgs struct {
	HomeDir    string
	SeedConfig tenderseed.Config
}

// Name returns the command name
func (*StartArgs) Name() string { return "start" }

// Synopsis returns a summary for the command
func (*StartArgs) Synopsis() string { return "start tenderseed" }

// Usage returns full usage for the command
func (*StartArgs) Usage() string {
	return `start

start the tenderseed
`
}

// SetFlags initializes any command flags
func (args *StartArgs) SetFlags(flagSet *flag.FlagSet) {
}

// Execute runs the command.
//
// This file names no stack. NewNode reads the configured one and hands back a
// Node; the logger, the transport and the signal handler all live behind that
// interface, because they are the parts the two stacks do not share.
func (args *StartArgs) Execute(_ context.Context, flagSet *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	node, err := tenderseed.NewNode(args.HomeDir, args.SeedConfig, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenderseed:", err)
		return subcommands.ExitFailure
	}

	if err := node.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "tenderseed:", err)
		return subcommands.ExitFailure
	}

	node.TrapSignal()
	node.Wait()
	return subcommands.ExitSuccess
}
