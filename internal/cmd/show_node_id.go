package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/AviaOne/tenderseed/internal/tenderseed"

	"github.com/google/subcommands"
)

// ShowNodeIDArgs for the show-node-id command
type ShowNodeIDArgs struct {
	HomeDir    string
	SeedConfig tenderseed.Config
}

// Name returns the command name
func (*ShowNodeIDArgs) Name() string { return "show-node-id" }

// Synopsis returns a summary for the command
func (*ShowNodeIDArgs) Synopsis() string { return "show the node id" }

// Usage returns full usage for the command
func (*ShowNodeIDArgs) Usage() string {
	return `show-node-id

Show the node id (public part of the node key), in the format of the
configured stack.

If a node key does not exist, it will be created and the id shown.
`
}

// SetFlags initializes any command flags
func (args *ShowNodeIDArgs) SetFlags(flagSet *flag.FlagSet) {
}

// Execute runs the command.
//
// This file names no stack: the identity format and the key file both belong
// to the stack, so the whole decision sits behind NodeID.
func (args *ShowNodeIDArgs) Execute(_ context.Context, flagSet *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	id, err := tenderseed.NodeID(args.HomeDir, args.SeedConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenderseed:", err)
		return subcommands.ExitFailure
	}

	fmt.Println(id)
	return subcommands.ExitSuccess
}
