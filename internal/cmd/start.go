package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/cometbft/cometbft/libs/log"
	cmtos "github.com/cometbft/cometbft/libs/os"
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

// Synopsis returns a ummary for the command
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

// Execute runs the command
func (args *StartArgs) Execute(_ context.Context, flagSet *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	logger := log.NewTMLogger(
		log.NewSyncWriter(os.Stdout),
	)

	seed, err := tenderseed.NewSeed(args.HomeDir, args.SeedConfig, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenderseed:", err)
		return subcommands.ExitFailure
	}

	cmtos.TrapSignal(seed.FilteredLogger, func() {
		seed.FilteredLogger.Info("shutting down...")
		if err := seed.Stop(); err != nil {
			seed.FilteredLogger.Error("error while shutting down", "err", err)
		}
	})

	if err := seed.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "tenderseed:", err)
		return subcommands.ExitFailure
	}

	seed.Wait()
	return subcommands.ExitSuccess
}
