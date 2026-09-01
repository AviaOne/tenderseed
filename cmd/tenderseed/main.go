package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AviaOne/tenderseed/internal/cmd"
	"github.com/AviaOne/tenderseed/internal/tenderseed"

	"github.com/google/subcommands"
	"github.com/mitchellh/go-homedir"
)

func main() {
	userHomeDir, err := homedir.Dir()
	if err != nil {
		fail(err)
	}

	homeDir := flag.String("home", filepath.Join(userHomeDir, ".tenderseed"), "path to tenderseed home directory")
	configFile := flag.String("config", "config/config.toml", "path to config.toml, relative to home or absolute")
	chainID := flag.String("chain-id", "", "chain id")
	seeds := flag.String("seeds", "", "comma separated list of seeds")

	// parse top level flags
	flag.Parse()

	// Only the commands that open a home directory need a configuration.
	// Loading it for "version" or "help" would create that directory and
	// write a config.toml as a side effect of asking a question.
	seedConfig := &tenderseed.Config{}
	switch flag.Arg(0) {
	case "start", "show-node-id":
		configFilePath := *configFile
		if !filepath.IsAbs(configFilePath) {
			configFilePath = filepath.Join(*homeDir, configFilePath)
		}
		if err := os.MkdirAll(filepath.Dir(configFilePath), 0o750); err != nil {
			fail(err)
		}

		seedConfig, err = tenderseed.LoadOrGenConfig(configFilePath)
		if err != nil {
			fail(err)
		}

		// Reported, never refused: a key this binary does not know may belong
		// to a newer version, and refusing it would break the compatibility
		// contract. A misspelled one would otherwise be silently ignored. No
		// logger exists yet at this point, so this goes to standard error like
		// fail does.
		for _, key := range tenderseed.UnknownKeys(configFilePath) {
			fmt.Fprintf(os.Stderr, "tenderseed: unknown configuration key %q, ignored\n", key)
		}

		// Get chain-id, seeds from ENV. An empty variable counts as unset.
		envChainID := os.Getenv("TENDERSEED_CHAIN_ID")
		envSeeds := os.Getenv("TENDERSEED_SEEDS")

		// Set chain-id, seeds from ARGS or ENV
		if *chainID != "" {
			seedConfig.ChainID = *chainID
		} else if envChainID != "" {
			seedConfig.ChainID = envChainID
		}
		if *seeds != "" {
			seedConfig.Seeds = *seeds
		} else if envSeeds != "" {
			seedConfig.Seeds = envSeeds
		}
	}

	subcommands.ImportantFlag("home")
	subcommands.ImportantFlag("config")

	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(&cmd.StartArgs{
		HomeDir:    *homeDir,
		SeedConfig: *seedConfig,
	}, "")
	subcommands.Register(&cmd.ShowNodeIDArgs{
		HomeDir:    *homeDir,
		SeedConfig: *seedConfig,
	}, "")
	subcommands.Register(&cmd.VersionArgs{}, "")

	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}

// fail reports an error and exits with a non-zero status.
func fail(err error) {
	fmt.Fprintln(os.Stderr, "tenderseed:", err)
	os.Exit(1)
}
