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

	configFilePath := *configFile
	if !filepath.IsAbs(configFilePath) {
		configFilePath = filepath.Join(*homeDir, configFilePath)
	}
	if err := tenderseed.MkdirAll(filepath.Dir(configFilePath), 0o750); err != nil {
		fail(err)
	}

	seedConfig, err := tenderseed.LoadOrGenConfig(configFilePath)
	if err != nil {
		fail(err)
	}

	// Get chain-id, seeds from ENV. An empty variable counts as unset.
	env_chainid := os.Getenv("TENDERSEED_CHAIN_ID")
	env_seeds := os.Getenv("TENDERSEED_SEEDS")

	// Set chain-id, seeds from ARGS or ENV
	if *chainID != "" {
		seedConfig.ChainID = *chainID
	} else if env_chainid != "" {
		seedConfig.ChainID = env_chainid
	}
	if *seeds != "" {
		seedConfig.Seeds = *seeds
	} else if env_seeds != "" {
		seedConfig.Seeds = env_seeds
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

	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}

// fail reports an error and exits with a non-zero status.
func fail(err error) {
	fmt.Fprintln(os.Stderr, "tenderseed:", err)
	os.Exit(1)
}
