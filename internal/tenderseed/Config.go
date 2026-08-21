package tenderseed

import (
	"fmt"
	"os"
	"time"

	toml "github.com/pelletier/go-toml"
)

// DefaultSeedDisconnectWaitPeriod is how long a crawled outbound peer is kept
// connected before the PEX reactor disconnects it.
//
// CometBFT hardcodes 28 hours for a full node (node/setup.go). That value is
// derived from the time a peer needs to become "good" through the consensus
// reactor, which a seed node does not run, so it does not apply here.
const DefaultSeedDisconnectWaitPeriod = 5 * time.Minute

// DefaultMetricsNamespace prefixes every exported series. It matches the
// upstream default (config.Namespace), so dashboards written for a full node
// work unchanged against a seed.
const DefaultMetricsNamespace = "cometbft"

// DefaultPeerCheckPeriod is how often the seed re-verifies the addresses it
// would serve. GetSelection returns at most maxGetSelection (250) addresses
// and a dial costs at most 4s (1s connect, 3s handshake), so a sweep with the
// default worker count finishes well inside this period.
const DefaultPeerCheckPeriod = 10 * time.Minute

// DefaultPeerCheckWorkers is how many verification dials run in parallel.
const DefaultPeerCheckWorkers = 8

// Config is a tenderseed configuration
//
//nolint:lll
type Config struct {
	ListenAddress            string `toml:"laddr" comment:"Address to listen for incoming connections"`
	ChainID                  string `toml:"chain_id" comment:"network identifier of the chain this seed serves"`
	LogLevel                 string `toml:"log_level" comment:"logging level to filter output (\"info\", \"debug\", \"error\" or \"none\")"`
	NodeKeyFile              string `toml:"node_key_file" comment:"path to node_key (relative to the seed home directory (-home) or an absolute path)"`
	AddrBookFile             string `toml:"addr_book_file" comment:"path to address book (relative to the seed home directory (-home) or an absolute path)"`
	AddrBookStrict           bool   `toml:"addr_book_strict" comment:"Set true for strict routability rules\n Set false for private or local networks"`
	MaxNumInboundPeers       int    `toml:"max_num_inbound_peers" comment:"maximum number of inbound connections"`
	MaxNumOutboundPeers      int    `toml:"max_num_outbound_peers" comment:"maximum number of outbound connections"`
	MaxPacketMsgPayloadSize  int    `toml:"max_packet_msg_payload_size" comment:"maximum size of a message packet payload, in bytes"`
	Seeds                    string `toml:"seeds" comment:"seed nodes we can use to discover peers"`
	SeedDisconnectWaitPeriod string `toml:"seed_disconnect_wait_period" comment:"how long a crawled peer stays connected before being disconnected, as a duration (\"5m\", \"30s\", \"1h\")"`
	AllowDuplicateIP         bool   `toml:"allow_duplicate_ip" comment:"allow multiple peers from the same IP address"`
	PeerCheckPeriod          string `toml:"peer_check_period" comment:"how often served addresses are re-verified, as a duration; 0 disables verification"`
	PeerCheckWorkers         int    `toml:"peer_check_workers" comment:"how many verification dials run in parallel"`
	MetricsListenAddress     string `toml:"metrics_listen_addr" comment:"address to serve Prometheus metrics on; empty disables them"`
	Moniker                  string `toml:"moniker" comment:"name announced to peers; empty means <chain_id>-seed"`
	MetricsNamespace         string `toml:"metrics_namespace" comment:"prefix of every exported metric series"`
}

// DisconnectWaitPeriod returns SeedDisconnectWaitPeriod as a duration.
// An empty value yields DefaultSeedDisconnectWaitPeriod.
func (config Config) DisconnectWaitPeriod() (time.Duration, error) {
	if config.SeedDisconnectWaitPeriod == "" {
		return DefaultSeedDisconnectWaitPeriod, nil
	}
	d, err := time.ParseDuration(config.SeedDisconnectWaitPeriod)
	if err != nil {
		return 0, fmt.Errorf("seed_disconnect_wait_period: %w", err)
	}
	if d < 0 {
		return 0, fmt.Errorf("seed_disconnect_wait_period: must not be negative, got %s", d)
	}
	return d, nil
}

// CheckPeriod returns PeerCheckPeriod as a duration.
// An empty value yields DefaultPeerCheckPeriod; zero disables verification.
func (config Config) CheckPeriod() (time.Duration, error) {
	if config.PeerCheckPeriod == "" {
		return DefaultPeerCheckPeriod, nil
	}
	d, err := time.ParseDuration(config.PeerCheckPeriod)
	if err != nil {
		return 0, fmt.Errorf("peer_check_period: %w", err)
	}
	if d < 0 {
		return 0, fmt.Errorf("peer_check_period: must not be negative, got %s", d)
	}
	return d, nil
}

// CheckWorkers returns the number of verification workers to run.
func (config Config) CheckWorkers() (int, error) {
	if config.PeerCheckWorkers == 0 {
		return DefaultPeerCheckWorkers, nil
	}
	if config.PeerCheckWorkers < 0 {
		return 0, fmt.Errorf("peer_check_workers: must not be negative, got %d", config.PeerCheckWorkers)
	}
	return config.PeerCheckWorkers, nil
}

// LoadOrGenConfig loads a seed config from file if the file exists
// If the file does not exist, make a default config, write it to the file
// Return either the loaded config or a default config
func LoadOrGenConfig(filePath string) (*Config, error) {
	config, err := LoadConfigFromFile(filePath)
	if err == nil {
		return config, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	// file did not exist
	config = DefaultConfig()
	err = WriteConfigToFile(filePath, *config)
	return config, err
}

// LoadConfigFromFile loads a seed config from a file.
// Decoding starts from DefaultConfig, so a partial file is valid: every key
// left out keeps its default value.
func LoadConfigFromFile(file string) (*Config, error) {
	config := DefaultConfig()
	reader, err := os.Open(file)
	if err != nil {
		return config, err
	}
	defer reader.Close()

	decoder := toml.NewDecoder(reader)
	if err := decoder.Decode(config); err != nil {
		return config, err
	}

	return config, nil
}

// WriteConfigToFile writes the seed config to file
func WriteConfigToFile(file string, config Config) error {
	bytes, err := toml.Marshal(config)
	if err != nil {
		return err
	}

	return os.WriteFile(file, bytes, 0o600)
}

// DefaultConfig returns a seed config initialized with default values
func DefaultConfig() *Config {
	return &Config{
		ListenAddress:            "tcp://0.0.0.0:26656",
		ChainID:                  "",
		LogLevel:                 "info",
		NodeKeyFile:              "config/node_key.json",
		AddrBookFile:             "data/addrbook.json",
		AddrBookStrict:           true,
		MaxNumInboundPeers:       100,
		MaxNumOutboundPeers:      60,
		MaxPacketMsgPayloadSize:  1024,
		Seeds:                    "",
		SeedDisconnectWaitPeriod: DefaultSeedDisconnectWaitPeriod.String(),
		AllowDuplicateIP:         true,
		PeerCheckPeriod:          DefaultPeerCheckPeriod.String(),
		PeerCheckWorkers:         DefaultPeerCheckWorkers,
		MetricsListenAddress:     "",
		Moniker:                  "",
		MetricsNamespace:         DefaultMetricsNamespace,
	}
}
