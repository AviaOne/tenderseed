package tenderseed

import (
	"fmt"
	"os"
	"reflect"
	"sort"
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
// would serve. A selection holds at most maxGetSelection (250) addresses and a
// dial costs at most 7s: 1s to connect (transport.go dialTimeout), then two
// consecutive 3s handshake deadlines, one for the secret connection and one
// for the node info exchange. A sweep of a full selection therefore takes
// about 3m40 with the default worker count, comfortably inside this period.
//
// The sweep draws that selection with the same bias the seed serves with, see
// sweepRoutine; the ceiling is the same either way.
const DefaultPeerCheckPeriod = 10 * time.Minute

// DefaultPeerCheckWorkers is how many verification dials run in parallel.
const DefaultPeerCheckWorkers = 8

// The p2p stacks a single tenderseed binary can serve. StackCosmos is
// CometBFT, StackTM2 is the stack of gno.land.
//
// The stack is declared in config.toml like everything else that depends on
// the chain served. Nothing in chain_id names it: it is a free string on both
// sides, so it cannot be derived. An absent key means StackCosmos, which is
// the behaviour of every version up to v2.2.2, so a config file written for a
// v1 or a v2 keeps working against a v3 binary without being touched.
const (
	StackCosmos = "cosmos"
	StackTM2    = "tm2"
)

// Config is a tenderseed configuration
//
//nolint:lll
type Config struct {
	ListenAddress            string `toml:"laddr" comment:"Address to listen for incoming connections"`
	ChainID                  string `toml:"chain_id" comment:"network identifier of the chain this seed serves"`
	Stack                    string `toml:"stack" comment:"p2p stack of the chain this seed serves (\"cosmos\" or \"tm2\"); empty means cosmos"`
	AppVersion               string `toml:"app_version" comment:"tm2 only: value announced for the \"app\" entry of the version set; it belongs to the chain, not to this binary. Empty matches a chain whose app version is not semver, which is what gno.land announces today"`
	LogLevel                 string `toml:"log_level" comment:"logging level to filter output (\"debug\", \"info\", \"warn\", \"error\" or \"none\")"`
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
	PeerCheckWorkers         int    `toml:"peer_check_workers" comment:"how many verification dials run in parallel; 0 means the default of 8, it does not disable anything"`
	MetricsListenAddress     string `toml:"metrics_listen_addr" comment:"address to serve Prometheus metrics on; empty disables them"`
	Moniker                  string `toml:"moniker" comment:"name announced to peers; empty means <chain_id>-seed"`
	MetricsNamespace         string `toml:"metrics_namespace" comment:"prefix of every exported metric series"`
}

// SeedStack returns the stack this seed serves. An empty value yields
// StackCosmos, so a file written before this key existed keeps its behaviour.
//
// An unrecognised value is refused, where an unrecognised key is only
// reported. The two are not the same mistake: an unknown key may belong to a
// newer binary and ignoring it costs one setting, while a misspelled stack
// would silently start the network code of the wrong chain and the failure
// would surface far from its cause.
func (config Config) SeedStack() (string, error) {
	switch config.Stack {
	case "", StackCosmos:
		return StackCosmos, nil
	case StackTM2:
		return StackTM2, nil
	}
	return "", fmt.Errorf("stack: unknown value %q, want %q or %q",
		config.Stack, StackCosmos, StackTM2)
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

// Validate rejects values that CometBFT accepts without complaint and then
// acts on. A negative peer limit or a payload size of zero is never intended,
// and the failure it produces is far from the value that caused it.
func (config Config) Validate() error {
	if _, err := config.SeedStack(); err != nil {
		return err
	}
	if config.MaxNumInboundPeers < 0 {
		return fmt.Errorf("max_num_inbound_peers: must not be negative, got %d", config.MaxNumInboundPeers)
	}
	if config.MaxNumOutboundPeers < 0 {
		return fmt.Errorf("max_num_outbound_peers: must not be negative, got %d", config.MaxNumOutboundPeers)
	}
	if config.MaxPacketMsgPayloadSize <= 0 {
		return fmt.Errorf("max_packet_msg_payload_size: must be positive, got %d", config.MaxPacketMsgPayloadSize)
	}
	return nil
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
	defer func() { _ = reader.Close() }()

	decoder := toml.NewDecoder(reader)
	if err := decoder.Decode(config); err != nil {
		return config, err
	}

	return config, nil
}

// UnknownKeys returns the top level keys of a config file that this binary does
// not know, so a caller can say so instead of letting them pass in silence.
//
// A misspelled key is the likeliest way an operator loses a setting: decoding
// ignores it and the default applies, with nothing to see. Refusing the file
// outright would be worse, since it would stop an older binary from reading a
// file written for a newer one, which the compatibility contract forbids. So
// this reports and never refuses, and it does so by comparing the parsed tree
// against the struct tags rather than by asking the decoder for a strict mode,
// which only knows how to refuse. A file that cannot be parsed yields no keys
// and no error here: the decoder in LoadConfigFromFile is what reports that.
func UnknownKeys(file string) []string {
	reader, err := os.Open(file)
	if err != nil {
		return nil
	}
	defer func() { _ = reader.Close() }()

	tree, err := toml.LoadReader(reader)
	if err != nil {
		return nil
	}

	known := make(map[string]bool)
	fields := reflect.TypeOf(Config{})
	for i := 0; i < fields.NumField(); i++ {
		if tag := fields.Field(i).Tag.Get("toml"); tag != "" {
			known[tag] = true
		}
	}

	var unknown []string
	for _, key := range tree.Keys() {
		if !known[key] {
			unknown = append(unknown, key)
		}
	}
	sort.Strings(unknown)
	return unknown
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
		Stack:                    StackCosmos,
		AppVersion:               "",
		LogLevel:                 "info",
		NodeKeyFile:              "config/node_key.json",
		AddrBookFile:             "data/addrbook.json",
		AddrBookStrict:           true,
		MaxNumInboundPeers:       100,
		MaxNumOutboundPeers:      60,
		MaxPacketMsgPayloadSize:  1024,
		Seeds:                    "",
		SeedDisconnectWaitPeriod: "5m",
		AllowDuplicateIP:         true,
		PeerCheckPeriod:          "10m",
		PeerCheckWorkers:         DefaultPeerCheckWorkers,
		MetricsListenAddress:     "",
		Moniker:                  "",
		MetricsNamespace:         DefaultMetricsNamespace,
	}
}
