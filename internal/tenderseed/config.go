package tenderseed

import (
	"bytes"
	"errors"
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
	// Field order is the order of the generated file, see WriteConfigToFile.
	// The keys an operator may have to change come first; the rest is grouped
	// by what it governs. The comment of the first key of a group carries the
	// banner that opens it. A comment already starting with "#" is not
	// prefixed again by the encoder, which is what makes a full width banner
	// possible; every following line of a comment receives one "#", so those
	// lines are written one character short on purpose.

	ChainID       string `toml:"chain_id" comment:"##############################################################\n##              WHAT YOU MAY NEED TO CHANGE               ###\n##                                                        ###\n##  after any change here, restart the seed.              ###\n##  systemd:  sudo systemctl restart tenderseed-<chain_id>###\n##  docker:   docker restart tenderseed-<chain_id>        ###\n#############################################################\n network identifier of the chain this seed serves"`
	Stack         string `toml:"stack" comment:"p2p stack of that chain, \"cosmos\" or \"tm2\"; empty means cosmos.\n It cannot be guessed from chain_id, and it decides the format of\n the node key and of the address book, so a home directory\n belongs to one stack"`
	Seeds         string `toml:"seeds" comment:"seed nodes we can use to discover peers, in the identity format\n of the stack above. May be emptied once the address book is\n populated"`
	ListenAddress string `toml:"laddr" comment:"Address to listen for incoming connections"`
	Moniker       string `toml:"moniker" comment:"name announced to peers; empty means <chain_id>-seed. It is a\n label only: nothing resolves it and nothing dials it"`
	AppVersion    string `toml:"app_version" comment:"tm2 only, usually empty: value announced for the \"app\" entry of\n the version set; it belongs to the chain, not to this binary"`

	NodeKeyFile  string `toml:"node_key_file" comment:"##############################################################\n##                         FILES                          ###\n#############################################################\n path to node_key (relative to the seed home directory (-home)\n or an absolute path)"`
	AddrBookFile string `toml:"addr_book_file" comment:"path to address book (relative to the seed home directory\n (-home) or an absolute path)"`

	MaxNumInboundPeers      int  `toml:"max_num_inbound_peers" comment:"##############################################################\n##                     NETWORK LIMITS                     ###\n#############################################################\n maximum number of inbound connections"`
	MaxNumOutboundPeers     int  `toml:"max_num_outbound_peers" comment:"maximum number of outbound connections"`
	MaxPacketMsgPayloadSize int  `toml:"max_packet_msg_payload_size" comment:"maximum size of a message packet payload, in bytes"`
	AllowDuplicateIP        bool `toml:"allow_duplicate_ip" comment:"allow multiple peers from the same IP address"`
	AddrBookStrict          bool `toml:"addr_book_strict" comment:"Set true for strict routability rules\n Set false for private or local networks"`

	SeedDisconnectWaitPeriod string `toml:"seed_disconnect_wait_period" comment:"##############################################################\n##                     SEED BEHAVIOUR                     ###\n#############################################################\n how long a crawled peer stays connected before being\n disconnected, as a duration (\"5m\", \"30s\", \"1h\")"`
	PeerCheckPeriod          string `toml:"peer_check_period" comment:"how often served addresses are re-verified, as a duration;\n 0 disables verification"`
	PeerCheckWorkers         int    `toml:"peer_check_workers" comment:"cosmos only: how many verification dials run in parallel;\n 0 means the default of 8, it does not disable anything"`

	LogLevel             string `toml:"log_level" comment:"##############################################################\n##                  LOGGING AND METRICS                   ###\n#############################################################\n logging level to filter output (\"debug\", \"info\", \"warn\",\n \"error\" or \"none\")"`
	MetricsListenAddress string `toml:"metrics_listen_addr" comment:"address to serve Prometheus metrics on; empty disables them"`
	MetricsNamespace     string `toml:"metrics_namespace" comment:"prefix of every exported metric series. Leave it as it is unless\n your dashboards need another prefix"`
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
// errEmptyMetricsNamespace is returned when the endpoint is asked for without
// a name to publish under. It belongs to both stacks: one configuration key
// means the same thing on each, refusal included.
var errEmptyMetricsNamespace = errors.New(
	"metrics_namespace is empty while metrics_listen_addr is set",
)

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
//
// stack is the stack to record in a file that has to be created, empty for
// the default. It is an argument rather than a value the caller sets
// afterwards because the identity of a seed is generated on the very first
// run, by the same command that creates this file, and the format of that
// identity belongs to the stack. Without a way to declare the stack at
// creation time, an operator serving TM2 would be handed a Cosmos identity
// and would have to delete it. A file that already exists is never
// rewritten, so this argument only ever affects a first run.
func LoadOrGenConfig(filePath string, stack string) (*Config, error) {
	config, err := LoadConfigFromFile(filePath)
	if err == nil {
		return config, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	// file did not exist
	config = DefaultConfig()
	if stack != "" {
		config.Stack = stack
	}
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

// WriteConfigToFile writes the seed config to file.
//
// The encoder is asked to preserve the declaration order of Config rather
// than sort the keys, which is its default. Sorted, the file opens on the
// address book paths and buries chain_id and seeds in the middle, so the
// values an operator has to supply are the ones they have to hunt for. Order
// is a property of the generated file only: nothing reads a config.toml by
// position, and an existing file is never rewritten.
func WriteConfigToFile(file string, config Config) error {
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Order(toml.OrderPreserve).Encode(config); err != nil {
		return err
	}

	return os.WriteFile(file, buf.Bytes(), 0o600)
}

// DefaultConfig returns a seed config initialized with default values
func DefaultConfig() *Config {
	// Same order as the struct, so the two stay readable side by side.
	return &Config{
		ChainID:                  "",
		Stack:                    StackCosmos,
		Seeds:                    "",
		ListenAddress:            "tcp://0.0.0.0:26656",
		Moniker:                  "",
		AppVersion:               "",
		NodeKeyFile:              "config/node_key.json",
		AddrBookFile:             "data/addrbook.json",
		MaxNumInboundPeers:       100,
		MaxNumOutboundPeers:      60,
		MaxPacketMsgPayloadSize:  1024,
		AllowDuplicateIP:         true,
		AddrBookStrict:           true,
		SeedDisconnectWaitPeriod: "5m",
		PeerCheckPeriod:          "10m",
		PeerCheckWorkers:         DefaultPeerCheckWorkers,
		LogLevel:                 "info",
		MetricsListenAddress:     "",
		MetricsNamespace:         DefaultMetricsNamespace,
	}
}

// CheckStackFlag refuses a -stack value that contradicts an existing
// configuration file.
//
// The other top level flags override the file and nothing on disk remembers
// it. This one is different: the stack decides the format of the node key and
// of the address book, so overriding it silently can leave a home whose
// config.toml says one stack and whose key file belongs to the other, which
// only surfaces at the next start. Measured: a home whose key file has been
// removed, which is what an identity rotation does, took a TM2 key while its
// file still said cosmos.
//
// So the flag settles the stack when the file is created, and afterwards it
// may confirm what the file says but never contradict it. Changing the stack
// of an established home is not a flag, it is a new home.
func (config Config) CheckStackFlag(flag string) error {
	if flag == "" {
		return nil
	}
	wanted, err := (Config{Stack: flag}).SeedStack()
	if err != nil {
		return err
	}
	current, err := config.SeedStack()
	if err != nil {
		return err
	}
	if wanted != current {
		return fmt.Errorf("-stack %s contradicts the configuration, which "+
			"serves %s. A home directory belongs to one stack: edit stack in "+
			"config.toml if that is what you mean, or point -home at another "+
			"directory", wanted, current)
	}
	return nil
}
