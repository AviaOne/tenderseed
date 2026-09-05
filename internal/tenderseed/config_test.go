package tenderseed

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	return path
}

// A seven-key config: no log_level, no seeds, no max_packet_msg_payload_size.
// Real deployments ship files like this, so partial files must keep loading.
const partialConfig = `addr_book_file = "data/addrbook.json"
addr_book_strict = true
chain_id = "test-chain-1"
laddr = "tcp://0.0.0.0:26656"
max_num_inbound_peers = 50
max_num_outbound_peers = 10
node_key_file = "config/node_key.json"
`

// A nine-key config: log_level is absent.
const fullConfig = `addr_book_file = "data/addrbook.json"
addr_book_strict = true
chain_id = "test-chain-2"
laddr = "tcp://0.0.0.0:26656"
max_num_inbound_peers = 500
max_num_outbound_peers = 200
max_packet_msg_payload_size = 1024
node_key_file = "config/node_key.json"
seeds = "0000000000000000000000000000000000000000@seed.example.com:26656"
`

func TestLoadPartialConfig(t *testing.T) {
	config, err := LoadConfigFromFile(writeConfig(t, partialConfig))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	if config.ChainID != "test-chain-1" {
		t.Errorf("ChainID = %q, want test-chain-1", config.ChainID)
	}
	if config.MaxNumInboundPeers != 50 {
		t.Errorf("MaxNumInboundPeers = %d, want 50", config.MaxNumInboundPeers)
	}
	if config.MaxNumOutboundPeers != 10 {
		t.Errorf("MaxNumOutboundPeers = %d, want 10", config.MaxNumOutboundPeers)
	}
	if config.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want the default info", config.LogLevel)
	}
	if config.MaxPacketMsgPayloadSize != 1024 {
		t.Errorf("MaxPacketMsgPayloadSize = %d, want the default 1024", config.MaxPacketMsgPayloadSize)
	}
	if config.Seeds != "" {
		t.Errorf("Seeds = %q, want empty", config.Seeds)
	}
	if !config.AllowDuplicateIP {
		t.Error("AllowDuplicateIP = false, want the default true")
	}
	wait, err := config.DisconnectWaitPeriod()
	if err != nil {
		t.Fatalf("DisconnectWaitPeriod: %v", err)
	}
	if wait != DefaultSeedDisconnectWaitPeriod {
		t.Errorf("DisconnectWaitPeriod = %s, want %s", wait, DefaultSeedDisconnectWaitPeriod)
	}
}

func TestLoadFullConfig(t *testing.T) {
	config, err := LoadConfigFromFile(writeConfig(t, fullConfig))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	if config.ChainID != "test-chain-2" {
		t.Errorf("ChainID = %q, want test-chain-2", config.ChainID)
	}
	if config.ListenAddress != "tcp://0.0.0.0:26656" {
		t.Errorf("ListenAddress = %q, want tcp://0.0.0.0:26656", config.ListenAddress)
	}
	if config.MaxNumInboundPeers != 500 {
		t.Errorf("MaxNumInboundPeers = %d, want 500", config.MaxNumInboundPeers)
	}
	if config.MaxNumOutboundPeers != 200 {
		t.Errorf("MaxNumOutboundPeers = %d, want 200", config.MaxNumOutboundPeers)
	}
	if config.Seeds == "" {
		t.Error("Seeds is empty, want the value from the file")
	}
	if config.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want the default info", config.LogLevel)
	}
}

func TestAllowDuplicateIPCanBeDisabled(t *testing.T) {
	config, err := LoadConfigFromFile(writeConfig(t, "allow_duplicate_ip = false\n"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	if config.AllowDuplicateIP {
		t.Error("AllowDuplicateIP = true, want false from the file")
	}
}

func TestDisconnectWaitPeriod(t *testing.T) {
	cases := []struct {
		value   string
		want    time.Duration
		wantErr bool
	}{
		{value: "", want: DefaultSeedDisconnectWaitPeriod},
		{value: "5m", want: 5 * time.Minute},
		{value: "30s", want: 30 * time.Second},
		{value: "28h", want: 28 * time.Hour},
		{value: "0s", want: 0},
		{value: "-5m", wantErr: true},
		{value: "later", wantErr: true},
	}
	for _, c := range cases {
		got, err := Config{SeedDisconnectWaitPeriod: c.value}.DisconnectWaitPeriod()
		if c.wantErr {
			if err == nil {
				t.Errorf("%q: expected an error, got %s", c.value, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected error %v", c.value, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q: got %s, want %s", c.value, got, c.want)
		}
	}
}

func TestValidateRejectsNegativeLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mutfn func(*Config)
	}{
		{"inbound", func(c *Config) { c.MaxNumInboundPeers = -1 }},
		{"outbound", func(c *Config) { c.MaxNumOutboundPeers = -1 }},
		{"payload negative", func(c *Config) { c.MaxPacketMsgPayloadSize = -1 }},
		{"payload zero", func(c *Config) { c.MaxPacketMsgPayloadSize = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultConfig()
			tc.mutfn(config)
			if err := config.Validate(); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

func TestValidateAcceptsTheDefaults(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
}

func TestDefaultConfigIsUsable(t *testing.T) {
	wait, err := DefaultConfig().DisconnectWaitPeriod()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	if wait != DefaultSeedDisconnectWaitPeriod {
		t.Errorf("got %s, want %s", wait, DefaultSeedDisconnectWaitPeriod)
	}
	period, err := DefaultConfig().CheckPeriod()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	if period != DefaultPeerCheckPeriod {
		t.Errorf("got %s, want %s", period, DefaultPeerCheckPeriod)
	}
}

func TestUnknownKeysAreReportedNotRefused(t *testing.T) {
	path := writeConfig(t, `
chain_id = "test-1"
peer_check_perod = "1m"
not_a_key = 3
`)

	config, err := LoadConfigFromFile(path)
	if err != nil {
		t.Fatalf("a file with unknown keys must still load: %v", err)
	}
	if config.PeerCheckPeriod != DefaultConfig().PeerCheckPeriod {
		t.Error("a misspelled key must not change the value it was aimed at")
	}

	unknown := UnknownKeys(path)
	if len(unknown) != 2 || unknown[0] != "not_a_key" || unknown[1] != "peer_check_perod" {
		t.Fatalf("unknown keys = %v, want [not_a_key peer_check_perod]", unknown)
	}

	if got := UnknownKeys(writeConfig(t, "chain_id = \"test-1\"\n")); len(got) != 0 {
		t.Errorf("a correct file reports %v, want nothing", got)
	}
	if got := UnknownKeys(filepath.Join(t.TempDir(), "absent.toml")); got != nil {
		t.Errorf("a missing file reports %v, want nothing", got)
	}
}

func TestSeedStack(t *testing.T) {
	cases := []struct {
		value   string
		want    string
		wantErr bool
	}{
		{value: "", want: StackCosmos},
		{value: StackCosmos, want: StackCosmos},
		{value: StackTM2, want: StackTM2},
		{value: "TM2", wantErr: true},
		{value: "tendermint2", wantErr: true},
		{value: "gno", wantErr: true},
	}
	for _, c := range cases {
		got, err := Config{Stack: c.value}.SeedStack()
		if c.wantErr {
			if err == nil {
				t.Errorf("%q: expected an error, got %q", c.value, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected error %v", c.value, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q: got %q, want %q", c.value, got, c.want)
		}
	}
}

// A config file written before the key existed must keep serving Cosmos, which
// is what makes a binary-only upgrade from v1 or v2 possible.
func TestStackIsAbsentFromAnOlderFile(t *testing.T) {
	config, err := LoadConfigFromFile(writeConfig(t, partialConfig))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	stack, err := config.SeedStack()
	if err != nil {
		t.Fatalf("SeedStack: %v", err)
	}
	if stack != StackCosmos {
		t.Errorf("stack = %q, want %q", stack, StackCosmos)
	}
	if len(UnknownKeys(writeConfig(t, "stack = \"tm2\"\n"))) != 0 {
		t.Error("stack must be a known key")
	}
}

func TestValidateRejectsAnUnknownStack(t *testing.T) {
	config := DefaultConfig()
	config.Stack = "tendermint2"
	if err := config.Validate(); err == nil {
		t.Fatal("expected an error, got none")
	}
}

func TestDefaultStackIsCosmos(t *testing.T) {
	stack, err := DefaultConfig().SeedStack()
	if err != nil {
		t.Fatalf("SeedStack: %v", err)
	}
	if stack != StackCosmos {
		t.Errorf("stack = %q, want %q", stack, StackCosmos)
	}
}

// app_version belongs to the chain, not to the binary, so it is a key and not
// a constant. Absent, it stays empty, which is what a chain announcing a non
// semver app version needs.
func TestAppVersionIsAKeyAndDefaultsToEmpty(t *testing.T) {
	if got := DefaultConfig().AppVersion; got != "" {
		t.Errorf("AppVersion = %q, want empty", got)
	}

	config, err := LoadConfigFromFile(writeConfig(t, "app_version = \"v1.2.3\"\n"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	if config.AppVersion != "v1.2.3" {
		t.Errorf("AppVersion = %q, want v1.2.3", config.AppVersion)
	}

	if got := UnknownKeys(writeConfig(t, "app_version = \"dev\"\n")); len(got) != 0 {
		t.Errorf("app_version must be a known key, got %v", got)
	}
}

// The generated file is meant to be read by a human, so its order is part of
// what it is: the keys an operator may have to change come first. The encoder
// sorts alphabetically unless told otherwise, and nothing else would notice a
// silent return to that default, so this pins it.
func TestGeneratedConfigKeepsFieldOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := WriteConfigToFile(path, *DefaultConfig()); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading config: %v", err)
	}
	text := string(body)

	// Alphabetical order would put every one of these before chain_id.
	for _, key := range []string{"addr_book_file", "addr_book_strict", "allow_duplicate_ip"} {
		if strings.Index(text, "\n"+key+" ") < strings.Index(text, "\nchain_id ") {
			t.Errorf("%s comes before chain_id, the file is sorted again", key)
		}
	}

	// And the groups follow one another.
	order := []string{"chain_id", "stack", "seeds", "laddr", "node_key_file",
		"max_num_inbound_peers", "seed_disconnect_wait_period", "log_level"}
	previous := -1
	for _, key := range order {
		at := strings.Index(text, "\n"+key+" ")
		if at < 0 {
			t.Fatalf("%s is absent from the generated file", key)
		}
		if at < previous {
			t.Errorf("%s is out of order", key)
		}
		previous = at
	}

	// A file a human reads needs its banners.
	if !strings.Contains(text, "WHAT YOU MAY NEED TO CHANGE") {
		t.Error("the first banner is missing")
	}
}

// The stack has to be recorded when the file is created, because the same
// first command creates the node identity and the identity format belongs to
// the stack.
func TestStackIsRecordedWhenTheFileIsCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")

	config, err := LoadOrGenConfig(path, StackTM2)
	if err != nil {
		t.Fatalf("generating config: %v", err)
	}
	if config.Stack != StackTM2 {
		t.Errorf("Stack = %q, want %q", config.Stack, StackTM2)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading config: %v", err)
	}
	if !strings.Contains(string(body), `stack = "tm2"`) {
		t.Error("the generated file does not record the stack")
	}

	// An existing file is never rewritten, so a later run without the
	// argument keeps what the operator has.
	again, err := LoadOrGenConfig(path, "")
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	if again.Stack != StackTM2 {
		t.Errorf("Stack = %q after a second load, want %q", again.Stack, StackTM2)
	}
}

// Without the argument the default stands, which is what a Cosmos operator
// gets and what every version up to v2.2.2 wrote.
func TestGeneratedConfigDefaultsToCosmos(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	config, err := LoadOrGenConfig(path, "")
	if err != nil {
		t.Fatalf("generating config: %v", err)
	}
	if config.Stack != StackCosmos {
		t.Errorf("Stack = %q, want %q", config.Stack, StackCosmos)
	}
}

// A home directory belongs to one stack. Reading one with the other must say
// so, since the decoder underneath names neither the file nor the cause.
func TestNodeKeyErrorNamesTheFileAndTheStack(t *testing.T) {
	home := t.TempDir()

	if _, err := NodeID(home, Config{
		NodeKeyFile: "config/node_key.json",
		Stack:       StackCosmos,
	}); err != nil {
		t.Fatalf("generating a cosmos key: %v", err)
	}

	_, err := NodeID(home, Config{
		NodeKeyFile: "config/node_key.json",
		Stack:       StackTM2,
	})
	if err == nil {
		t.Fatal("reading a cosmos key as tm2 must fail")
	}
	for _, want := range []string{"node_key.json", StackTM2, "one stack"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// The flag settles the stack when the file is created. Afterwards it may
// confirm what the file says, never contradict it, because the key file and
// the address book on disk follow the stack.
func TestCheckStackFlagRefusesAContradiction(t *testing.T) {
	cosmos := Config{Stack: StackCosmos}
	older := Config{} // written before the key existed

	if err := cosmos.CheckStackFlag(""); err != nil {
		t.Errorf("no flag must be accepted: %v", err)
	}
	if err := cosmos.CheckStackFlag(StackCosmos); err != nil {
		t.Errorf("a flag confirming the file must be accepted: %v", err)
	}
	if err := older.CheckStackFlag(StackCosmos); err != nil {
		t.Errorf("an absent stack means cosmos: %v", err)
	}
	if err := cosmos.CheckStackFlag(StackTM2); err == nil {
		t.Error("a flag contradicting the file must be refused")
	}
	if err := older.CheckStackFlag(StackTM2); err == nil {
		t.Error("a flag contradicting an absent stack must be refused")
	}
	if err := cosmos.CheckStackFlag("bogus"); err == nil {
		t.Error("an unknown value must be refused")
	}
}

// A key file can fail to be read for reasons that have nothing to do with the
// stack. The message must not name a cause it has not established.
func TestNodeKeyErrorDoesNotAssertACause(t *testing.T) {
	home := t.TempDir()
	// A directory where a file is expected fails for a reason of its own.
	if err := os.MkdirAll(filepath.Join(home, "config", "node_key.json"), 0o750); err != nil {
		t.Fatalf("preparing the case: %v", err)
	}

	_, err := NodeID(home, Config{
		NodeKeyFile: "config/node_key.json",
		Stack:       StackTM2,
	})
	if err == nil {
		t.Fatal("reading a directory as a key must fail")
	}
	if !strings.Contains(err.Error(), "node_key.json") {
		t.Errorf("error %q does not name the file", err)
	}
	if !strings.Contains(err.Error(), "common cause") {
		t.Errorf("error %q states a cause it has not established", err)
	}
}

// TestValidateRefusesNegativeWorkers pins that the refusal belongs to the
// common check and not to the stack that reads the key: a negative worker
// count used to stop one stack from starting and start the other in silence.
func TestValidateRefusesNegativeWorkers(t *testing.T) {
	t.Parallel()

	for _, stack := range []string{"", StackCosmos, StackTM2} {
		config := DefaultConfig()
		config.Stack = stack
		config.PeerCheckWorkers = -1

		if err := config.Validate(); err == nil {
			t.Fatalf("stack %q accepted a negative peer_check_workers", stack)
		}
	}
}
