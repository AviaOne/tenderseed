package tenderseed

import (
	"os"
	"path/filepath"
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
