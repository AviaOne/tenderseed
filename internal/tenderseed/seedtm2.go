package tenderseed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	bftversion "github.com/gnolang/gno/tm2/pkg/bft/version"
	osm "github.com/gnolang/gno/tm2/pkg/os"
	"github.com/gnolang/gno/tm2/pkg/p2p"
	p2pconfig "github.com/gnolang/gno/tm2/pkg/p2p/config"
	"github.com/gnolang/gno/tm2/pkg/p2p/conn"
	"github.com/gnolang/gno/tm2/pkg/p2p/discovery"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
	verset "github.com/gnolang/gno/tm2/pkg/versionset"
)

// seedTM2ReactorName is the name the discovery reactor is registered under on
// the switch. The switch keys its reactors by name, and the name is only ever
// used for logging and for the reactor map.
const seedTM2ReactorName = "discovery"

// SeedTM2 is a single seed node instance on the TM2 stack: a transport, a peer
// store, a discovery reactor and a switch, serving exactly one chain.
//
// It is the counterpart of Seed, not a variant of it. The two share their
// configuration, their metrics endpoint and their command line, and nothing
// else: the identity format, the key file, the address string and the wire
// format have no overlap between the stacks, so a seed list written for one is
// not transposable to the other.
//
// Like Seed, the construction order in NewSeedTM2 matters. The node info,
// channels included, is passed to the transport by value and the handshake
// uses that copy, so a reactor registered later never reaches what peers are
// told.
type SeedTM2 struct {
	Config  Config
	HomeDir string

	Logger *slog.Logger

	NodeKey   *p2ptypes.NodeKey
	Store     *SeedBook
	Transport *p2p.MultiplexTransport
	Switch    *p2p.MultiplexSwitch
	metrics   *http.Server
}

// slogLevel maps the log_level key onto a slog level. The accepted values are
// those the Cosmos side already accepts, so one configuration key means the
// same thing on both stacks.
func slogLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "none":
		// Above every defined level, so nothing is ever emitted.
		return slog.Level(math.MaxInt32), nil
	}
	return 0, fmt.Errorf("log_level: unknown value %q", level)
}

// splitAndTrimList splits a comma separated list and drops empty entries.
// The Cosmos side gets this from a CometBFT helper, which this stack must not
// import.
func splitAndTrimList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// NewSeedTM2 builds every component of a TM2 seed node and wires them
// together. It listens on the configured address but does not start the
// switch.
func NewSeedTM2(homeDir string, seedConfig Config, out io.Writer) (*SeedTM2, error) {
	s := &SeedTM2{
		Config:  seedConfig,
		HomeDir: homeDir,
	}

	if s.Config.ChainID == "" {
		return nil, errors.New("chain_id is not set: set it in config.toml, pass -chain-id, or set TENDERSEED_CHAIN_ID")
	}

	if err := s.Config.Validate(); err != nil {
		return nil, err
	}

	level, err := slogLevel(s.Config.LogLevel)
	if err != nil {
		return nil, err
	}
	s.Logger = slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: level}))

	chainID := s.Config.ChainID
	nodeKeyFilePath := s.Config.NodeKeyFile
	addrBookFilePath := s.Config.AddrBookFile

	if !filepath.IsAbs(nodeKeyFilePath) {
		nodeKeyFilePath = filepath.Join(homeDir, nodeKeyFilePath)
	}
	if !filepath.IsAbs(addrBookFilePath) {
		addrBookFilePath = filepath.Join(homeDir, addrBookFilePath)
	}

	if err := os.MkdirAll(filepath.Dir(nodeKeyFilePath), 0o750); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(addrBookFilePath), 0o750); err != nil {
		return nil, err
	}

	nodeKey, err := p2ptypes.LoadOrMakeNodeKey(nodeKeyFilePath)
	if err != nil {
		return nil, nodeKeyError(nodeKeyFilePath, StackTM2, err)
	}
	s.NodeKey = nodeKey

	moniker := s.Config.Moniker
	if moniker == "" {
		moniker = fmt.Sprintf("%s-seed", chainID)
	}

	// The version set announced in the handshake. Four of its five entries are
	// constants of the TM2 code and are taken as they are. The fifth, "app",
	// belongs to the chain and not to the binary, which is why it is a
	// configuration key: a chain announcing a real semver one day would refuse
	// a seed that hardcoded anything else. The default, empty, has an empty
	// semver major, which is what gno.land's own "app" value also yields.
	//
	// The set is cloned because Set mutates its receiver, and the package
	// variable is shared with anything else that reads it.
	vset := slices.Clone(bftversion.VersionSet)
	vset.Set(verset.VersionInfo{
		Name:    "app",
		Version: s.Config.AppVersion,
	})

	addr, err := p2ptypes.NewNetAddressFromString(
		p2ptypes.NetAddressString(nodeKey.ID(), s.Config.ListenAddress),
	)
	if err != nil {
		return nil, err
	}

	// A seed announces the discovery channel and nothing else, and this is a
	// hard constraint rather than a choice. The channel descriptors of a
	// connection come only from the registered reactors, so announcing a
	// channel no reactor serves makes the peer send on it and the receive loop
	// stop the connection on the first unknown channel. Measured: announcing
	// the seven channels of a full node yields zero peers.
	channels := []byte{discovery.Channel}

	nodeInfo := p2ptypes.NodeInfo{
		VersionSet: vset,
		NetAddress: addr,
		Network:    chainID,
		Version:    Version,
		Channels:   channels,
		Moniker:    moniker,
	}

	p2pConfig := p2pconfig.DefaultP2PConfig()
	p2pConfig.ListenAddress = s.Config.ListenAddress
	p2pConfig.MaxPacketMsgPayloadSize = s.Config.MaxPacketMsgPayloadSize
	p2pConfig.AllowDuplicateIP = s.Config.AllowDuplicateIP

	s.Logger.Info("tenderseed",
		"key", nodeKey.ID(),
		"stack", StackTM2,
		"listen", s.Config.ListenAddress,
		"chain", chainID,
		"log-level", s.Config.LogLevel,
		"max-inbound", s.Config.MaxNumInboundPeers,
		"max-outbound", s.Config.MaxNumOutboundPeers,
		"max-packet-msg-payload-size", s.Config.MaxPacketMsgPayloadSize,
		"allow-duplicate-ip", s.Config.AllowDuplicateIP,
		"app-version", s.Config.AppVersion,
		"strict-routing", s.Config.AddrBookStrict,
		"seed-disconnect-wait-period", s.Config.SeedDisconnectWaitPeriod,
		"peer-check-period", s.Config.PeerCheckPeriod,
	)

	s.Transport = p2p.NewMultiplexTransport(
		nodeInfo,
		*nodeKey,
		conn.MConfigFromP2P(p2pConfig),
		s.Logger.With("module", "transport"),
	)
	if err := s.Transport.Listen(*addr); err != nil {
		return nil, err
	}

	store, err := NewSeedBook(
		addrBookFilePath,
		*addr,
		s.Logger.With("module", "book"),
	)
	if err != nil {
		s.closeTransport()
		return nil, err
	}
	s.Store = store

	disconnectWait, err := s.Config.DisconnectWaitPeriod()
	if err != nil {
		s.closeTransport()
		return nil, err
	}

	checkPeriod, err := s.Config.CheckPeriod()
	if err != nil {
		s.closeTransport()
		return nil, err
	}

	var metrics *seedTM2Metrics

	// The counters share the fate of the endpoint, as they do on the Cosmos
	// side: no listener, no series. The transport is listening from Listen
	// above, so every failing path past it releases the socket.
	//
	// The namespace is checked before the series are built rather than after,
	// so a refused configuration never registers anything first.
	if s.Config.MetricsListenAddress != "" {
		if s.Config.MetricsNamespace == "" {
			s.closeTransport()
			return nil, errEmptyMetricsNamespace
		}

		metrics, err = newSeedTM2Metrics(s.Config.MetricsNamespace)
		if err != nil {
			s.closeTransport()
			return nil, err
		}
	}

	reactor := NewSeedReactorTM2(
		store,
		s.Config.AddrBookStrict,
		disconnectWait,
		checkPeriod,
		metrics,
		s.Logger.With("module", seedTM2ReactorName),
	)

	seedAddrs, errs := p2ptypes.NewNetAddressFromStrings(splitAndTrimList(s.Config.Seeds))
	for _, seedErr := range errs {
		s.Logger.Error("invalid seed address", "err", seedErr)
	}

	s.Switch = p2p.NewMultiplexSwitch(
		s.Transport,
		p2p.WithSeeds(seedAddrs),
		p2p.WithMaxInboundPeers(uint64(s.Config.MaxNumInboundPeers)),
		p2p.WithMaxOutboundPeers(uint64(s.Config.MaxNumOutboundPeers)),
		p2p.WithAllowDuplicateIP(s.Config.AllowDuplicateIP),
		p2p.WithReactor(seedTM2ReactorName, reactor),
	)
	s.Switch.SetLogger(quietSeedLogger(s.Logger).With("module", "switch"))

	return s, nil
}

// Start starts the switch, which starts the discovery reactor.
func (s *SeedTM2) Start() error {
	if addr := s.Config.MetricsListenAddress; addr != "" {
		s.metrics = newMetricsServer(addr)
		go func() {
			s.Logger.Info("serving metrics", "addr", addr)
			if err := s.metrics.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.Logger.Error("metrics server stopped", "err", err)
			}
		}()
	}
	if err := s.Switch.Start(); err != nil {
		s.stopMetrics()
		s.closeTransport()
		return err
	}
	return nil
}

// Wait blocks until the switch stops.
func (s *SeedTM2) Wait() {
	s.Switch.Wait()
}

// TrapSignal installs the shutdown handler of the TM2 stack.
//
// Trap after Start, like the Cosmos side does and for the same reason: before
// that point there is nothing to save and nothing to stop.
func (s *SeedTM2) TrapSignal() {
	osm.TrapSignal(func() {
		s.Logger.Info("shutting down...")
		if err := s.Stop(); err != nil {
			s.Logger.Error("error while shutting down", "err", err)
		}
	})
}

// Stop stops the switch and releases the listening socket. The peer store is
// flushed by the discovery reactor as the switch stops it.
func (s *SeedTM2) Stop() error {
	if !s.Switch.IsRunning() {
		s.stopMetrics()
		s.closeTransport()
		return nil
	}
	s.stopMetrics()
	err := s.Switch.Stop()
	s.closeTransport()
	return err
}

// closeTransport releases the listening socket. Stopping the switch stops the
// peers and the reactors but never touches the transport.
func (s *SeedTM2) closeTransport() {
	if s.Transport == nil {
		return
	}
	if err := s.Transport.Close(); err != nil {
		s.Logger.Error("could not close transport", "err", err)
	}
}

// stopMetrics shuts the metrics server down if one is running.
func (s *SeedTM2) stopMetrics() {
	if s.metrics != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.metrics.Shutdown(ctx); err != nil {
			s.Logger.Error("could not stop metrics server", "err", err)
		}
	}
}
