package tenderseed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	cmtstrings "github.com/cometbft/cometbft/libs/strings"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/version"
)

// Seed is a single seed node instance: a transport, an address book, a PEX
// reactor in seed mode and a switch, serving exactly one chain.
//
// The construction order in NewSeed is significant and must not be changed:
// the node info channel list is built before the transport, because
// NewMultiplexTransport takes the node info by value and the handshake uses
// that copy. Adding a reactor later does not change what the transport
// announces to peers.
type Seed struct {
	Config  Config
	HomeDir string

	Logger         log.Logger
	FilteredLogger log.Logger

	NodeKey   *p2p.NodeKey
	AddrBook  pex.AddrBook
	Reactor   *SeedReactor
	Switch    *p2p.Switch
	metrics   *http.Server
	Transport *p2p.MultiplexTransport
}

// NewSeed builds every component of a seed node and wires them together.
// It listens on the configured address but does not start the switch.
// Version is the software version announced to peers during the handshake.
// Override it at build time with -ldflags "-X github.com/AviaOne/tenderseed/internal/tenderseed.Version=<value>".
var Version = "2.0.0"

func NewSeed(homeDir string, seedConfig Config, logger log.Logger) (*Seed, error) {
	s := &Seed{
		Config:  seedConfig,
		HomeDir: homeDir,
		Logger:  logger,
	}

	if s.Config.ChainID == "" {
		return nil, errors.New("chain_id is not set: set it in config.toml, pass -chain-id, or set TENDERSEED_CHAIN_ID")
	}

	chainID := s.Config.ChainID
	nodeKeyFilePath := s.Config.NodeKeyFile
	addrBookFilePath := s.Config.AddrBookFile

	if !filepath.IsAbs(nodeKeyFilePath) {
		nodeKeyFilePath = filepath.Join(homeDir, nodeKeyFilePath)
	}
	if !filepath.IsAbs(addrBookFilePath) {
		addrBookFilePath = filepath.Join(homeDir, addrBookFilePath)
	}

	if err := MkdirAll(filepath.Dir(nodeKeyFilePath), 0o750); err != nil {
		return nil, err
	}
	if err := MkdirAll(filepath.Dir(addrBookFilePath), 0o750); err != nil {
		return nil, err
	}

	disconnectWait, err := s.Config.DisconnectWaitPeriod()
	if err != nil {
		return nil, err
	}

	checkPeriod, err := s.Config.CheckPeriod()
	if err != nil {
		return nil, err
	}

	checkWorkers, err := s.Config.CheckWorkers()
	if err != nil {
		return nil, err
	}

	// SeedMode, ListenAddress, Seeds and PersistentPeers are left at their
	// defaults on purpose: CometBFT v0.40.0 never reads them from P2PConfig.
	// Seed mode and the seed list reach the PEX reactor through
	// pex.ReactorConfig below, and listening is done by Transport.Listen.
	p2pConfig := config.DefaultP2PConfig()
	p2pConfig.AllowDuplicateIP = s.Config.AllowDuplicateIP

	// allow a lot of inbound peers since we disconnect from them quickly in seed mode
	p2pConfig.MaxNumInboundPeers = s.Config.MaxNumInboundPeers

	// keep trying to make outbound connections to exchange peering info
	p2pConfig.MaxNumOutboundPeers = s.Config.MaxNumOutboundPeers

	// allow increasing maximum size of a message packet payload
	// because there are some chains that override this and result in larger payloads
	p2pConfig.MaxPacketMsgPayloadSize = s.Config.MaxPacketMsgPayloadSize

	nodeKey, err := p2p.LoadOrGenNodeKey(nodeKeyFilePath)
	if err != nil {
		return nil, err
	}
	s.NodeKey = nodeKey

	logOption, err := log.AllowLevel(s.Config.LogLevel)
	if err != nil {
		return nil, err
	}
	s.FilteredLogger = log.NewFilter(logger, logOption)

	logger.Info("tenderseed",
		"key", nodeKey.ID(),
		"listen", s.Config.ListenAddress,
		"chain", chainID,
		"log-level", s.Config.LogLevel,
		"strict-routing", s.Config.AddrBookStrict,
		"max-inbound", s.Config.MaxNumInboundPeers,
		"max-outbound", s.Config.MaxNumOutboundPeers,
		"max-packet-msg-payload-size", s.Config.MaxPacketMsgPayloadSize,
		"allow-duplicate-ip", s.Config.AllowDuplicateIP,
		"seed-disconnect-wait-period", disconnectWait,
		"peer-check-period", checkPeriod,
		"peer-check-workers", checkWorkers,
	)

	protocolVersion := p2p.NewProtocolVersion(
		version.P2PProtocol,
		version.BlockProtocol,
		0,
	)

	moniker := s.Config.Moniker
	if moniker == "" {
		moniker = fmt.Sprintf("%s-seed", chainID)
	}

	// Channels must be complete before the transport is built: the handshake
	// announces this list and nothing added later reaches it.
	channels := []byte{pex.PexChannel}

	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: protocolVersion,
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      s.Config.ListenAddress,
		Network:         chainID,
		Version:         Version,
		Channels:        channels,
		Moniker:         moniker,
	}

	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeInfo.DefaultNodeID, nodeInfo.ListenAddr))
	if err != nil {
		return nil, err
	}

	s.Transport = p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(p2pConfig))
	if err := s.Transport.Listen(*addr); err != nil {
		return nil, err
	}

	s.AddrBook = pex.NewAddrBook(addrBookFilePath, s.Config.AddrBookStrict)
	s.AddrBook.SetLogger(s.FilteredLogger.With("module", "book"))

	// Register our own address, as a full node does in node/setup.go; a seed
	// that skips it can hand its own address to its clients. With the default
	// laddr this cannot match the address peers announce us under: what
	// actually keeps verification from dialling the seed is the node ID test
	// in verify.
	s.AddrBook.AddOurAddress(addr)

	s.Reactor = NewSeedReactor(s.AddrBook, &pex.ReactorConfig{
		SeedMode: true,
		Seeds:    cmtstrings.SplitAndTrim(s.Config.Seeds, ",", " "),
		// Upstream leaves this at zero for a third-party binary, which
		// disconnects every crawled peer on the first crawl round.
		SeedDisconnectWaitPeriod: disconnectWait,
	}, checkWorkers, checkPeriod, s.FilteredLogger.With("module", "pex"))

	switchOptions := []p2p.SwitchOption{}
	if s.Config.MetricsListenAddress != "" {
		switchOptions = append(switchOptions, p2p.WithMetrics(
			p2p.PrometheusMetrics(s.Config.MetricsNamespace, "chain_id", chainID, "moniker", moniker),
		))
	}
	s.Switch = p2p.NewSwitch(p2pConfig, s.Transport, switchOptions...)
	s.Switch.SetLogger(s.FilteredLogger.With("module", "switch"))
	s.Switch.SetNodeKey(nodeKey)
	s.Switch.SetAddrBook(s.AddrBook)
	s.Switch.AddReactor("pex", s.Reactor)

	// last
	s.Switch.SetNodeInfo(nodeInfo)

	return s, nil
}

// Start starts the switch, which starts the address book and the PEX reactor.
func (s *Seed) Start() error {
	if addr := s.Config.MetricsListenAddress; addr != "" {
		s.metrics = newMetricsServer(addr)
		go func() {
			s.FilteredLogger.Info("serving metrics", "addr", addr)
			if err := s.metrics.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.FilteredLogger.Error("metrics server stopped", "err", err)
			}
		}()
	}
	if err := s.Switch.Start(); err != nil {
		// The metrics goroutine is already running; the switch is not.
		// Stop is not usable here, it also touches the address book.
		s.stopMetrics()
		return err
	}
	return nil
}

// Wait blocks until the switch stops.
func (s *Seed) Wait() {
	s.Switch.Wait()
}

// Stop saves the address book to disk and stops the switch.
func (s *Seed) Stop() error {
	// A signal can arrive between the trap and Switch.Start, in which case
	// there is nothing to save and nothing to stop.
	if !s.Switch.IsRunning() {
		s.stopMetrics()
		return nil
	}
	s.AddrBook.Save()
	s.stopMetrics()
	return s.Switch.Stop()
}

// stopMetrics shuts the metrics server down if one is running.
func (s *Seed) stopMetrics() {
	if s.metrics != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.metrics.Shutdown(ctx); err != nil {
			s.FilteredLogger.Error("could not stop metrics server", "err", err)
		}
	}
}
