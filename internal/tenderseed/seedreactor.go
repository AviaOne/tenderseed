package tenderseed

import (
	"sync"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	tmp2p "github.com/cometbft/cometbft/proto/tendermint/p2p"
)

// queuePerWorker sizes the verification queue. 32 is minGetSelection in
// cometbft/p2p/pex/params.go, so every worker keeps one minimal selection
// in reserve.
const queuePerWorker = 32

// SeedReactor wraps the upstream PEX reactor and adds the one thing a seed
// node cannot get from it: verification of the addresses it hands out.
//
// Upstream only ever marks an address good from the consensus reactor
// (consensus/reactor.go:1030 and :1034), which a seed does not run. Its
// address book therefore stays entirely in the new buckets and the 70% bias
// towards old ones has nothing to select. This type fills that gap.
//
// Verification has two sources, and both are needed. Addresses arriving in a
// PexAddrs message are checked on arrival, which keeps the book fresh; the
// selection the seed would actually serve is re-checked periodically, which
// keeps it honest. Without the second, an address marked good once would be
// served first forever, dead or not, and the fork would be worse than the
// original.
type SeedReactor struct {
	*pex.Reactor

	book    pex.AddrBook
	logger  log.Logger
	workers int
	period  time.Duration

	addrs chan *p2p.NetAddress
	quit  chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

// NewSeedReactor builds a PEX reactor in seed mode with address verification.
// A period of zero disables verification and leaves the upstream reactor alone.
func NewSeedReactor(book pex.AddrBook, config *pex.ReactorConfig, workers int, period time.Duration, logger log.Logger) *SeedReactor {
	if workers < 1 {
		workers = 1
	}
	r := &SeedReactor{
		Reactor: pex.NewReactor(book, config),
		book:    book,
		logger:  logger,
		workers: workers,
		period:  period,
		addrs:   make(chan *p2p.NetAddress, workers*queuePerWorker),
		quit:    make(chan struct{}),
	}
	r.Reactor.SetLogger(logger)
	return r
}

// Start starts the upstream reactor, then the verification workers.
//
// Start is overridden rather than OnStart: pex.NewReactor pins the service
// implementation to the upstream reactor, so BaseService.Start would only ever
// call its OnStart, never ours.
func (r *SeedReactor) Start() error {
	if err := r.Reactor.Start(); err != nil {
		return err
	}
	if r.period == 0 {
		r.logger.Info("peer verification disabled", "reason", "peer_check_period is zero")
		return nil
	}
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.verifyWorker()
	}
	r.wg.Add(1)
	go r.sweepRoutine()
	r.logger.Info("peer verification started", "workers", r.workers, "period", r.period)
	return nil
}

// Stop stops the verification workers, then the upstream reactor.
func (r *SeedReactor) Stop() error {
	r.once.Do(func() { close(r.quit) })
	r.wg.Wait()
	return r.Reactor.Stop()
}

// Receive hands every message to the upstream reactor first, then queues the
// addresses of a PexAddrs message for verification.
//
// Delegating is not optional. ReceiveAddrs is what clears requestsSent
// (pex_reactor.go:359); skipping it would leave RequestAddrs permanently short
// circuited for that peer (:340), so the seed would never ask it for addresses
// again while the connection lasts.
func (r *SeedReactor) Receive(e p2p.Envelope) {
	r.Reactor.Receive(e)

	// A zero peer_check_period disables verification, so no worker drains the
	// queue. Drop the addresses here rather than fill it once and then log
	// every address that follows.
	if r.period == 0 {
		return
	}

	msg, ok := e.Message.(*tmp2p.PexAddrs)
	if !ok {
		return
	}
	addrs, err := p2p.NetAddressesFromProto(msg.Addrs)
	if err != nil {
		// The upstream reactor has already punished the sender for this.
		return
	}
	r.enqueue(addrs)
}

// enqueue offers addresses to the workers, dropping them when the queue is
// full. Dropping is deliberate: the periodic sweep will pick up anything
// missed, and an unbounded queue would turn a burst of addresses into memory
// pressure.
func (r *SeedReactor) enqueue(addrs []*p2p.NetAddress) {
	for _, addr := range addrs {
		select {
		case r.addrs <- addr:
		case <-r.quit:
			return
		default:
			r.logger.Debug("verification queue full, dropping address", "addr", addr)
		}
	}
}

// verifyWorker consumes the queue until the reactor stops.
func (r *SeedReactor) verifyWorker() {
	defer r.wg.Done()
	for {
		select {
		case addr := <-r.addrs:
			r.verify(addr)
		case <-r.quit:
			return
		}
	}
}

// sweepRoutine re-queues the selection the seed would actually serve, so that
// an address marked good once cannot stay good forever.
func (r *SeedReactor) sweepRoutine() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.period)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			selection := r.book.GetSelection()
			r.logger.Debug("re-verifying served selection", "count", len(selection))
			r.enqueue(selection)
		case <-r.quit:
			return
		}
	}
}

// verify dials an address, marks the book accordingly, and hangs up.
func (r *SeedReactor) verify(addr *p2p.NetAddress) {
	sw := r.Reactor.Switch
	if sw == nil || addr == nil {
		return
	}
	if addr.ID == sw.NodeInfo().ID() {
		return
	}
	if r.book.IsBanned(addr) {
		return
	}
	if sw.IsDialingOrExistingAddress(addr) {
		return
	}
	if err := sw.DialPeerWithAddress(addr); err != nil {
		if _, ok := err.(p2p.ErrCurrentlyDialingOrExistingAddress); ok {
			return
		}
		r.book.MarkAttempt(addr)
		r.logger.Debug("address failed verification", "addr", addr, "err", err)
		return
	}

	// MarkGood does nothing for an address the book does not know, so it has
	// to be added first. cosmoseed does these two in the opposite order.
	if err := r.book.AddAddress(addr, addr); err != nil {
		r.logger.Debug("could not add verified address", "addr", addr, "err", err)
	}
	r.book.MarkGood(addr.ID)

	// Hang up straight away. NibiruChain leaves these connections open, which
	// wastes outbound slots and blurs the roles: the upstream crawler
	// collects addresses, this reactor only judges them.
	if peer := sw.Peers().Get(addr.ID); peer != nil {
		sw.StopPeerGracefully(peer)
	}
}
