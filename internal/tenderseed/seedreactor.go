package tenderseed

import (
	"errors"
	"sync"
	"sync/atomic"
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

// verifyBackoffCap bounds the exponential backoff applied to a failing
// address. It stays below the 24h ban the upstream crawler applies after
// maxAttemptsToDial, so that this reactor never becomes the slower of the two
// clocks and no third timescale appears.
const verifyBackoffCap = 4 * time.Hour

// verifyBackoffMaxShift is the failure count at which the backoff reaches its
// cap: 1<<14 seconds is 4h33m. Beyond it the shift is not computed, which also
// keeps it clear of overflow.
const verifyBackoffMaxShift = 14

// verifyStateTTL is the age past which an entry is dropped whatever the book
// says. Twice the cap, so an entry is only ever dropped once it has become
// eligible again and the counter it holds has no further effect.
const verifyStateTTL = 2 * verifyBackoffCap

// verifySweepMargin is the worst case time an address can spend in the queue:
// the queue holds workers*queuePerWorker addresses drained by workers in
// parallel, so 32 dials deep whatever the worker count, and a dial costs at
// most 7s (1s connect, then two consecutive 3s handshake deadlines in
// MultiplexTransport.Dial). 5 minutes covers 224s with room to spare.
const verifySweepMargin = 5 * time.Minute

// Outcomes of a verification decision. Every decision increments exactly one
// of these, so the sum is the number of decisions taken and the shares are
// readable. skippedLocal covers the cases where nothing was learned about the
// address itself: our own address, a banned one, a connection already open or
// under way, and the duplicate rejections below.
const (
	resultSuccess        = "success"
	resultFailure        = "failure"
	resultSkippedBackoff = "skipped_backoff"
	resultSkippedFresh   = "skipped_fresh"
	resultSkippedLocal   = "skipped_local"
	resultDroppedFull    = "dropped_full"
)

var verifyResults = []string{
	resultSuccess,
	resultFailure,
	resultSkippedBackoff,
	resultSkippedFresh,
	resultSkippedLocal,
	resultDroppedFull,
}

// verifyState is the last verdict this reactor reached on one address.
//
// It exists because verify used to be stateless, which had two costs. A dead
// address in the served selection was re-dialled at every sweep for as long as
// the upstream crawler took to evict it, roughly 35 hours, with no spacing at
// all; and an address just verified could be dialled again immediately when
// another peer mentioned it, for a second full handshake that taught nothing.
//
// One structure, one lock, two disjoint policies. A failure spaces the next
// attempt exponentially, on the same scale as the upstream crawler. A success
// suppresses re-verification for less than one period, so that the periodic
// re-check the seed exists for is never the thing being skipped.
type verifyState struct {
	addr  *p2p.NetAddress
	last  time.Time
	fails int
}

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

	// freshFor is how long a successful verdict suppresses re-verification.
	// Zero disables the suppression entirely.
	freshFor time.Duration

	mtx   sync.Mutex
	state map[p2p.ID]*verifyState

	counts  map[string]*atomic.Int64
	seen    map[string]int64
	metrics *verifyMetrics

	addrs chan *p2p.NetAddress
	quit  chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

// NewSeedReactor builds a PEX reactor in seed mode with address verification.
// A period of zero disables verification and leaves the upstream reactor alone.
// A nil metrics value disables the exported counters and nothing else.
func NewSeedReactor(book pex.AddrBook, config *pex.ReactorConfig, workers int, period time.Duration, metrics *verifyMetrics, logger log.Logger) *SeedReactor {
	if workers < 1 {
		workers = 1
	}
	r := &SeedReactor{
		Reactor:  pex.NewReactor(book, config),
		book:     book,
		logger:   logger,
		workers:  workers,
		period:   period,
		freshFor: freshWindow(period),
		state:    make(map[p2p.ID]*verifyState),
		counts:   newCounts(),
		seen:     make(map[string]int64),
		metrics:  metrics,
		addrs:    make(chan *p2p.NetAddress, workers*queuePerWorker),
		quit:     make(chan struct{}),
	}
	r.Reactor.SetLogger(logger)
	return r
}

func newCounts() map[string]*atomic.Int64 {
	counts := make(map[string]*atomic.Int64, len(verifyResults))
	for _, result := range verifyResults {
		counts[result] = new(atomic.Int64)
	}
	return counts
}

// freshWindow is how long a successful verdict is trusted, derived from the
// sweep period rather than exposed as a key of its own.
//
// It must stay strictly below the period, and by more than the worst case time
// an address spends in the queue. Were it equal to the period, a sweep would
// re-queue an address exactly as its previous verdict expires, and the queue
// traversal alone would decide whether it is re-verified or skipped: an
// arbitrary and unreproducible share of the re-verifications would vanish with
// nothing to show for it. Below the margin the window collapses to zero and
// every address is re-verified, because re-judging what is served is the point
// and the saving is only a by-product.
func freshWindow(period time.Duration) time.Duration {
	if period <= 0 {
		return 0
	}
	window := period / 2
	if margin := period - verifySweepMargin; margin < window {
		window = margin
	}
	if window <= 0 {
		return 0
	}
	return window
}

// backoffFor is the upstream formula, 2^n seconds, capped. Sharing the scale
// with pex_reactor.go:544 keeps the two accountings comparable, and leaves the
// spacing independent of peer_check_period: shortening the period to refresh
// faster would otherwise harden the punishment of failures at the same time.
func backoffFor(fails int) time.Duration {
	if fails >= verifyBackoffMaxShift {
		return verifyBackoffCap
	}
	backoff := time.Duration(1<<uint(fails)) * time.Second
	if backoff > verifyBackoffCap {
		return verifyBackoffCap
	}
	return backoff
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
	r.logger.Info("peer verification started",
		"workers", r.workers,
		"period", r.period,
		"fresh_for", r.freshFor,
		"backoff_cap", verifyBackoffCap,
	)
	return nil
}

// Stop stops the verification workers, then the upstream reactor.
func (r *SeedReactor) Stop() error {
	r.once.Do(func() { close(r.quit) })
	r.wg.Wait()
	return r.Reactor.Stop()
}

// count records one decision. Every path that ends a verification calls it
// exactly once, including the ones that dial nothing.
func (r *SeedReactor) count(result string) {
	if counter, ok := r.counts[result]; ok {
		counter.Add(1)
	}
	if r.metrics != nil {
		r.metrics.observe(result)
	}
}

// eligible reports whether an address may be dialled now, and when it may not,
// which policy held it back.
func (r *SeedReactor) eligible(addr *p2p.NetAddress) (bool, string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	state, ok := r.state[addr.ID]
	if !ok {
		return true, ""
	}
	if time.Since(state.last) >= r.waitFor(state) {
		return true, ""
	}
	if state.fails == 0 {
		return false, resultSkippedFresh
	}
	return false, resultSkippedBackoff
}

// waitFor is how long one entry holds an address back. The caller holds mtx.
func (r *SeedReactor) waitFor(state *verifyState) time.Duration {
	if state.fails == 0 {
		return r.freshFor
	}
	return backoffFor(state.fails)
}

// recordSuccess resets the entry: the address answered, whatever it did before.
func (r *SeedReactor) recordSuccess(addr *p2p.NetAddress) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.state[addr.ID] = &verifyState{addr: addr, last: time.Now(), fails: 0}
}

// recordFailure advances the backoff by one step.
func (r *SeedReactor) recordFailure(addr *p2p.NetAddress) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	state, ok := r.state[addr.ID]
	if !ok {
		state = &verifyState{}
		r.state[addr.ID] = state
	}
	state.addr = addr
	state.last = time.Now()
	if state.fails < verifyBackoffMaxShift {
		state.fails++
	}
}

// purge bounds the map. An entry whose address has left the book can never be
// served or queued again, so it is dropped, but only once it has come out of
// its wait: dropping it earlier would hand back a free dial to an address that
// was told to wait. Anything past the TTL goes regardless.
func (r *SeedReactor) purge() int {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	now := time.Now()
	dropped := 0
	for id, state := range r.state {
		age := now.Sub(state.last)
		if age > verifyStateTTL {
			delete(r.state, id)
			dropped++
			continue
		}
		if age >= r.waitFor(state) && !r.book.HasAddress(state.addr) {
			delete(r.state, id)
			dropped++
		}
	}
	return dropped
}

// runningSource is the part of p2p.Peer the queue decision needs. Keeping it
// narrow makes the decision testable without a Switch or a live connection.
type runningSource interface {
	IsRunning() bool
}

// shouldQueue reports whether the addresses carried by a PexAddrs message may
// be verified, given the state the upstream reactor left its sender in.
//
// The upstream reactor refuses a list nobody asked for: ReceiveAddrs returns
// ErrUnsolicitedList (pex_reactor.go:357), not one address of the batch reaches
// the book, and the sender is stopped and banned before Receive returns
// (:290-293). Verifying that batch anyway would let an unsolicited peer have
// addresses of its choosing dialled and promoted, defeating the very control
// the delegation just applied.
//
// A stopped sender is the signal. The book cannot be asked instead: MarkBad
// only records an address the book already holds (addrbook.go, addBadPeer
// returns false when addrLookup misses), which is precisely not the case for
// an inbound peer the seed never dialled.
//
// A sender stopped for an unrelated reason costs this batch and nothing more:
// the periodic sweep re-verifies the served selection anyway.
func (r *SeedReactor) shouldQueue(src runningSource) bool {
	// A zero peer_check_period disables verification, so no worker drains the
	// queue. Drop the addresses here rather than fill it once and then log
	// every address that follows.
	if r.period == 0 {
		return false
	}
	return src != nil && src.IsRunning()
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

	msg, ok := e.Message.(*tmp2p.PexAddrs)
	if !ok {
		return
	}
	if !r.shouldQueue(e.Src) {
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
//
// The eligibility test here is an optimisation of capacity, not the decision:
// keeping addresses that would be skipped out of the queue leaves room for the
// fresh ones arriving from PexAddrs. The test that decides is the one in
// verify, which runs whatever this one concluded.
func (r *SeedReactor) enqueue(addrs []*p2p.NetAddress) {
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		if ok, reason := r.eligible(addr); !ok {
			r.count(reason)
			continue
		}
		select {
		case r.addrs <- addr:
		case <-r.quit:
			return
		default:
			r.count(resultDroppedFull)
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
			dropped := r.purge()
			selection := r.book.GetSelection()
			r.logSweep(len(selection), dropped)
			r.enqueue(selection)
		case <-r.quit:
			return
		}
	}
}

// logSweep reports what the decisions since the previous sweep amounted to.
// It is not part of the validation protocol, which reads the exported
// counters; it is what an operator without Prometheus can see. It carries the
// same outcomes as those counters so the two cannot tell different stories.
func (r *SeedReactor) logSweep(selection, dropped int) {
	fields := []interface{}{"selection", selection, "state_dropped", dropped, "state_size", r.stateSize()}
	for _, result := range verifyResults {
		total := r.counts[result].Load()
		fields = append(fields, result, total-r.seen[result])
		r.seen[result] = total
	}
	r.logger.Info("verification sweep", fields...)
}

func (r *SeedReactor) stateSize() int {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return len(r.state)
}

// verify dials an address, marks the book accordingly, and hangs up.
func (r *SeedReactor) verify(addr *p2p.NetAddress) {
	sw := r.Reactor.Switch
	if sw == nil || addr == nil {
		return
	}
	if addr.ID == sw.NodeInfo().ID() {
		r.count(resultSkippedLocal)
		return
	}
	if r.book.IsBanned(addr) {
		r.count(resultSkippedLocal)
		return
	}
	if ok, reason := r.eligible(addr); !ok {
		r.count(reason)
		return
	}
	if sw.IsDialingOrExistingAddress(addr) {
		r.count(resultSkippedLocal)
		return
	}
	if err := sw.DialPeerWithAddress(addr); err != nil {
		if isLocalDialCollision(err) {
			r.count(resultSkippedLocal)
			return
		}
		r.recordFailure(addr)
		r.book.MarkAttempt(addr)
		r.count(resultFailure)
		r.logger.Debug("address failed verification", "addr", addr, "err", err)
		return
	}

	// MarkGood does nothing for an address the book does not know, so it has
	// to be added first. cosmoseed does these two in the opposite order.
	if err := r.book.AddAddress(addr, addr); err != nil {
		r.logger.Debug("could not add verified address", "addr", addr, "err", err)
	}
	r.book.MarkGood(addr.ID)
	r.recordSuccess(addr)
	r.count(resultSuccess)

	// Hang up straight away. NibiruChain leaves these connections open, which
	// wastes outbound slots and blurs the roles: the upstream crawler
	// collects addresses, this reactor only judges them.
	if peer := sw.Peers().Get(addr.ID); peer != nil {
		sw.StopPeerGracefully(peer)
	}
}

// isLocalDialCollision reports whether a failed dial says something about this
// node rather than about the address.
//
// Two shapes reach here. ErrCurrentlyDialingOrExistingAddress comes from
// DialPeerWithAddress itself (switch.go:610). ErrRejected with IsDuplicate
// comes from either filterConn on the connection (transport.go:378) or
// filterPeer on the ID (switch.go:846), and its usual cause is a peer that
// connected to us while we were dialling it. Counting either as a failed
// attempt would mark a live address against the book, and the counter it feeds
// is shared with the upstream crawler. Every other rejection stays a verdict:
// an incompatible network or a failed authentication is about the address.
func isLocalDialCollision(err error) bool {
	var dialing p2p.ErrCurrentlyDialingOrExistingAddress
	if errors.As(err, &dialing) {
		return true
	}
	var rejected p2p.ErrRejected
	return errors.As(err, &rejected) && rejected.IsDuplicate()
}
