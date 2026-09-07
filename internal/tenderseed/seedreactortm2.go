package tenderseed

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/gnolang/gno/tm2/pkg/amino"
	"github.com/gnolang/gno/tm2/pkg/p2p"
	"github.com/gnolang/gno/tm2/pkg/p2p/conn"
	"github.com/gnolang/gno/tm2/pkg/p2p/discovery"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
)

// maxAddressesServed bounds one discovery answer.
//
// The core answers with at most 30, which is not a protocol limit but the size
// of the peer set it draws from. The wire imposes none that matters here: the
// receiving side accepts 5 MB on this channel and validates the list without
// counting it, while an encoded address is on the order of 60 bytes, so 250
// addresses are three orders of magnitude below the ceiling.
//
// 250 is the selection size the Cosmos side of this binary already serves, so
// both stacks hand out the same amount and the two can be compared.
const maxAddressesServed = 250

// maxAddressesLearned bounds what one answer may add to the book.
//
// Nothing on the wire bounds it: the core validates an answer without ever
// counting its entries. The book has a ceiling, so filling it is enough to
// push out what this seed had reached. The value is what this seed serves, so
// two of these seeds talking to each other lose nothing.
const maxAddressesLearned = maxAddressesServed

// answerWindow is how long an answer may follow the request that asked for
// it. Wide on purpose: too wide costs one stale answer taken, too narrow
// costs a seed that learns nothing.
const answerWindow = time.Minute

// minServeInterval is the shortest gap between two answers served to the same
// peer.
//
// A request is ten bytes and an answer is thousands, so a peer that repeats
// gets this seed to spend its bandwidth, and a sort of the whole book under
// lock, at whatever rate it likes. The Cosmos side has this bound from its
// upstream reactor; this one had none. Legitimate peers ask on their own
// crawl tick and never come close.
const minServeInterval = time.Second

// discoveryInterval is how often the seed asks one random peer for addresses.
// It is the core's own interval: this reactor changes what a seed answers,
// never how often it asks.
const discoveryInterval = 3 * time.Second

// bookSaveInterval is how often the book is written to disk when it changed.
const bookSaveInterval = 30 * time.Second

// freshnessFactor sets how long a success stays good, as a multiple of the
// verification period.
//
// It must be more than one: a sweep takes time, and an address checked just
// before the next sweep would otherwise fall out of the answer while it was
// being rechecked, making the seed serve nothing every other minute. Three
// leaves room for a sweep to miss once without emptying the answer.
const freshnessFactor = 3

// maxConsecutiveFailures is how many failed attempts in a row an address gets
// before it leaves the book.
const maxConsecutiveFailures = 5

// dialCost is what one address costs the switch, worst case.
//
// The switch dials one address at a time, in one loop, and an address that
// never answers is paid in full: three seconds of dial context, then the two
// handshake deadlines the transport sets once the socket is up, three seconds
// each, neither of them covered by the dial context. Nine seconds, and a
// batch is limited by how many of those the period can hold.
//
// It used to say three, describing the dial alone, and called that a
// deliberate under-estimate in the same breath as it explained that
// under-estimating marks attempts that never happened. Both halves could not
// be true. Nothing was visibly wrong because the outbound limit, sixty by
// default, was the binding term either way; raise that limit and the batch
// grew past what the period could dial, so the tail of every pass was marked
// before anything reached it, counted as failed, and evicted after five
// passes. The cost is what makes the arithmetic honest at any limit, so it is
// the true worst case now, and over-estimating it costs only a slower
// rotation.
const dialCost = 9 * time.Second

// errSeedServed is the reason recorded when the seed closes a connection it
// has finished answering. It is not a failure; it is the seed's whole purpose.
var errSeedServed = errors.New("seed has served its addresses")

// errSeedQueueFull is the reason recorded when a peer stops taking what it
// asked for. Unlike errSeedServed this one is a fault and is reported as an
// error, because a peer that does not drain the channel it opened is either
// broken or doing it on purpose.
var errSeedQueueFull = errors.New("peer is not draining the discovery channel")

// cycleIntervalFloor and cycleIntervalCeiling bound how often the cycle looks
// at the peers. The interval follows the wait, so a short wait is honoured
// closely, without letting a very short one turn into a busy loop or a very
// long one delay every hang up.
const (
	cycleIntervalFloor   = time.Second
	cycleIntervalCeiling = 30 * time.Second
)

// seedChannel is the discovery channel descriptor.
//
// The channel byte comes from the core package, so it can never drift. The
// rest is local to this side of a connection and does not have to match the
// peer: each end applies its own ceiling to what it receives, and nothing in
// the handshake compares them.
//
// The ceiling is the one field where repeating the core is wrong. Five
// megabytes is what a full node sets for a channel that carries far more than
// addresses; here it is the size of a message a stranger may have this seed
// assemble, decode and validate before a single rule of this reactor runs,
// and decoding an address resolves it when it carries a name rather than an
// address. So a stranger sets the size of the work and picks the names looked
// up, once per message, as often as it likes.
//
// This holds an answer, and an answer holds at most maxAddressesServed
// addresses of about seventy bytes: tens of kilobytes. A quarter of a
// megabyte leaves more than an order of magnitude of room and cuts what one
// connection can pin by twenty. A peer that sends more has its connection
// closed by the core, before this reactor sees anything.
var seedChannel = &conn.ChannelDescriptor{
	ID:                  discovery.Channel,
	Priority:            1,
	SendQueueCapacity:   20,
	RecvMessageCapacity: 262144,
}

// SeedReactorTM2 is the discovery reactor of a TM2 seed node.
//
// It speaks the core's protocol, its channel and its message types, so the
// bytes on the wire are the ones every TM2 node already understands. What it
// replaces is everything behind them, and it replaces it because the core's
// behaviour makes a seed useless:
//
//   - A core node answers from its connected peers, capped at 30. A node
//     announcing the discovery channel alone holds 4 connections against 67
//     known addresses, so answering from connections serves 4 where the book
//     holds 67.
//   - Nothing in the core closes an idle connection. A seed that never hangs
//     up fills its own slots with peers it has already served.
//   - The core skips the routability test on purpose, to keep loopback
//     addresses usable in local clusters. A public seed handing out private
//     addresses hands out addresses nobody can dial.
//
// It owns its book rather than the core's, because the qualification of an
// address and the address itself are one state and belong in one file.
type SeedReactorTM2 struct {
	p2p.BaseReactor

	book   *SeedBook
	logger *slog.Logger

	// strict drops addresses that are not routable, honouring
	// addr_book_strict on this stack.
	strict bool

	// wait is how long a peer stays connected after being served.
	wait time.Duration

	// checkPeriod is how often addresses are re-verified. Zero disables the
	// sweep and the ageing with it.
	checkPeriod time.Duration

	// maxOutbound is the switch's own outbound limit, held here because the
	// switch discards anything above it at the moment it is handed over,
	// with a log line and nothing this reactor can hear.
	maxOutbound int

	// metrics is nil when the endpoint is disabled.
	metrics *seedTM2Metrics

	// notes is what this seed remembers of each peer it talks to: when it
	// last asked that peer for addresses, and when it last answered it.
	// Both exist to bound what one peer can make this seed do.
	notesMtx sync.Mutex
	notes    map[p2ptypes.ID]*peerNotes

	ctx      context.Context
	cancelFn context.CancelFunc
}

// NewSeedReactorTM2 builds the seed reactor.
func NewSeedReactorTM2(
	book *SeedBook,
	strict bool,
	wait time.Duration,
	checkPeriod time.Duration,
	maxOutbound int,
	metrics *seedTM2Metrics,
	logger *slog.Logger,
) *SeedReactorTM2 {
	ctx, cancelFn := context.WithCancel(context.Background())

	r := &SeedReactorTM2{
		book:        book,
		logger:      logger,
		strict:      strict,
		wait:        wait,
		checkPeriod: checkPeriod,
		maxOutbound: maxOutbound,
		metrics:     metrics,
		notes:       make(map[p2ptypes.ID]*peerNotes),
		ctx:         ctx,
		cancelFn:    cancelFn,
	}

	r.BaseReactor = *p2p.NewBaseReactor("seed", r)
	r.SetLogger(logger)

	return r
}

// GetChannels returns the discovery channel.
func (r *SeedReactorTM2) GetChannels() []*conn.ChannelDescriptor {
	return []*conn.ChannelDescriptor{seedChannel}
}

// OnStart dials what the book already holds, then runs the crawl.
func (r *SeedReactorTM2) OnStart() error {
	// Bounded like a sweep pass, and proven addresses first: a slot spent on
	// one this seed has reached can prove it again, where a slot spent on
	// hearsay may prove nothing. The whole book used to go at once, which the
	// switch takes whole, having no bound of its own beyond the outbound
	// limit it reads once at hand over.
	budget := r.sweepBudget()

	peers := r.book.FreshBatch(0, budget)
	if len(peers) == 0 {
		peers = r.book.GetPeers()
	}

	if len(peers) > budget {
		peers = peers[:budget]
	}

	if len(peers) > 0 {
		r.logger.Info("dialing known addresses", "count", len(peers), "book", r.book.Size())
		r.Switch.DialPeers(peers...)
	}

	go r.crawl()
	go r.persist()

	if r.wait > 0 {
		go r.cycle()
	}

	if r.checkPeriod > 0 {
		go r.sweep()
	} else {
		r.logger.Warn("verification disabled, served addresses will never expire")
	}

	return nil
}

// sweep re-tries the addresses that have gone stale, on peer_check_period.
//
// It hands them to the switch rather than dialling them itself. The switch is
// already the only thing that dials on this stack, it holds the outbound
// limit and the duplicate-IP rule, and a second dialler beside it would fight
// it for slots.
func (r *SeedReactorTM2) sweep() {
	ticker := time.NewTicker(r.checkPeriod)
	defer ticker.Stop()

	// One pass before the first tick. The sweep is the only thing that hands
	// addresses to the switch now, so waiting a whole period would leave
	// everything learned during it undialled.
	r.sweepOnce()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
		}

		r.sweepOnce()
	}
}

// sweepOnce is one pass of the sweep.
//
// The rule it enforces is the whole of this function: an attempt is marked
// only for an address actually handed to the switch. A failure is deduced
// from an attempt that no success followed, which is sound reasoning about a
// dial that took place and says nothing at all about one that did not. Two
// cases used to break it, and both are now taken out before anything is
// marked:
//
//   - an address this seed already holds a connection to. The switch skips it
//     silently, so no success can ever follow, so the deduction counted a
//     failure against a peer that was answering at that very moment. Six
//     sweeps of that and the book evicted a live address.
//   - an address above what the switch can take. Over its outbound limit the
//     switch discards the whole remainder with a log line, and a batch larger
//     than the period can dial leaves its tail marked before it was tried.
func (r *SeedReactorTM2) sweepOnce() {
	// The whole stale set, ordered, then narrowed. Narrowing before the
	// order would hand the ceiling to whatever the map returned first.
	stale := r.book.StaleBatch(r.freshness(), 0)

	eligible := make([]*p2ptypes.NetAddress, 0, len(stale))
	connected := 0

	for _, addr := range stale {
		if r.Switch.Peers().Has(addr.ID) {
			connected++
			continue
		}

		eligible = append(eligible, addr)
	}

	overBudget := 0

	if budget := r.sweepBudget(); len(eligible) > budget {
		overBudget = len(eligible) - budget
		eligible = eligible[:budget]
	}

	for _, addr := range eligible {
		r.book.MarkAttempt(addr)
	}

	if len(eligible) > 0 {
		r.Switch.DialPeers(eligible...)
	}

	dropped := r.book.DropFailing(maxConsecutiveFailures)

	fresh := len(r.book.FreshBatch(r.freshness(), r.servableCeiling()))

	r.metrics.observeMany(resultRetried, stageSweep, len(eligible))
	r.metrics.observeMany(resultDropped, stageSweep, dropped)
	r.metrics.observeMany(resultSkippedConnected, stageSweep, connected)
	r.metrics.observeMany(resultSkippedBudget, stageSweep, overBudget)
	r.metrics.setBook(r.book.Size(), fresh)

	r.logger.Info("verification sweep",
		"tried", len(eligible),
		"connected", connected,
		"over_budget", overBudget,
		"dropped", dropped,
		"book", r.book.Size(),
		"fresh", fresh,
	)
}

// sweepBudget is how many addresses one sweep may hand to the switch.
//
// Two ceilings, and the batch takes the lower. Slots, because the switch drops
// everything above its outbound limit at the moment it is handed over.
// Throughput, because the switch dials one at a time and a dead address costs
// a full timeout, so a batch bigger than the period can hold is a batch whose
// tail is marked again before it was ever tried.
func (r *SeedReactorTM2) sweepBudget() int {
	slots := r.maxOutbound - int(r.Switch.Peers().NumOutbound())
	if slots < 0 {
		slots = 0
	}

	throughput := int(r.checkPeriod / dialCost)

	if throughput < slots {
		return throughput
	}

	return slots
}

// servableCeiling is how many addresses this seed may call fresh.
//
// Fresh means proven recently, so a seed cannot promise it for more addresses
// than it can prove again inside the window. The window is freshnessFactor
// periods, one period proves at most one batch, so the ceiling is that
// product. What lies above stays in the book, held and unserved, waiting its
// turn to be proven rather than being handed out on an expired proof. The
// Cosmos side already serves a subset of a larger book; this is the same
// arrangement rather than a new one.
//
// A batch is bounded by two things, and so is this, by the same two: the rate
// at which the switch dials, and its outbound limit, above which a hand over
// is discarded. Taking only the first promised fresh more addresses than the
// seed could ever prove again whenever the limit was the binding one.
//
// Where the sweep reads the free slots of the moment, this reads the
// configured limit. A ceiling on what may be called fresh has to hold across
// the whole window rather than follow the connections open at the instant a
// request arrives, and reading the free slots here would empty the answer
// exactly when the seed is busiest.
//
// The answer ceiling applies on top: this bounds what may be called fresh,
// maxAddressesServed bounds what fits in one message.
func (r *SeedReactorTM2) servableCeiling() int {
	// A disabled sweep disables the ageing with it, so nothing goes stale and
	// the answer ceiling is the only one left.
	if r.checkPeriod <= 0 {
		return maxAddressesServed
	}

	batch := int(r.checkPeriod / dialCost)
	if r.maxOutbound < batch {
		batch = r.maxOutbound
	}

	// This ceiling reaches the book as a limit, where zero means no limit at
	// all, so it never falls below one. A seed whose period or whose outbound
	// limit leaves it unable to prove even one address a pass serves nothing
	// regardless: the freshness window empties the batch on its own, and one
	// is the honest floor where zero would mean the whole book.
	if batch < 1 {
		batch = 1
	}

	if ceiling := batch * freshnessFactor; ceiling < maxAddressesServed {
		return ceiling
	}

	return maxAddressesServed
}

// freshness is how long a success stays good.
func (r *SeedReactorTM2) freshness() time.Duration {
	return r.checkPeriod * freshnessFactor
}

// OnStop stops the loops and writes the book out.
func (r *SeedReactorTM2) OnStop() {
	r.cancelFn()

	if err := r.book.Flush(); err != nil {
		r.logger.Error("unable to save address book", "err", err)
	}
}

// crawl asks one random peer for addresses, on a fixed interval.
func (r *SeedReactorTM2) crawl() {
	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			peers := r.Switch.Peers().List()
			if len(peers) == 0 {
				continue
			}

			go r.request(peers[randomBelow(len(peers))])
		}
	}
}

// persist writes the book out when it has changed.
func (r *SeedReactorTM2) persist() {
	ticker := time.NewTicker(bookSaveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if err := r.book.Save(); err != nil {
				r.logger.Error("unable to save address book", "err", err)
			}
		}
	}
}

// AddPeer records a reached address and asks it for more.
//
// Only outbound peers count as verified: we chose them and we reached them.
// An inbound peer proves that it can reach us, which says nothing about
// whether anyone else can reach it.
func (r *SeedReactorTM2) AddPeer(peer p2p.PeerConn) {
	if !peer.IsOutbound() {
		return
	}

	r.book.MarkSuccess(peer.SocketAddr())

	go r.request(peer)
}

// RemovePeer forgets what this seed remembered of a peer that has gone.
//
// It no longer cancels a hang up: the cycle reads the peer set as it is, so a
// peer that has left is simply not there any more. What it clears is the
// pending request, which a peer must not be able to answer after leaving and
// coming back.
func (r *SeedReactorTM2) RemovePeer(peer p2p.PeerConn, reason any) {
	r.notesMtx.Lock()
	defer r.notesMtx.Unlock()

	delete(r.notes, peer.ID())
}

// request asks a peer for its addresses.
func (r *SeedReactorTM2) request(peer p2p.PeerConn) {
	payload, err := amino.MarshalAny(&discovery.Request{})
	if err != nil {
		r.logger.Error("unable to marshal discovery request", "err", err)
		return
	}

	// Non blocking on purpose. A blocking send waits up to ten seconds on a
	// full queue, and there is nothing to wait for: the crawl comes round
	// again in seconds, and a peer that cannot take a request now is one this
	// seed has no reason to hold a goroutine for.
	if !peer.TrySend(discovery.Channel, payload) {
		r.logger.Debug("unable to send discovery request", "peer", peer.ID())

		return
	}

	r.noteRequest(peer.ID())
}

// peerNotes is what this seed remembers of one peer.
type peerNotes struct {
	asked  time.Time
	served time.Time
}

// noteRequest records that this peer was asked, and forgets the peers whose
// last exchange is older than the window, so the table cannot grow on its own.
func (r *SeedReactorTM2) noteRequest(id p2ptypes.ID) {
	now := time.Now()

	r.notesMtx.Lock()
	defer r.notesMtx.Unlock()

	for peer, notes := range r.notes {
		if now.Sub(notes.asked) > answerWindow && now.Sub(notes.served) > answerWindow {
			delete(r.notes, peer)
		}
	}

	notes, found := r.notes[id]
	if !found {
		notes = &peerNotes{}
		r.notes[id] = notes
	}

	notes.asked = now
}

// answersOurRequest reports whether an answer from this peer follows a request
// this seed sent, and consumes that request: one request buys one answer.
//
// This is the control the Cosmos side gets from its upstream reactor, which
// refuses an address list nobody asked for. This stack has no such refusal,
// and without one an answer is the single thing a stranger controls whole:
// when it comes, how often, and what it carries.
func (r *SeedReactorTM2) answersOurRequest(id p2ptypes.ID) bool {
	r.notesMtx.Lock()
	defer r.notesMtx.Unlock()

	notes, found := r.notes[id]
	if !found || notes.asked.IsZero() {
		return false
	}

	asked := notes.asked
	notes.asked = time.Time{}

	return time.Since(asked) <= answerWindow
}

// mayServe reports whether this peer may be answered now, and records the
// answer when it may.
func (r *SeedReactorTM2) mayServe(id p2ptypes.ID) bool {
	now := time.Now()

	r.notesMtx.Lock()
	defer r.notesMtx.Unlock()

	notes, found := r.notes[id]
	if !found {
		notes = &peerNotes{}
		r.notes[id] = notes
	}

	if !notes.served.IsZero() && now.Sub(notes.served) < minServeInterval {
		return false
	}

	notes.served = now

	return true
}

// Receive handles the two discovery messages.
func (r *SeedReactorTM2) Receive(chID byte, peer p2p.PeerConn, msgBytes []byte) {
	var msg discovery.Message

	if err := amino.UnmarshalAny(msgBytes, &msg); err != nil {
		r.logger.Error("unable to unmarshal discovery message", "err", err)
		return
	}

	if err := msg.ValidateBasic(); err != nil {
		r.logger.Warn("unable to validate discovery message", "err", err)
		return
	}

	switch msg := msg.(type) {
	case *discovery.Request:
		if !r.mayServe(peer.ID()) {
			r.logger.Debug("ignoring a request that came too soon", "peer", peer.ID())
			r.metrics.observe(resultTooSoon, stageServe)

			return
		}

		if err := r.serve(peer); err != nil {
			r.logger.Warn("unable to answer discovery request", "peer", peer.ID(), "err", err)
		}
	case *discovery.Response:
		if !r.answersOurRequest(peer.ID()) {
			r.logger.Debug("ignoring addresses nobody asked for", "peer", peer.ID())
			r.metrics.observe(resultUnsolicited, stageLearn)

			return
		}

		r.learn(msg.Peers)
	default:
		r.logger.Warn("invalid discovery message received", "peer", peer.ID())
	}
}

// serve answers one request from the book, then schedules the hang up.
func (r *SeedReactorTM2) serve(peer p2p.PeerConn) error {
	addrs := r.selection(peer.ID())

	// An empty answer is not sendable: the receiving side rejects a response
	// carrying no peer. Saying nothing is the right behaviour of a seed that
	// has learned nothing yet, and the peer will ask again.
	//
	// The hang up is scheduled all the same. A peer we cannot answer holds an
	// inbound slot for as long as one we answered, and holding it serves
	// nobody: not the peer, which learns nothing by staying, and not the next
	// one, which finds the slot taken. Keeping it would be the core behaviour
	// this reactor exists to correct, reappearing on the one seed state where
	// it costs the most, the empty book of a seed that has just started.
	if len(addrs) == 0 {
		r.metrics.observe(resultEmpty, stageServe)
		r.logger.Warn("no verified address to serve",
			"peer", peer.ID(),
			"book", r.book.Size(),
			"verified", r.book.VerifiedSize(),
		)

		r.hangUp(peer)

		return nil
	}

	payload, err := amino.MarshalAny(&discovery.Response{Peers: addrs})
	if err != nil {
		return fmt.Errorf("unable to marshal discovery response, %w", err)
	}

	// Non blocking, and this one is not a comfort. serve runs on the receive
	// loop of the very peer it answers, so a blocking send stops this seed
	// from reading that peer for as long as it refuses to take the answer:
	// ten seconds, once per request, on a channel where requests are not
	// rate limited. A peer that asks and does not read is served nothing and
	// hung up on.
	if !peer.TrySend(discovery.Channel, payload) {
		r.metrics.observe(resultFailed, stageServe)
		r.Switch.StopPeerForError(peer, errSeedQueueFull)

		return fmt.Errorf("unable to send discovery response to peer %s", peer.ID())
	}

	r.metrics.observe(resultServed, stageServe)
	r.logger.Debug("served addresses",
		"peer", peer.ID(),
		"count", len(addrs),
		"book", r.book.Size(),
		"verified", r.book.VerifiedSize(),
	)

	r.hangUp(peer)

	return nil
}

// selection draws what to serve from the addresses this seed has reached
// itself, excluding the requester: handing a node back to itself wastes a slot
// and teaches it nothing.
//
// Only reached addresses are served, and that is the whole difference between
// this seed and a node that merely repeats what it was told. Measured on the
// live network: 47 of the 65 addresses announced to a fresh seed answered,
// so serving the book whole would hand out 28% addresses nobody can dial, and
// every node bootstrapping from this seed would spend its outbound slots on
// them.
//
// A seed that has reached nothing yet says nothing, rather than falling back
// to hearsay. That silence lasts seconds, the time to dial the configured
// seeds, and saying nothing is honest where repeating unverified addresses
// would not be.
func (r *SeedReactorTM2) selection(requester p2ptypes.ID) []*p2ptypes.NetAddress {
	known := r.book.FreshBatch(r.freshness(), r.servableCeiling())
	addrs := make([]*p2ptypes.NetAddress, 0, len(known))

	for _, addr := range known {
		if addr == nil || addr.ID == requester {
			continue
		}
		if !r.acceptable(addr) {
			continue
		}
		addrs = append(addrs, addr)
	}

	shuffleAddresses(addrs)

	if len(addrs) > maxAddressesServed {
		addrs = addrs[:maxAddressesServed]
	}

	return addrs
}

// acceptable reports whether an address may be served or stored.
//
// Validate is always required: the receiving side runs it on every address of
// a response and drops the whole message if one fails, so a single bad entry
// costs the entire answer. Routable is required on top when addr_book_strict
// is set, which is what that key means on this stack.
func (r *SeedReactorTM2) acceptable(addr *p2ptypes.NetAddress) bool {
	if addr == nil {
		return false
	}
	if err := addr.Validate(); err != nil {
		return false
	}
	if r.strict && !addr.Routable() {
		return false
	}
	return true
}

// learn stores what a peer sent us and offers it to the switch.
//
// Filtering on the way in and not only on the way out is deliberate: an
// unroutable address kept in the book would be dialled, would occupy an
// outbound slot, and would come back at every restart, having never been
// servable in the first place.
// What arrives past maxAddressesLearned is counted as rejected, which is what
// it is: announced and not kept.
//
// Nothing is handed to the switch here while the sweep runs, and that is the
// point. The dial queue is neither bounded nor deduplicated, nothing empties
// it and nothing reports its depth, so every hand over outside a budget was a
// deposit into a place with no bottom: the sweep's own addresses then waited
// behind it, were marked as tried long before they were dialled, counted as
// failed, and dropped although they were alive. The sweep hands over instead,
// because it is the only thing that counts what a period can dial. What is
// learned here is kept, and dialled on the next pass.
//
// Unless there is no next pass. A zero peer_check_period runs no sweep, and
// then nothing at all would ever be dialled after start up: this path would
// keep a book it never tries. So it hands over as the core does, unbounded,
// which is precisely the upstream behaviour that setting asks for.
func (r *SeedReactorTM2) learn(addrs []*p2ptypes.NetAddress) {
	kept := make([]*p2ptypes.NetAddress, 0, min(len(addrs), maxAddressesLearned))
	rejected := 0

	for _, addr := range addrs {
		if len(kept) == maxAddressesLearned || !r.acceptable(addr) {
			rejected++

			continue
		}

		kept = append(kept, addr)
	}

	r.metrics.observeMany(resultAccepted, stageLearn, len(kept))
	r.metrics.observeMany(resultRejected, stageLearn, rejected)

	if len(kept) == 0 {
		return
	}

	r.book.AddPeers(kept...)

	if r.checkPeriod <= 0 {
		r.Switch.DialPeers(kept...)
	}

	r.metrics.setBook(r.book.Size(), len(r.book.FreshBatch(r.freshness(), r.servableCeiling())))
}

// hangUp closes the connection of a peer that has had its answer.
//
// The wait is seed_disconnect_wait_period, the key the Cosmos side already
// uses for the same decision, so one setting means one thing on both stacks.
// At zero the connection goes now; the answer is sent before this is reached,
// so nothing is cut short.
//
// Above zero nothing is scheduled here, and that is the point. A timer per
// peer outlived the connection it was started for, so a peer that left and
// came back had its second visit cut short by the timer of its first. The
// cycle closes every connection older than the wait instead: one rule, one
// place, no timer to outlive anything.
func (r *SeedReactorTM2) hangUp(peer p2p.PeerConn) {
	if r.wait <= 0 {
		r.Switch.StopPeerForError(peer, errSeedServed)
	}
}

// cycle closes the connections that have lasted long enough, on their own
// loop.
func (r *SeedReactorTM2) cycle() {
	ticker := time.NewTicker(r.cycleInterval())
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
		}

		r.cycleOnce()
	}
}

// cycleOnce closes every connection older than the wait.
//
// Every connection, and that is the correction. The hang up used to be
// scheduled from the answer, so it reached the peers this seed had served and
// no others: an inbound peer that never asked for anything held its slot for
// ever, and an outbound one this seed had dialled was never re-dialled, so its
// last proof was never renewed. Both are what the release notes already
// promise to fix, and both are what the Cosmos side fixes through the core
// with this same key.
//
// Cycling an outbound peer is also what makes a held connection provable
// again: the next sweep dials it, and reaching it records a new success.
func (r *SeedReactorTM2) cycleOnce() {
	cycled := 0

	for _, peer := range r.Switch.Peers().List() {
		if peer == nil || peer.Status().Duration < r.wait {
			continue
		}

		r.Switch.StopPeerForError(peer, errSeedServed)
		cycled++
	}

	if cycled == 0 {
		return
	}

	r.metrics.observeMany(resultCycled, stageCycle, cycled)
	r.logger.Debug("cycled connections", "count", cycled)
}

// cycleInterval is how often the cycle runs, following the wait between the
// two bounds.
func (r *SeedReactorTM2) cycleInterval() time.Duration {
	if r.wait < cycleIntervalFloor {
		return cycleIntervalFloor
	}

	if r.wait > cycleIntervalCeiling {
		return cycleIntervalCeiling
	}

	return r.wait
}

// shuffleAddresses shuffles in place, so that two consecutive requesters do
// not get the same head of the book.
func shuffleAddresses(addrs []*p2ptypes.NetAddress) {
	for i := len(addrs) - 1; i > 0; i-- {
		j := randomBelow(i + 1)
		addrs[i], addrs[j] = addrs[j], addrs[i]
	}
}

// randomBelow returns a random index in [0, n), falling back to the first
// index when the random source is unavailable.
func randomBelow(n int) int {
	if n <= 1 {
		return 0
	}

	index, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}

	return int(index.Int64())
}
