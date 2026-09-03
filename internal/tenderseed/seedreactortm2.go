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

// errSeedServed is the reason recorded when the seed closes a connection it
// has finished answering. It is not a failure; it is the seed's whole purpose.
var errSeedServed = errors.New("seed has served its addresses")

// seedChannel is the discovery channel descriptor.
//
// Every field repeats the core's, which is not a choice: the descriptors of a
// connection come from the registered reactors on each side, and two nodes
// that describe the same channel differently do not agree on its queue or its
// message ceiling. The channel byte itself comes from the core package, so it
// can never drift.
var seedChannel = &conn.ChannelDescriptor{
	ID:                  discovery.Channel,
	Priority:            1,
	SendQueueCapacity:   20,
	RecvMessageCapacity: 5242880,
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

	// metrics is nil when the endpoint is disabled.
	metrics *seedTM2Metrics

	mtx       sync.Mutex
	scheduled map[p2ptypes.ID]struct{}

	ctx      context.Context
	cancelFn context.CancelFunc
}

// NewSeedReactorTM2 builds the seed reactor.
func NewSeedReactorTM2(
	book *SeedBook,
	strict bool,
	wait time.Duration,
	checkPeriod time.Duration,
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
		metrics:     metrics,
		scheduled:   make(map[p2ptypes.ID]struct{}),
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
	if peers := r.book.GetPeers(); len(peers) > 0 {
		r.logger.Info("dialing known addresses", "count", len(peers))
		r.Switch.DialPeers(peers...)
	}

	go r.crawl()
	go r.persist()

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

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
		}

		stale := r.book.Stale(r.freshness())
		if len(stale) == 0 {
			continue
		}

		for _, addr := range stale {
			r.book.MarkAttempt(addr)
		}

		r.Switch.DialPeers(stale...)

		dropped := r.book.DropFailing(maxConsecutiveFailures)

		r.metrics.observeMany(resultRetried, stageSweep, len(stale))
		r.metrics.observeMany(resultDropped, stageSweep, dropped)
		r.metrics.setBook(r.book.Size(), len(r.book.Fresh(r.freshness())))

		r.logger.Info("verification sweep",
			"tried", len(stale),
			"dropped", dropped,
			"book", r.book.Size(),
			"fresh", len(r.book.Fresh(r.freshness())),
		)
	}
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

// RemovePeer forgets a peer's pending hang up.
func (r *SeedReactorTM2) RemovePeer(peer p2p.PeerConn, reason any) {
	r.forget(peer.ID())
}

// request asks a peer for its addresses.
func (r *SeedReactorTM2) request(peer p2p.PeerConn) {
	payload, err := amino.MarshalAny(&discovery.Request{})
	if err != nil {
		r.logger.Error("unable to marshal discovery request", "err", err)
		return
	}

	if !peer.Send(discovery.Channel, payload) {
		r.logger.Debug("unable to send discovery request", "peer", peer.ID())
	}
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
		if err := r.serve(peer); err != nil {
			r.logger.Warn("unable to answer discovery request", "peer", peer.ID(), "err", err)
		}
	case *discovery.Response:
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

		r.scheduleDisconnect(peer)

		return nil
	}

	payload, err := amino.MarshalAny(&discovery.Response{Peers: addrs})
	if err != nil {
		return fmt.Errorf("unable to marshal discovery response, %w", err)
	}

	if !peer.Send(discovery.Channel, payload) {
		r.metrics.observe(resultFailed, stageServe)
		return fmt.Errorf("unable to send discovery response to peer %s", peer.ID())
	}

	r.metrics.observe(resultServed, stageServe)
	r.logger.Debug("served addresses",
		"peer", peer.ID(),
		"count", len(addrs),
		"book", r.book.Size(),
		"verified", r.book.VerifiedSize(),
	)

	r.scheduleDisconnect(peer)

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
	known := r.book.Fresh(r.freshness())
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
func (r *SeedReactorTM2) learn(addrs []*p2ptypes.NetAddress) {
	kept := make([]*p2ptypes.NetAddress, 0, len(addrs))

	for _, addr := range addrs {
		if r.acceptable(addr) {
			kept = append(kept, addr)
		}
	}

	r.metrics.observeMany(resultAccepted, stageLearn, len(kept))
	r.metrics.observeMany(resultRejected, stageLearn, len(addrs)-len(kept))

	if len(kept) == 0 {
		return
	}

	r.book.AddPeers(kept...)
	r.Switch.DialPeers(kept...)
	r.metrics.setBook(r.book.Size(), len(r.book.Fresh(r.freshness())))
}

// scheduleDisconnect closes the connection once the peer has had its answer.
//
// The wait is seed_disconnect_wait_period, the key the Cosmos side already
// uses for the same decision, so one setting means one thing on both stacks.
// It is not zero by default because the answer is sent asynchronously: hanging
// up the instant Send returns would race the send queue draining.
//
// At most one timer per peer: a peer that asks repeatedly is not granted a
// longer stay than one that asks once.
func (r *SeedReactorTM2) scheduleDisconnect(peer p2p.PeerConn) {
	if r.wait <= 0 {
		r.Switch.StopPeerForError(peer, errSeedServed)
		return
	}

	id := peer.ID()

	r.mtx.Lock()
	if _, pending := r.scheduled[id]; pending {
		r.mtx.Unlock()
		return
	}
	r.scheduled[id] = struct{}{}
	r.mtx.Unlock()

	go func() {
		timer := time.NewTimer(r.wait)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-r.ctx.Done():
			r.forget(id)
			return
		}

		r.forget(id)

		// The peer may have left on its own in the meantime.
		if p := r.Switch.Peers().Get(id); p != nil {
			r.Switch.StopPeerForError(p, errSeedServed)
		}
	}()
}

func (r *SeedReactorTM2) forget(id p2ptypes.ID) {
	r.mtx.Lock()
	delete(r.scheduled, id)
	r.mtx.Unlock()
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
