package tenderseed

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/gnolang/gno/tm2/pkg/amino"
	"github.com/gnolang/gno/tm2/pkg/p2p"
	"github.com/gnolang/gno/tm2/pkg/p2p/conn"
	"github.com/gnolang/gno/tm2/pkg/p2p/discovery"
	"github.com/gnolang/gno/tm2/pkg/p2p/events"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
)

// fakePeerSet is the part of the switch's peer set the sweep reads: whether a
// peer is already held, and how many outbound slots are taken.
type fakePeerSet struct {
	held        map[p2ptypes.ID]struct{}
	list        []p2p.PeerConn
	numOutbound uint64
}

func (s *fakePeerSet) Add(p2p.PeerConn) error       { return nil }
func (s *fakePeerSet) Remove(p2ptypes.ID) bool      { return false }
func (s *fakePeerSet) Get(p2ptypes.ID) p2p.PeerConn { return nil }
func (s *fakePeerSet) List() []p2p.PeerConn         { return s.list }
func (s *fakePeerSet) NumInbound() uint64           { return 0 }
func (s *fakePeerSet) NumOutbound() uint64          { return s.numOutbound }

func (s *fakePeerSet) Has(key p2ptypes.ID) bool {
	_, held := s.held[key]

	return held
}

// fakeSwitch records what it was handed instead of dialling it.
type fakeSwitch struct {
	peers   *fakePeerSet
	dialed  [][]*p2ptypes.NetAddress
	stopped []p2p.PeerConn
}

func newFakeSwitch() *fakeSwitch {
	return &fakeSwitch{peers: &fakePeerSet{held: make(map[p2ptypes.ID]struct{})}}
}

func (s *fakeSwitch) Broadcast(byte, []byte) {}
func (s *fakeSwitch) Peers() p2p.PeerSet     { return s.peers }

func (s *fakeSwitch) StopPeerForError(peer p2p.PeerConn, _ error) {
	s.stopped = append(s.stopped, peer)
}

func (s *fakeSwitch) Subscribe(events.EventFilter) (<-chan events.Event, func()) {
	return nil, func() {}
}

func (s *fakeSwitch) DialPeers(addrs ...*p2ptypes.NetAddress) {
	batch := make([]*p2ptypes.NetAddress, len(addrs))
	copy(batch, addrs)

	s.dialed = append(s.dialed, batch)
}

// lastBatch returns what the switch was handed at the last sweep.
func (s *fakeSwitch) lastBatch() []*p2ptypes.NetAddress {
	if len(s.dialed) == 0 {
		return nil
	}

	return s.dialed[len(s.dialed)-1]
}

// newSweepFixture wires a reactor onto a book and a fake switch. The period is
// an hour, so the throughput ceiling is wide and never the thing under test
// unless a test sets out to make it so.
func newSweepFixture(t *testing.T, period time.Duration, maxOutbound int) (
	*SeedReactorTM2, *SeedBook, *fakeSwitch,
) {
	t.Helper()

	book, _ := newBookForTest(t)
	sw := newFakeSwitch()

	reactor := NewSeedReactorTM2(
		book,
		false,
		time.Second,
		period,
		maxOutbound,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	reactor.SetSwitch(sw)

	return reactor, book, sw
}

// TestSweepMarksOnlyWhatItHandsOver is the regression test of the defect this
// delivery exists for: an attempt used to be marked for every stale address,
// including those the switch was never going to dial.
func TestSweepMarksOnlyWhatItHandsOver(t *testing.T) {
	t.Parallel()

	t.Run("an address already connected is neither marked nor handed over", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 60)

		held := bookAddr(t, 1)
		other := bookAddr(t, 2)

		book.AddPeers(held, other)
		sw.peers.held[held.ID] = struct{}{}

		for range maxConsecutiveFailures + 2 {
			reactor.sweepOnce()
		}

		if book.Size() != 1 {
			t.Fatalf("book holds %d addresses, expected the connected one alone", book.Size())
		}

		for _, addr := range book.GetPeers() {
			if addr.ID != held.ID {
				t.Fatalf("the book kept %s, expected the connected address", addr)
			}
		}

		for _, batch := range sw.dialed {
			for _, addr := range batch {
				if addr.ID == held.ID {
					t.Fatal("a connected address was handed to the switch")
				}
			}
		}
	})

	t.Run("nothing is marked when no slot is free", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 4)
		sw.peers.numOutbound = 4

		for n := range 3 {
			book.AddPeers(bookAddr(t, 10+n))
		}

		for range maxConsecutiveFailures + 2 {
			reactor.sweepOnce()
		}

		if book.Size() != 3 {
			t.Fatalf("book holds %d addresses, expected 3 untouched", book.Size())
		}

		if len(sw.dialed) != 0 {
			t.Fatalf("the switch was handed %d batches, expected none", len(sw.dialed))
		}
	})

	t.Run("only the free slots are used", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 5)
		sw.peers.numOutbound = 3

		for n := range 6 {
			book.AddPeers(bookAddr(t, 20+n))
		}

		reactor.sweepOnce()

		if got := len(sw.lastBatch()); got != 2 {
			t.Fatalf("the switch was handed %d addresses, expected the 2 free slots", got)
		}
	})

	t.Run("the batch is bounded by what the period can dial", func(t *testing.T) {
		t.Parallel()

		// Three dials fit in this period at the price of one dial.
		reactor, book, sw := newSweepFixture(t, 3*dialCost, 60)

		for n := range 10 {
			book.AddPeers(bookAddr(t, 30+n))
		}

		reactor.sweepOnce()

		if got := len(sw.lastBatch()); got != 3 {
			t.Fatalf("the switch was handed %d addresses, expected 3", got)
		}
	})

	t.Run("what was handed over is exactly what was marked", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, 4*dialCost, 60)

		for n := range 10 {
			book.AddPeers(bookAddr(t, 40+n))
		}

		reactor.sweepOnce()

		handed := sw.lastBatch()

		// Marked addresses are the ones the next batch will not start with,
		// since being tried is news and news sends an address to the back.
		next := book.StaleUnprovenBatch(len(handed))

		for _, before := range handed {
			for _, after := range next {
				if after.ID == before.ID {
					t.Fatalf("%s was handed over and not marked", after)
				}
			}
		}
	})
}

// TestSweepEviction pins the arithmetic. Six passes, not five: marking records
// the date that makes the next pass countable, so the first pass on a stale
// address costs nothing.
func TestSweepEviction(t *testing.T) {
	t.Parallel()

	t.Run("a dead address leaves after six passes", func(t *testing.T) {
		t.Parallel()

		reactor, book, _ := newSweepFixture(t, time.Hour, 60)
		book.AddPeers(bookAddr(t, 50))

		for pass := range 5 {
			reactor.sweepOnce()

			if book.Size() != 1 {
				t.Fatalf("the address left after %d passes, expected 6", pass+1)
			}
		}

		reactor.sweepOnce()

		if book.Size() != 0 {
			t.Fatal("the address did not leave after six passes")
		}
	})

	t.Run("a connected address never leaves", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 60)

		held := bookAddr(t, 60)
		book.AddPeers(held)
		sw.peers.held[held.ID] = struct{}{}

		for range 20 {
			reactor.sweepOnce()
		}

		if book.Size() != 1 {
			t.Fatal("a connected address was evicted")
		}
	})
}

// TestServableCeiling covers the promise: fresh means proven recently, so a
// seed may not call fresh more addresses than it can prove again in the window.
func TestServableCeiling(t *testing.T) {
	t.Parallel()

	t.Run("the ceiling follows what the period can prove", func(t *testing.T) {
		t.Parallel()

		reactor, _, _ := newSweepFixture(t, 10*dialCost, 60)

		if got := reactor.servableCeiling(); got != 10*freshnessFactor {
			t.Fatalf("ceiling is %d, expected %d", got, 10*freshnessFactor)
		}
	})

	t.Run("the answer ceiling still applies on top", func(t *testing.T) {
		t.Parallel()

		// Both bounds wide, so neither of them is the one under test.
		reactor, _, _ := newSweepFixture(t, time.Hour, 100)

		if got := reactor.servableCeiling(); got != maxAddressesServed {
			t.Fatalf("ceiling is %d, expected %d", got, maxAddressesServed)
		}
	})

	t.Run("the outbound limit bounds it as the period does", func(t *testing.T) {
		t.Parallel()

		// The period could prove far more than twenty addresses a pass; the
		// switch would discard everything above its limit, so the seed may
		// not call fresh what that limit keeps it from ever proving again.
		reactor, _, _ := newSweepFixture(t, time.Hour, 20)

		if got := reactor.servableCeiling(); got != 20*freshnessFactor {
			t.Fatalf("ceiling is %d, expected %d", got, 20*freshnessFactor)
		}
	})

	t.Run("the ceiling never reaches the value the book reads as no limit", func(t *testing.T) {
		t.Parallel()

		// Zero is what SeedBook.FreshBatch takes as no limit at all, so a
		// ceiling of zero would serve the whole book: the exact opposite of
		// what this ceiling is for.
		for _, maxOutbound := range []int{0, 1} {
			reactor, _, _ := newSweepFixture(t, time.Hour, maxOutbound)

			if got := reactor.servableCeiling(); got < 1 {
				t.Fatalf("ceiling is %d with %d outbound, expected at least 1", got, maxOutbound)
			}
		}
	})

	t.Run("what is above the ceiling is held and not served", func(t *testing.T) {
		t.Parallel()

		reactor, book, _ := newSweepFixture(t, 2*dialCost, 60)

		// Six proven addresses, a ceiling of two times the factor.
		for n := range 10 {
			addr := bookAddr(t, 70+n)
			book.AddPeers(addr)
			book.MarkSuccess(addr)
		}

		served := reactor.selection("")

		if len(served) != 2*freshnessFactor {
			t.Fatalf("served %d addresses, expected %d", len(served), 2*freshnessFactor)
		}

		if book.Size() != 10 {
			t.Fatalf("book holds %d addresses, expected all 10 held", book.Size())
		}
	})
}

// fakePeer is the part of a peer the service and the cycle read.
type fakePeer struct {
	p2p.PeerConn

	id       p2ptypes.ID
	duration time.Duration
	accepts  bool
	outbound bool
	sent     int
	flushed  int
}

func (p *fakePeer) IsOutbound() bool { return p.outbound }

func (p *fakePeer) FlushStop() { p.flushed++ }

func (p *fakePeer) ID() p2ptypes.ID { return p.id }

func (p *fakePeer) Status() conn.ConnectionStatus {
	return conn.ConnectionStatus{Duration: p.duration}
}

func (p *fakePeer) TrySend(byte, []byte) bool {
	if !p.accepts {
		return false
	}

	p.sent++

	return true
}

func (p *fakePeer) Send(byte, []byte) bool {
	panic("the seed must never use the blocking send")
}

// TestServeDoesNotBlockOnAPeer covers the defect where answering ran on the
// receive loop of the peer being answered, so a peer that stopped reading
// froze that loop for ten seconds, once per request, at will.
func TestServeDoesNotBlockOnAPeer(t *testing.T) {
	t.Parallel()

	t.Run("a peer that does not read is hung up on", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 60)

		addr := bookAddr(t, 200)
		book.AddPeers(addr)
		book.MarkSuccess(addr)

		peer := &fakePeer{id: bookAddr(t, 201).ID, accepts: false}

		if err := reactor.serve(peer); err == nil {
			t.Fatal("serving a peer that does not read reported no error")
		}

		if len(sw.stopped) != 1 {
			t.Fatalf("%d peers were hung up on, expected 1", len(sw.stopped))
		}
	})

	t.Run("a peer that reads is served and kept", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 60)

		addr := bookAddr(t, 210)
		book.AddPeers(addr)
		book.MarkSuccess(addr)

		peer := &fakePeer{id: bookAddr(t, 211).ID, accepts: true}

		if err := reactor.serve(peer); err != nil {
			t.Fatalf("unable to serve: %v", err)
		}

		if peer.sent != 1 {
			t.Fatalf("the peer received %d messages, expected 1", peer.sent)
		}

		if len(sw.stopped) != 0 {
			t.Fatal("a peer that read its answer was hung up on")
		}
	})
}

// TestCycle covers the rule that replaced the per peer timer: every connection
// old enough goes, whether it asked for anything or not.
func TestCycle(t *testing.T) {
	t.Parallel()

	t.Run("a silent peer loses its slot like a served one", func(t *testing.T) {
		t.Parallel()

		reactor, _, sw := newSweepFixture(t, time.Hour, 60)

		silent := &fakePeer{id: bookAddr(t, 220).ID, duration: 2 * time.Second}
		sw.peers.list = []p2p.PeerConn{silent}

		reactor.cycleOnce()

		if len(sw.stopped) != 1 {
			t.Fatalf("%d peers were cycled, expected the silent one", len(sw.stopped))
		}
	})

	t.Run("a young connection is left alone", func(t *testing.T) {
		t.Parallel()

		reactor, _, sw := newSweepFixture(t, time.Hour, 60)

		young := &fakePeer{id: bookAddr(t, 230).ID, duration: time.Millisecond}
		sw.peers.list = []p2p.PeerConn{young}

		reactor.cycleOnce()

		if len(sw.stopped) != 0 {
			t.Fatal("a young connection was cycled")
		}
	})

	t.Run("the interval follows the wait between its bounds", func(t *testing.T) {
		t.Parallel()

		reactor, _, _ := newSweepFixture(t, time.Hour, 60)

		if got := reactor.cycleInterval(); got != time.Second {
			t.Fatalf("interval is %s, expected the one second floor", got)
		}
	})
}

// TestOnePeerIsBounded is the regression test of what one peer could do: empty
// what the seed serves with a single answer, and fill a queue nothing bounds.
func TestOnePeerIsBounded(t *testing.T) {
	t.Parallel()

	answer := func(t *testing.T, count int) []byte {
		t.Helper()

		addrs := make([]*p2ptypes.NetAddress, 0, count)
		for n := range count {
			addrs = append(addrs, bookAddr(t, 1000+n))
		}

		payload, err := amino.MarshalAny(&discovery.Response{Peers: addrs})
		if err != nil {
			t.Fatalf("unable to marshal the answer: %v", err)
		}

		return payload
	}

	request := func(t *testing.T) []byte {
		t.Helper()

		payload, err := amino.MarshalAny(&discovery.Request{})
		if err != nil {
			t.Fatalf("unable to marshal the request: %v", err)
		}

		return payload
	}

	t.Run("an answer nobody asked for is ignored", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 900).ID, accepts: true}

		reactor.Receive(discovery.Channel, peer, answer(t, 20))

		if book.Size() != 0 {
			t.Fatalf("the book took %d addresses nobody asked for", book.Size())
		}

		if len(sw.dialed) != 0 {
			t.Fatal("an answer nobody asked for reached the switch")
		}
	})

	t.Run("an answer to our own request is taken", func(t *testing.T) {
		t.Parallel()

		reactor, book, _ := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 901).ID, accepts: true}

		reactor.noteRequest(peer.ID())
		reactor.Receive(discovery.Channel, peer, answer(t, 20))

		if book.Size() != 20 {
			t.Fatalf("the book took %d addresses, expected 20", book.Size())
		}
	})

	t.Run("one request buys one answer", func(t *testing.T) {
		t.Parallel()

		reactor, book, _ := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 902).ID, accepts: true}

		reactor.noteRequest(peer.ID())
		reactor.Receive(discovery.Channel, peer, answer(t, 5))
		reactor.Receive(discovery.Channel, peer, answer(t, 20))

		if book.Size() != 5 {
			t.Fatalf("the book took %d addresses, expected the first answer alone", book.Size())
		}
	})

	t.Run("what one answer may add is capped by what a period can prove", func(t *testing.T) {
		t.Parallel()

		// maxAddressesLearned still bounds one message, but it is no longer
		// the term that bites. What an answer may have this seed take in is
		// rated against what one period of verification is able to dial, so a
		// peer cannot decide what the sweep spends its budget on.
		reactor, book, _ := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 903).ID, accepts: true}

		reactor.noteRequest(peer.ID())
		reactor.Receive(discovery.Channel, peer, answer(t, maxAddressesLearned+50))

		if got, want := book.Size(), reactor.provableBatch(); got != want {
			t.Fatalf("the book took %d addresses, expected %d", got, want)
		}

		if reactor.provableBatch() >= maxAddressesLearned {
			t.Fatal("this fixture no longer makes the allowance the binding term")
		}
	})

	t.Run("learning hands nothing to the switch", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 904).ID, accepts: true}

		reactor.noteRequest(peer.ID())
		reactor.Receive(discovery.Channel, peer, answer(t, 40))

		if book.Size() != 40 {
			t.Fatalf("the book took %d addresses, expected all 40 kept", book.Size())
		}

		if len(sw.dialed) != 0 {
			t.Fatalf("learning handed %d batches to the switch, expected none", len(sw.dialed))
		}
	})

	t.Run("a peer that has left cannot answer afterwards", func(t *testing.T) {
		t.Parallel()

		reactor, book, _ := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 905).ID, accepts: true}

		reactor.noteRequest(peer.ID())
		reactor.RemovePeer(peer, nil)
		reactor.Receive(discovery.Channel, peer, answer(t, 10))

		if book.Size() != 0 {
			t.Fatalf("the book took %d addresses after the peer had left", book.Size())
		}
	})

	t.Run("requests coming too fast are not all answered", func(t *testing.T) {
		t.Parallel()

		reactor, book, _ := newSweepFixture(t, time.Hour, 60)
		book.AddPeers(bookAddr(t, 906))
		book.MarkSuccess(bookAddr(t, 906))

		peer := &fakePeer{id: bookAddr(t, 907).ID, accepts: true}
		payload := request(t)

		for range 5 {
			reactor.Receive(discovery.Channel, peer, payload)
		}

		if peer.sent != 1 {
			t.Fatalf("the seed answered %d times in a row, expected 1", peer.sent)
		}
	})
}

// TestImmediateHangUpFlushes covers the answer that was queued and thrown away
// by the close that followed it, while being counted as served.
func TestImmediateHangUpFlushes(t *testing.T) {
	t.Parallel()

	reactor, book, sw := newSweepFixture(t, time.Hour, 60)
	reactor.wait = 0

	served := bookAddr(t, 1400)
	book.AddPeers(served)
	book.MarkSuccess(served)

	peer := &fakePeer{id: bookAddr(t, 1401).ID, accepts: true}

	if err := reactor.serve(peer); err != nil {
		t.Fatalf("unable to serve: %v", err)
	}

	if peer.flushed != 1 {
		t.Fatalf("the peer was flushed %d times, expected once before the close", peer.flushed)
	}

	if len(sw.stopped) != 1 {
		t.Fatalf("%d peers were stopped, expected one", len(sw.stopped))
	}
}

// TestStartDialsTheWholeBookWithoutASweep covers what a zero peer_check_period
// asks for: the upstream behaviour, which dials the book at start up. Read as
// a budget, zero left a seed that dialled nothing at all.
func TestStartDialsTheWholeBookWithoutASweep(t *testing.T) {
	t.Parallel()

	reactor, book, sw := newSweepFixture(t, 0, 60)

	for n := range 4 {
		book.AddPeers(bookAddr(t, 1410+n))
	}

	if err := reactor.OnStart(); err != nil {
		t.Fatalf("unable to start: %v", err)
	}

	defer reactor.OnStop()

	if got := len(sw.lastBatch()); got != 4 {
		t.Fatalf("the switch was handed %d addresses, expected the whole book", got)
	}
}

// TestRequestIsNotedBeforeItIsSent covers the window an answer could fall into.
func TestRequestIsNotedBeforeItIsSent(t *testing.T) {
	t.Parallel()

	t.Run("a request that fails to leave is taken back", func(t *testing.T) {
		t.Parallel()

		reactor, _, _ := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 1420).ID, accepts: false}

		reactor.request(peer)

		if reactor.answersOurRequest(peer.ID()) {
			t.Fatal("a request that never left bought an answer")
		}
	})

	t.Run("a request that left buys one answer", func(t *testing.T) {
		t.Parallel()

		reactor, _, _ := newSweepFixture(t, time.Hour, 60)
		peer := &fakePeer{id: bookAddr(t, 1421).ID, accepts: true}

		reactor.request(peer)

		if !reactor.answersOurRequest(peer.ID()) {
			t.Fatal("a request that left did not buy an answer")
		}
	})
}

// TestRenewalComesBeforeExploration is the regression test of the critical
// defect: a peer that answers with addresses nobody can reach used to take the
// whole sweep budget, so the addresses this seed had proven were never proven
// again and the served set emptied while the book still held them.
func TestRenewalComesBeforeExploration(t *testing.T) {
	t.Parallel()

	t.Run("an expired proof is renewed before hearsay is explored", func(t *testing.T) {
		t.Parallel()

		// Two dials fit in this period, and there is far more hearsay than
		// that: without the two classes the proven address never comes up.
		reactor, book, sw := newSweepFixture(t, 2*dialCost, 60)

		proven := bookAddr(t, 300)
		book.AddPeers(proven)
		book.MarkSuccess(proven)
		book.peers[proven.String()].lastOK = time.Now().Add(-time.Hour)

		for n := range 50 {
			book.AddPeers(bookAddr(t, 310+n))
		}

		reactor.sweepOnce()

		batch := sw.lastBatch()
		if len(batch) != 2 {
			t.Fatalf("the switch was handed %d addresses, expected 2", len(batch))
		}

		if batch[0].ID != proven.ID {
			t.Fatalf("the batch starts with %s, expected the expired proof", batch[0])
		}
	})

	t.Run("continuous injection does not empty the served set", func(t *testing.T) {
		t.Parallel()

		reactor, book, sw := newSweepFixture(t, 10*dialCost, 60)

		// One address this seed has reached and keeps reaching.
		proven := bookAddr(t, 400)
		book.AddPeers(proven)
		book.MarkSuccess(proven)

		for pass := range 6 {
			// The book is filled directly, so this test measures the two
			// classes alone: the allowance would otherwise stop the flood
			// before the sweep ever saw it, and both are needed.
			for n := range 200 {
				book.AddPeers(bookAddr(t, 1000+pass*200+n))
			}

			// The proof expires, so the sweep has to renew it.
			book.peers[proven.String()].lastOK = time.Now().Add(-time.Hour)

			reactor.sweepOnce()

			// Whatever the switch was handed is dialled: the proven address
			// answers, the invented ones do not.
			for _, addr := range sw.lastBatch() {
				if addr.ID == proven.ID {
					book.MarkSuccess(addr)
				}
			}
		}

		if len(book.Fresh(reactor.freshness())) == 0 {
			t.Fatal("the served set emptied under injection")
		}

		if !book.Knows(proven) {
			t.Fatal("the proven address left the book under injection")
		}
	})
}

// TestExplorationAllowance pins what one peer may make this seed take in.
func TestExplorationAllowance(t *testing.T) {
	t.Parallel()

	t.Run("a peer may not name more than a period can prove", func(t *testing.T) {
		t.Parallel()

		// Three dials fit in this period, so three unknown addresses do too.
		reactor, book, _ := newSweepFixture(t, 3*dialCost, 60)

		peer := bookAddr(t, 500).ID

		flood := make([]*p2ptypes.NetAddress, 0, 20)
		for n := range 20 {
			flood = append(flood, bookAddr(t, 510+n))
		}

		reactor.learn(peer, flood)

		if got := book.Size(); got != 3 {
			t.Fatalf("the book took %d addresses, expected the 3 a period can prove", got)
		}

		// A second answer in the same window adds nothing more.
		reactor.learn(peer, flood)

		if got := book.Size(); got != 3 {
			t.Fatalf("the book took %d addresses over two answers, expected 3", got)
		}
	})

	t.Run("an address the book already holds costs no allowance", func(t *testing.T) {
		t.Parallel()

		reactor, book, _ := newSweepFixture(t, dialCost, 60)

		known := bookAddr(t, 600)
		book.AddPeers(known)

		peer := bookAddr(t, 601).ID

		// One known address, repeated, never spends the allowance.
		for range 5 {
			reactor.learn(peer, []*p2ptypes.NetAddress{known})
		}

		if got := book.Size(); got != 1 {
			t.Fatalf("the book holds %d addresses, expected 1", got)
		}

		// The allowance is therefore still whole for a new one.
		reactor.learn(peer, []*p2ptypes.NetAddress{bookAddr(t, 602)})

		if got := book.Size(); got != 2 {
			t.Fatalf("the book holds %d addresses, expected the new one taken", got)
		}
	})

	t.Run("a silent peer does not get a fresh allowance by waiting", func(t *testing.T) {
		t.Parallel()

		reactor, _, _ := newSweepFixture(t, dialCost, 60)

		peer := bookAddr(t, 700).ID

		reactor.learn(peer, []*p2ptypes.NetAddress{bookAddr(t, 701)})

		// The purge runs on every request this seed sends out. It must not
		// drop an entry whose exploration window is still open.
		reactor.noteRequest(bookAddr(t, 702).ID)

		reactor.notesMtx.Lock()
		notes, found := reactor.notes[peer]
		reactor.notesMtx.Unlock()

		if !found {
			t.Fatal("the peer's entry was purged while its window was open")
		}

		if notes.learned != 1 {
			t.Fatalf("the peer's count is %d, expected 1", notes.learned)
		}
	})
}

// TestCrawlAsksOutboundPeersOnly covers the cost of the attack rather than the
// attack itself: what an inbound peer says is worth what it costs to become
// one.
func TestCrawlAsksOutboundPeersOnly(t *testing.T) {
	t.Parallel()

	inbound := &fakePeer{id: bookAddr(t, 800).ID, accepts: true}
	outbound := &fakePeer{id: bookAddr(t, 801).ID, accepts: true, outbound: true}

	kept := outboundOnly([]p2p.PeerConn{inbound, outbound, nil})

	if len(kept) != 1 {
		t.Fatalf("%d peers kept, expected the outbound one alone", len(kept))
	}

	if kept[0].ID() != outbound.ID() {
		t.Fatalf("kept %s, expected the outbound peer", kept[0].ID())
	}
}

// TestLearnDialsWhenTheSweepIsOff covers what a zero peer_check_period asks
// for: no verification, and the upstream behaviour back, which means learning
// hands addresses over because nothing else will.
func TestLearnDialsWhenTheSweepIsOff(t *testing.T) {
	t.Parallel()

	addrs := make([]*p2ptypes.NetAddress, 0, 4)
	for n := range 4 {
		addrs = append(addrs, bookAddr(t, 1200+n))
	}

	payload, err := amino.MarshalAny(&discovery.Response{Peers: addrs})
	if err != nil {
		t.Fatalf("unable to marshal the answer: %v", err)
	}

	reactor, book, sw := newSweepFixture(t, 0, 60)
	peer := &fakePeer{id: bookAddr(t, 1210).ID, accepts: true}

	reactor.noteRequest(peer.ID())
	reactor.Receive(discovery.Channel, peer, payload)

	if book.Size() != 4 {
		t.Fatalf("the book took %d addresses, expected 4", book.Size())
	}

	if got := len(sw.lastBatch()); got != 4 {
		t.Fatalf("the switch was handed %d addresses, expected 4", got)
	}
}
