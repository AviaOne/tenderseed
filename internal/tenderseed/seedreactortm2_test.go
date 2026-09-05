package tenderseed

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/gnolang/gno/tm2/pkg/p2p"
	"github.com/gnolang/gno/tm2/pkg/p2p/conn"
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
		next := book.StaleBatch(reactor.freshness(), len(handed))

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

		reactor, _, _ := newSweepFixture(t, time.Hour, 60)

		if got := reactor.servableCeiling(); got != maxAddressesServed {
			t.Fatalf("ceiling is %d, expected %d", got, maxAddressesServed)
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
	sent     int
}

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
