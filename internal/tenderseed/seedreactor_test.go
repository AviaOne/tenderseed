package tenderseed

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
)

func TestCheckPeriod(t *testing.T) {
	cases := []struct {
		value   string
		want    time.Duration
		wantErr bool
	}{
		{value: "", want: DefaultPeerCheckPeriod},
		{value: "10m", want: 10 * time.Minute},
		{value: "45s", want: 45 * time.Second},
		{value: "0s", want: 0},
		{value: "-1m", wantErr: true},
		{value: "soon", wantErr: true},
	}
	for _, c := range cases {
		got, err := Config{PeerCheckPeriod: c.value}.CheckPeriod()
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

func TestCheckWorkers(t *testing.T) {
	got, err := Config{PeerCheckWorkers: 0}.CheckWorkers()
	if err != nil || got != DefaultPeerCheckWorkers {
		t.Errorf("zero: got %d, %v, want the default %d", got, err, DefaultPeerCheckWorkers)
	}
	got, err = Config{PeerCheckWorkers: 3}.CheckWorkers()
	if err != nil || got != 3 {
		t.Errorf("three: got %d, %v, want 3", got, err)
	}
	if _, err = (Config{PeerCheckWorkers: -1}).CheckWorkers(); err == nil {
		t.Error("negative: expected an error")
	}
}

// newTestReactor builds a reactor with a queue of a known size and no
// upstream side effects: nothing is started, so no dial can happen.
func newTestReactor(t *testing.T, workers int) *SeedReactor {
	t.Helper()
	return &SeedReactor{
		logger:   testLogger(),
		period:   DefaultPeerCheckPeriod,
		freshFor: freshWindow(DefaultPeerCheckPeriod),
		state:    make(map[p2p.ID]*verifyState),
		counts:   newCounts(),
		seen:     make(map[string]int64),
		addrs:    make(chan *p2p.NetAddress, workers*queuePerWorker),
		quit:     make(chan struct{}),
	}
}

func testLogger() log.Logger {
	return log.NewFilter(log.NewTMLogger(io.Discard), log.AllowNone())
}

func testAddr(t *testing.T) *p2p.NetAddress {
	t.Helper()
	id := "deadbeef00000000000000000000000000000000"
	addr, err := p2p.NewNetAddressString(id + "@1.2.3.4:26656")
	if err != nil {
		t.Fatalf("building address: %v", err)
	}
	return addr
}

// fakeSource stands in for a peer whose running state the test controls.
type fakeSource struct {
	running bool
}

func (f fakeSource) IsRunning() bool { return f.running }

func TestShouldQueue(t *testing.T) {
	r := newTestReactor(t, 1)
	r.period = time.Minute

	if !r.shouldQueue(fakeSource{running: true}) {
		t.Error("a running sender means upstream accepted the list")
	}
	if r.shouldQueue(fakeSource{running: false}) {
		t.Error("a stopped sender means upstream refused the list")
	}
	if r.shouldQueue(nil) {
		t.Error("no sender at all, nothing to trust")
	}

	r.period = 0
	if r.shouldQueue(fakeSource{running: true}) {
		t.Error("verification is disabled, nothing should be queued")
	}
}

func TestEnqueueFillsTheQueue(t *testing.T) {
	r := newTestReactor(t, 1)
	batch := make([]*p2p.NetAddress, queuePerWorker)
	for i := range batch {
		batch[i] = testAddr(t)
	}
	r.enqueue(batch)
	if len(r.addrs) != queuePerWorker {
		t.Errorf("queued %d, want %d", len(r.addrs), queuePerWorker)
	}
}

func TestEnqueueDropsWhenFull(t *testing.T) {
	r := newTestReactor(t, 1)
	batch := make([]*p2p.NetAddress, queuePerWorker*3)
	for i := range batch {
		batch[i] = testAddr(t)
	}
	r.enqueue(batch)
	if len(r.addrs) != queuePerWorker {
		t.Errorf("queued %d, want it capped at %d", len(r.addrs), queuePerWorker)
	}
}

type fakeBook struct {
	pex.AddrBook
	has map[string]bool
}

func (b fakeBook) HasAddress(addr *p2p.NetAddress) bool { return b.has[addr.String()] }

func namedAddr(t *testing.T, nibble string, port int) *p2p.NetAddress {
	t.Helper()
	id := nibble + "000000000000000000000000000000000000000"
	addr, err := p2p.NewNetAddressString(fmt.Sprintf("%s@1.2.3.4:%d", id, port))
	if err != nil {
		t.Fatalf("building address: %v", err)
	}
	return addr
}

func TestFreshWindowStaysBelowThePeriod(t *testing.T) {
	for _, tc := range []struct {
		period time.Duration
		want   time.Duration
	}{
		{0, 0},
		{4 * time.Minute, 0},
		{5 * time.Minute, 0},
		{8 * time.Minute, 3 * time.Minute},
		{10 * time.Minute, 5 * time.Minute},
		{time.Hour, 30 * time.Minute},
	} {
		if got := freshWindow(tc.period); got != tc.want {
			t.Errorf("freshWindow(%s) = %s, want %s", tc.period, got, tc.want)
		}
		if window := freshWindow(tc.period); window > 0 && window+verifySweepMargin > tc.period {
			t.Errorf("freshWindow(%s) = %s leaves less than the sweep margin", tc.period, window)
		}
	}
}

func TestBackoffMatchesUpstreamAndCaps(t *testing.T) {
	for _, tc := range []struct {
		fails int
		want  time.Duration
	}{
		{1, 2 * time.Second},
		{3, 8 * time.Second},
		{13, 8192 * time.Second},
		{verifyBackoffMaxShift, verifyBackoffCap},
		{40, verifyBackoffCap},
	} {
		if got := backoffFor(tc.fails); got != tc.want {
			t.Errorf("backoffFor(%d) = %s, want %s", tc.fails, got, tc.want)
		}
	}
}

func TestSuccessSuppressesThenReleases(t *testing.T) {
	r := newTestReactor(t, 1)
	addr := namedAddr(t, "a", 26656)

	r.recordSuccess(addr)
	if ok, reason := r.eligible(addr); ok || reason != resultSkippedFresh {
		t.Fatalf("just verified: ok=%v reason=%q", ok, reason)
	}

	r.state[addr.ID].last = time.Now().Add(-r.freshFor)
	if ok, _ := r.eligible(addr); !ok {
		t.Fatal("the window has elapsed, the address must be re-verified")
	}
}

func TestFailuresSpaceOutAndAreCapped(t *testing.T) {
	r := newTestReactor(t, 1)
	addr := namedAddr(t, "b", 26657)

	r.recordFailure(addr)
	if ok, reason := r.eligible(addr); ok || reason != resultSkippedBackoff {
		t.Fatalf("just failed: ok=%v reason=%q", ok, reason)
	}
	if got := r.state[addr.ID].fails; got != 1 {
		t.Fatalf("fails = %d, want 1", got)
	}

	for i := 0; i < 100; i++ {
		r.recordFailure(addr)
	}
	if got := r.state[addr.ID].fails; got != verifyBackoffMaxShift {
		t.Fatalf("fails = %d, want it capped at %d", got, verifyBackoffMaxShift)
	}

	r.state[addr.ID].last = time.Now().Add(-verifyBackoffCap)
	if ok, _ := r.eligible(addr); !ok {
		t.Fatal("the cap has elapsed, the address must be dialled again")
	}

	r.recordSuccess(addr)
	if got := r.state[addr.ID].fails; got != 0 {
		t.Fatalf("a success must reset the counter, got %d", got)
	}
}

func TestPurgeKeepsWaitingEntriesAndDropsTheRest(t *testing.T) {
	kept := namedAddr(t, "c", 26658)
	waiting := namedAddr(t, "d", 26659)
	gone := namedAddr(t, "e", 26660)
	stale := namedAddr(t, "f", 26661)

	r := newTestReactor(t, 1)
	r.book = fakeBook{has: map[string]bool{
		kept.String():    true,
		waiting.String(): false,
		gone.String():    false,
		stale.String():   true,
	}}

	for _, addr := range []*p2p.NetAddress{kept, waiting, gone, stale} {
		r.recordFailure(addr)
	}
	// waiting is out of the book but still inside its backoff; gone is out of
	// both; stale is in the book but older than the TTL.
	r.state[gone.ID].last = time.Now().Add(-time.Hour)
	r.state[stale.ID].last = time.Now().Add(-verifyStateTTL - time.Hour)

	if dropped := r.purge(); dropped != 2 {
		t.Fatalf("dropped %d entries, want 2", dropped)
	}
	if _, ok := r.state[kept.ID]; !ok {
		t.Error("an address still in the book must be kept")
	}
	if _, ok := r.state[waiting.ID]; !ok {
		t.Error("an address still inside its backoff must be kept")
	}
	if _, ok := r.state[gone.ID]; ok {
		t.Error("an address out of the book and out of its wait must go")
	}
	if _, ok := r.state[stale.ID]; ok {
		t.Error("an address past the TTL must go")
	}
}

func TestEveryDecisionIsCounted(t *testing.T) {
	r := newTestReactor(t, 1)
	addr := namedAddr(t, "0", 26662)

	r.recordSuccess(addr)
	r.enqueue([]*p2p.NetAddress{addr})
	if got := r.counts[resultSkippedFresh].Load(); got != 1 {
		t.Fatalf("skipped_fresh = %d, want 1", got)
	}
	if len(r.addrs) != 0 {
		t.Fatal("a suppressed address must not reach the queue")
	}
}

func TestLocalDialCollisionsAreNotVerdicts(t *testing.T) {
	if !isLocalDialCollision(p2p.ErrCurrentlyDialingOrExistingAddress{Addr: "x"}) {
		t.Error("a dial already under way says nothing about the address")
	}
	if !isLocalDialCollision(fmt.Errorf("wrapped: %w", p2p.ErrCurrentlyDialingOrExistingAddress{Addr: "x"})) {
		t.Error("the test must survive wrapping, the switch wraps its dial errors")
	}
	// The duplicate case cannot be built here: every field of p2p.ErrRejected
	// is unexported and the package offers no constructor, so only p2p itself
	// can produce one. What is checked instead is the half that matters for
	// correctness in the other direction: a rejection that is not a duplicate
	// stays a verdict on the address and must still be marked.
	if isLocalDialCollision(p2p.ErrRejected{}) {
		t.Error("a rejection that is not a duplicate is a verdict")
	}
	if isLocalDialCollision(errors.New("connection refused")) {
		t.Error("a network failure is a verdict")
	}
}
