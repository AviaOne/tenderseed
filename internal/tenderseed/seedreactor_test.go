package tenderseed

import (
	"io"
	"testing"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
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
		logger: testLogger(),
		addrs:  make(chan *p2p.NetAddress, workers*queuePerWorker),
		quit:   make(chan struct{}),
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
