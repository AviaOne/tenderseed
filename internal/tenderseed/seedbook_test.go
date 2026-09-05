package tenderseed

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gnolang/gno/tm2/pkg/crypto"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
)

// bookAddr builds a well formed address whose identity is derived from n, so
// two calls with the same n give the same address and two calls with different
// n never collide.
//
// The identity is built from an address rather than written out, because the
// stack validates it by decoding it: any string that is not a real encoded
// address is refused before the book ever sees it.
func bookAddr(t *testing.T, n int) *p2ptypes.NetAddress {
	t.Helper()

	id := crypto.AddressFromPreimage([]byte(fmt.Sprintf("tenderseed-test-%d", n))).ID()

	addr, err := p2ptypes.NewNetAddressFromString(
		fmt.Sprintf("%s@10.0.%d.%d:26656", id, n/256%256, n%256),
	)
	if err != nil {
		t.Fatalf("unable to build test address %d: %v", n, err)
	}

	return addr
}

// newBookForTest returns an empty non strict book backed by a file in a
// temporary directory, and the path of that file.
func newBookForTest(t *testing.T) (*SeedBook, string) {
	t.Helper()

	return newBookForTestWith(t, false)
}

// newStrictBookForTest is the same book with addr_book_strict set.
func newStrictBookForTest(t *testing.T) (*SeedBook, string) {
	t.Helper()

	return newBookForTestWith(t, true)
}

func newBookForTestWith(t *testing.T, strict bool) (*SeedBook, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "addrbook.json")
	self := bookAddr(t, 999)

	book, err := NewSeedBook(path, *self, strict, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unable to open book: %v", err)
	}

	return book, path
}

func TestSeedBookAddPeers(t *testing.T) {
	t.Parallel()

	t.Run("an address is stored once", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		book.AddPeers(addr, addr, bookAddr(t, 1))

		if got := book.Size(); got != 1 {
			t.Fatalf("size: got %d, want 1", got)
		}
	})

	t.Run("our own address is never stored", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		book.AddPeers(bookAddr(t, 999), bookAddr(t, 1))

		if got := book.Size(); got != 1 {
			t.Fatalf("size: got %d, want 1", got)
		}
	})

	t.Run("a known address is not verified by hearsay", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		book.AddPeers(bookAddr(t, 1))

		if got := book.VerifiedSize(); got != 0 {
			t.Fatalf("verified: got %d, want 0", got)
		}
		if got := len(book.Verified()); got != 0 {
			t.Fatalf("verified list: got %d, want 0", got)
		}
	})

	t.Run("the book evicts the oldest first", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		book.maxPeers = 2

		book.AddPeers(bookAddr(t, 1))
		time.Sleep(2 * time.Millisecond)
		book.AddPeers(bookAddr(t, 2))
		time.Sleep(2 * time.Millisecond)
		book.AddPeers(bookAddr(t, 3))

		if got := book.Size(); got != 2 {
			t.Fatalf("size: got %d, want 2", got)
		}

		for _, addr := range book.GetPeers() {
			if addr.String() == bookAddr(t, 1).String() {
				t.Fatal("the oldest address survived the eviction")
			}
		}
	})
}

func TestSeedBookVerification(t *testing.T) {
	t.Parallel()

	t.Run("a success clears the failures", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		book.MarkAttempt(addr)
		book.MarkAttempt(addr)
		book.MarkSuccess(addr)

		if got := book.peers[addr.String()].fails; got != 0 {
			t.Fatalf("fails: got %d, want 0", got)
		}
		if got := book.VerifiedSize(); got != 1 {
			t.Fatalf("verified: got %d, want 1", got)
		}
	})

	t.Run("a failure is deduced from an attempt without a success", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		// The first attempt has nothing before it to judge.
		book.MarkAttempt(addr)
		if got := book.peers[addr.String()].fails; got != 0 {
			t.Fatalf("fails after one attempt: got %d, want 0", got)
		}

		book.MarkAttempt(addr)
		book.MarkAttempt(addr)
		if got := book.peers[addr.String()].fails; got != 2 {
			t.Fatalf("fails after three attempts: got %d, want 2", got)
		}
	})

	t.Run("an address that keeps failing leaves the book", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		for range 4 {
			book.MarkAttempt(addr)
		}

		if got := book.DropFailing(5); got != 0 {
			t.Fatalf("dropped below the limit: got %d, want 0", got)
		}

		book.MarkAttempt(addr)
		book.MarkAttempt(addr)

		if got := book.DropFailing(5); got != 1 {
			t.Fatalf("dropped at the limit: got %d, want 1", got)
		}
		if got := book.Size(); got != 0 {
			t.Fatalf("size: got %d, want 0", got)
		}
	})

	t.Run("a limit of zero drops nothing", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		for range 10 {
			book.MarkAttempt(addr)
		}

		if got := book.DropFailing(0); got != 0 {
			t.Fatalf("dropped: got %d, want 0", got)
		}
	})
}

func TestSeedBookSelectionWindows(t *testing.T) {
	t.Parallel()

	t.Run("a success ages out of the fresh set", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		book.MarkSuccess(addr)
		book.peers[addr.String()].lastOK = time.Now().Add(-time.Hour)

		if got := len(book.Fresh(time.Minute)); got != 0 {
			t.Fatalf("fresh: got %d, want 0", got)
		}
		if got := len(book.Verified()); got != 1 {
			t.Fatalf("verified: got %d, want 1", got)
		}
	})

	t.Run("a window of zero disables the ageing", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		book.MarkSuccess(addr)
		book.peers[addr.String()].lastOK = time.Now().Add(-time.Hour)

		if got := len(book.Fresh(0)); got != 1 {
			t.Fatalf("fresh: got %d, want 1", got)
		}
	})

	t.Run("stale holds what was never reached", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		fresh := bookAddr(t, 1)

		book.AddPeers(bookAddr(t, 2))
		book.MarkSuccess(fresh)

		stale := book.Stale(time.Minute)
		if len(stale) != 1 {
			t.Fatalf("stale: got %d, want 1", len(stale))
		}
		if stale[0].String() == fresh.String() {
			t.Fatal("a freshly reached address was called stale")
		}
	})

	t.Run("the caller cannot reach into the book", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		addr := bookAddr(t, 1)

		book.AddPeers(addr)

		got := book.GetPeers()
		if len(got) != 1 {
			t.Fatalf("peers: got %d, want 1", len(got))
		}

		before := book.peers[addr.String()].addr.IP.String()

		// The last byte, because a parsed address is carried on sixteen
		// bytes whose leading ones are already zero.
		got[0].IP[len(got[0].IP)-1] ^= 0xff

		if after := book.peers[addr.String()].addr.IP.String(); after != before {
			t.Fatalf("the book was modified through the returned slice: %s became %s", before, after)
		}
	})
}

func TestSeedBookPersistence(t *testing.T) {
	t.Parallel()

	t.Run("what was learned survives a restart", func(t *testing.T) {
		t.Parallel()

		book, path := newBookForTest(t)
		reached := bookAddr(t, 1)
		failing := bookAddr(t, 2)

		book.MarkSuccess(reached)
		book.MarkAttempt(failing)
		book.MarkAttempt(failing)

		if err := book.Save(); err != nil {
			t.Fatalf("unable to save: %v", err)
		}

		again, err := NewSeedBook(path, *bookAddr(t, 999), false,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("unable to reopen: %v", err)
		}

		if got := again.Size(); got != 2 {
			t.Fatalf("size: got %d, want 2", got)
		}
		if got := again.VerifiedSize(); got != 1 {
			t.Fatalf("verified: got %d, want 1", got)
		}
		if got := again.peers[failing.String()].fails; got != 1 {
			t.Fatalf("fails: got %d, want 1", got)
		}
		if again.peers[reached.String()].lastOK.IsZero() {
			t.Fatal("the success was not kept")
		}
	})

	t.Run("an unchanged book is not rewritten", func(t *testing.T) {
		t.Parallel()

		book, path := newBookForTest(t)
		book.AddPeers(bookAddr(t, 1))

		if err := book.Save(); err != nil {
			t.Fatalf("unable to save: %v", err)
		}

		first, err := os.Stat(path)
		if err != nil {
			t.Fatalf("unable to stat: %v", err)
		}

		time.Sleep(10 * time.Millisecond)

		if err := book.Save(); err != nil {
			t.Fatalf("unable to save again: %v", err)
		}

		second, err := os.Stat(path)
		if err != nil {
			t.Fatalf("unable to stat again: %v", err)
		}

		if !first.ModTime().Equal(second.ModTime()) {
			t.Fatal("an unchanged book was written twice")
		}
	})

	t.Run("a flush writes whether or not anything changed", func(t *testing.T) {
		t.Parallel()

		book, path := newBookForTest(t)
		book.AddPeers(bookAddr(t, 1))

		if err := book.Flush(); err != nil {
			t.Fatalf("unable to flush: %v", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("nothing was written: %v", err)
		}
	})

	t.Run("a missing file is not an error", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "absent.json")

		book, err := NewSeedBook(path, *bookAddr(t, 999), false,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("a missing file was refused: %v", err)
		}
		if got := book.Size(); got != 0 {
			t.Fatalf("size: got %d, want 0", got)
		}
		if _, err := os.Stat(path); err == nil {
			t.Fatal("opening the book created the file")
		}
	})

	t.Run("an unreadable file is copied aside and the seed still starts", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "addrbook.json")
		if err := os.WriteFile(path, []byte("{ this is not json"), 0o644); err != nil {
			t.Fatalf("unable to write: %v", err)
		}

		book, err := NewSeedBook(path, *bookAddr(t, 999), false,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("a corrupt file stopped the seed: %v", err)
		}
		if got := book.Size(); got != 0 {
			t.Fatalf("size: got %d, want 0", got)
		}

		copied, err := os.ReadFile(path + ".corrupt")
		if err != nil {
			t.Fatalf("no copy was kept: %v", err)
		}
		if string(copied) != "{ this is not json" {
			t.Fatal("the copy does not hold the original bytes")
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("the original was moved rather than copied: %v", err)
		}
	})

	t.Run("one unreadable address does not cost the book", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "addrbook.json")
		content := fmt.Sprintf(
			`{"peers":[{"addr":"not-an-address","last_seen":1},{"addr":"%s","last_seen":1}]}`,
			bookAddr(t, 1).String(),
		)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("unable to write: %v", err)
		}

		book, err := NewSeedBook(path, *bookAddr(t, 999), false,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("unable to open: %v", err)
		}
		if got := book.Size(); got != 1 {
			t.Fatalf("size: got %d, want 1", got)
		}
	})

	t.Run("a book written elsewhere reads with the core fields alone", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "addrbook.json")
		content := fmt.Sprintf(
			`{"peers":[{"addr":"%s","last_seen":1700000000}]}`,
			bookAddr(t, 1).String(),
		)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("unable to write: %v", err)
		}

		book, err := NewSeedBook(path, *bookAddr(t, 999), false,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("unable to open: %v", err)
		}
		if got := book.Size(); got != 1 {
			t.Fatalf("size: got %d, want 1", got)
		}
		if got := book.VerifiedSize(); got != 0 {
			t.Fatalf("an unqualified address came back verified: got %d, want 0", got)
		}
	})

	t.Run("our own address is dropped on load", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "addrbook.json")
		content := fmt.Sprintf(
			`{"peers":[{"addr":"%s","last_seen":1},{"addr":"%s","last_seen":1}]}`,
			bookAddr(t, 999).String(), bookAddr(t, 1).String(),
		)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("unable to write: %v", err)
		}

		book, err := NewSeedBook(path, *bookAddr(t, 999), false,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("unable to open: %v", err)
		}
		if got := book.Size(); got != 1 {
			t.Fatalf("size: got %d, want 1", got)
		}
	})
}

// routableAddr builds a well formed address that passes the routability test.
//
// The ordinary test addresses live in 10.0.x.x, a private range, so none of
// them is routable and none of them can show that addr_book_strict lets the
// right addresses through. 203.0.113.0/24 is reserved for documentation and
// belongs to none of the ranges the core refuses, which is what makes it
// usable here and unusable anywhere else.
func routableAddr(t *testing.T, n int) *p2ptypes.NetAddress {
	t.Helper()

	id := crypto.AddressFromPreimage([]byte(fmt.Sprintf("tenderseed-routable-%d", n))).ID()

	addr, err := p2ptypes.NewNetAddressFromString(
		fmt.Sprintf("%s@203.0.113.%d:26656", id, 1+n%200),
	)
	if err != nil {
		t.Fatalf("unable to build routable address %d: %v", n, err)
	}

	if !addr.Routable() {
		t.Fatalf("test address %s is not routable, the test itself is wrong", addr)
	}

	return addr
}

// loopbackAddr builds a well formed but unroutable address: the exact thing
// addr_book_strict exists to keep out.
func loopbackAddr(t *testing.T, n int) *p2ptypes.NetAddress {
	t.Helper()

	id := crypto.AddressFromPreimage([]byte(fmt.Sprintf("tenderseed-loopback-%d", n))).ID()

	addr, err := p2ptypes.NewNetAddressFromString(
		fmt.Sprintf("%s@127.0.0.%d:26656", id, 1+n%200),
	)
	if err != nil {
		t.Fatalf("unable to build loopback address %d: %v", n, err)
	}

	return addr
}

// TestSeedBookStrict covers every way into the book, one by one. The defect
// this replaces was not that the rule was absent, it was that the rule was
// applied at some of the ways in and not all of them.
func TestSeedBookStrict(t *testing.T) {
	t.Parallel()

	t.Run("hearsay does not bring in an unroutable address", func(t *testing.T) {
		t.Parallel()

		book, _ := newStrictBookForTest(t)
		book.AddPeers(loopbackAddr(t, 1))

		if book.Size() != 0 {
			t.Fatalf("book holds %d addresses, expected none", book.Size())
		}
	})

	t.Run("a success does not bring in an unroutable address", func(t *testing.T) {
		t.Parallel()

		book, _ := newStrictBookForTest(t)
		book.MarkSuccess(loopbackAddr(t, 2))

		if book.Size() != 0 {
			t.Fatalf("book holds %d addresses, expected none", book.Size())
		}
	})

	t.Run("an attempt does not bring in an unroutable address", func(t *testing.T) {
		t.Parallel()

		book, _ := newStrictBookForTest(t)
		book.MarkAttempt(loopbackAddr(t, 3))

		if book.Size() != 0 {
			t.Fatalf("book holds %d addresses, expected none", book.Size())
		}
	})

	t.Run("the file does not bring back an unroutable address", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "addrbook.json")
		self := bookAddr(t, 999)
		quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

		loose, err := NewSeedBook(path, *self, false, quiet)
		if err != nil {
			t.Fatalf("unable to open book: %v", err)
		}

		loose.AddPeers(loopbackAddr(t, 4), routableAddr(t, 5))

		if err := loose.Save(); err != nil {
			t.Fatalf("unable to save book: %v", err)
		}

		strict, err := NewSeedBook(path, *self, true, quiet)
		if err != nil {
			t.Fatalf("unable to reopen book: %v", err)
		}

		if strict.Size() != 1 {
			t.Fatalf("book holds %d addresses, expected the routable one alone", strict.Size())
		}
	})

	t.Run("what the book refuses never reaches the file", func(t *testing.T) {
		t.Parallel()

		book, path := newStrictBookForTest(t)

		kept := routableAddr(t, 7)
		book.AddPeers(loopbackAddr(t, 6), kept)

		if err := book.Flush(); err != nil {
			t.Fatalf("unable to flush book: %v", err)
		}

		written, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("unable to read book file: %v", err)
		}

		if strings.Contains(string(written), "127.0.0.") {
			t.Fatal("an unroutable address reached the file")
		}

		if !strings.Contains(string(written), kept.String()) {
			t.Fatal("the routable address did not reach the file")
		}
	})

	t.Run("a routable address enters by every way in", func(t *testing.T) {
		t.Parallel()

		book, _ := newStrictBookForTest(t)

		book.AddPeers(routableAddr(t, 100))
		if book.Size() != 1 {
			t.Fatalf("hearsay: book holds %d addresses, expected 1", book.Size())
		}

		book.MarkSuccess(routableAddr(t, 101))
		if book.Size() != 2 {
			t.Fatalf("success: book holds %d addresses, expected 2", book.Size())
		}

		book.MarkAttempt(routableAddr(t, 102))
		if book.Size() != 3 {
			t.Fatalf("attempt: book holds %d addresses, expected 3", book.Size())
		}
	})

	t.Run("without the key nothing is refused", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)
		book.AddPeers(loopbackAddr(t, 8))

		if book.Size() != 1 {
			t.Fatalf("book holds %d addresses, expected the loopback one", book.Size())
		}
	})

	t.Run("a malformed address is refused whatever the key", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		empty := &p2ptypes.NetAddress{}
		book.AddPeers(empty)
		book.MarkAttempt(empty)
		book.MarkSuccess(empty)

		if book.Size() != 0 {
			t.Fatalf("book holds %d addresses, expected none", book.Size())
		}
	})
}

// TestSeedBookBatches covers the order and the ceiling. Without an order the
// batch is drawn at random from a map, and the tail of a large book is never
// checked; without a ceiling the seed promises freshness for more addresses
// than it can prove again.
func TestSeedBookBatches(t *testing.T) {
	t.Parallel()

	t.Run("the stale batch starts with the oldest news", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		first, second, third := bookAddr(t, 11), bookAddr(t, 12), bookAddr(t, 13)

		book.AddPeers(first, second, third)

		// Trying an address is news about it, so it goes to the back.
		book.MarkAttempt(third)
		book.MarkAttempt(second)

		batch := book.StaleBatch(time.Minute, 0)
		if len(batch) != 3 {
			t.Fatalf("batch holds %d addresses, expected 3", len(batch))
		}

		if batch[0].String() != first.String() {
			t.Fatalf("batch starts with %s, expected the never tried one", batch[0])
		}

		if batch[2].String() != second.String() {
			t.Fatalf("batch ends with %s, expected the most recently tried one", batch[2])
		}
	})

	t.Run("being tried sends an address to the back", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		for n := range 5 {
			book.AddPeers(bookAddr(t, 20+n))
		}

		head := book.StaleBatch(time.Minute, 2)
		if len(head) != 2 {
			t.Fatalf("batch holds %d addresses, expected 2", len(head))
		}

		for _, addr := range head {
			book.MarkAttempt(addr)
		}

		next := book.StaleBatch(time.Minute, 2)

		for _, taken := range head {
			for _, addr := range next {
				if addr.String() == taken.String() {
					t.Fatalf("%s came back in the next batch", addr)
				}
			}
		}
	})

	t.Run("the fresh batch is bounded and most recent first", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		for n := range 5 {
			addr := bookAddr(t, 30+n)
			book.AddPeers(addr)
			book.MarkSuccess(addr)
		}

		bounded := book.FreshBatch(time.Minute, 2)
		if len(bounded) != 2 {
			t.Fatalf("batch holds %d addresses, expected 2", len(bounded))
		}

		whole := book.FreshBatch(time.Minute, 0)
		if len(whole) != 5 {
			t.Fatalf("unbounded batch holds %d addresses, expected 5", len(whole))
		}

		if bounded[0].String() != whole[0].String() {
			t.Fatal("the bounded batch does not start with the most recent proof")
		}
	})

	t.Run("what is above the ceiling stays held", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		for n := range 4 {
			addr := bookAddr(t, 40+n)
			book.AddPeers(addr)
			book.MarkSuccess(addr)
		}

		if len(book.FreshBatch(time.Minute, 1)) != 1 {
			t.Fatal("the ceiling is not applied")
		}

		if book.Size() != 4 {
			t.Fatalf("book holds %d addresses, expected 4 held", book.Size())
		}
	})

	t.Run("the caller cannot reach into the book through a batch", func(t *testing.T) {
		t.Parallel()

		book, _ := newBookForTest(t)

		addr := bookAddr(t, 50)
		book.AddPeers(addr)
		book.MarkSuccess(addr)

		batch := book.FreshBatch(time.Minute, 0)
		if len(batch) != 1 {
			t.Fatalf("batch holds %d addresses, expected 1", len(batch))
		}

		batch[0].IP[0] ^= 0xff

		again := book.FreshBatch(time.Minute, 0)
		if again[0].String() != addr.String() {
			t.Fatalf("the book was reached through the batch: %s", again[0])
		}
	})
}

// TestSeedBookConcurrentSave writes from several goroutines at once. Run under
// the race detector it covers the window where Save releases the state lock to
// marshal and write.
func TestSeedBookConcurrentSave(t *testing.T) {
	t.Parallel()

	book, path := newBookForTest(t)

	for n := range 20 {
		book.AddPeers(bookAddr(t, 60+n))
	}

	var group sync.WaitGroup

	for range 8 {
		group.Add(1)

		go func() {
			defer group.Done()

			for n := range 10 {
				book.AddPeers(bookAddr(t, 100+n))

				if err := book.Save(); err != nil {
					t.Errorf("unable to save book: %v", err)
					return
				}
			}
		}()
	}

	group.Wait()

	if err := book.Flush(); err != nil {
		t.Fatalf("unable to flush book: %v", err)
	}

	self := bookAddr(t, 999)

	reopened, err := NewSeedBook(path, *self, false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unable to reopen book: %v", err)
	}

	if reopened.Size() != book.Size() {
		t.Fatalf("file holds %d addresses, memory holds %d", reopened.Size(), book.Size())
	}
}
