package tenderseed

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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

// newBookForTest returns an empty book backed by a file in a temporary directory,
// and the path of that file.
func newBookForTest(t *testing.T) (*SeedBook, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "addrbook.json")
	self := bookAddr(t, 999)

	book, err := NewSeedBook(path, *self, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

		again, err := NewSeedBook(path, *bookAddr(t, 999),
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

		book, err := NewSeedBook(path, *bookAddr(t, 999),
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

		book, err := NewSeedBook(path, *bookAddr(t, 999),
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

		book, err := NewSeedBook(path, *bookAddr(t, 999),
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

		book, err := NewSeedBook(path, *bookAddr(t, 999),
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

		book, err := NewSeedBook(path, *bookAddr(t, 999),
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("unable to open: %v", err)
		}
		if got := book.Size(); got != 1 {
			t.Fatalf("size: got %d, want 1", got)
		}
	})
}
