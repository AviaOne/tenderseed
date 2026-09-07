package tenderseed

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	osm "github.com/gnolang/gno/tm2/pkg/os"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
)

// defaultBookMaxPeers bounds the book. It is the core's own limit, kept so
// that a seed and a full node hold the same amount before evicting.
const defaultBookMaxPeers = 1000

// bookEntry is one address as it is written to disk.
//
// The first two fields are the core's format, byte for byte. The last three
// are ours, and they are the whole point of this file: what the verification
// needs and nothing else, consecutive failures, last attempt, last success.
// No promotion, no bucket, no new-versus-old bias. The core's bias exists to
// compensate a book fed by gossip with unverified addresses; here what is
// served is meant to be verified, so there is nothing to compensate, and the
// measured network holds 67 addresses where a bucket system sized for
// thousands would have nothing to sort.
//
// The two formats are one file on purpose. A core book reads here with the
// three extra fields at zero, and this file reads as a core book anywhere
// else, since unknown fields are ignored. One file, one state, no drift
// between two halves of the same truth.
type bookEntry struct {
	Addr     string `json:"addr"`
	LastSeen int64  `json:"last_seen"`
	Fails    int    `json:"fails,omitempty"`
	LastTry  int64  `json:"last_try,omitempty"`
	LastOK   int64  `json:"last_ok,omitempty"`
}

type bookFile struct {
	Peers []bookEntry `json:"peers"`
}

// bookRecord is one address in memory.
type bookRecord struct {
	addr     *p2ptypes.NetAddress
	lastSeen time.Time
	fails    int
	lastTry  time.Time
	lastOK   time.Time
}

// lastNews is the latest of the three dates this book keeps: the last
// success, the last attempt, and the last mention. It is the last time this
// seed learned anything at all about the address, whoever it learned it from.
//
// The mention belongs here, and leaving it out was the whole of a critical
// fault. An address that had never been tried had neither a success nor an
// attempt, so its news was the zero time and it came in front of every proven
// address waiting to be proven again. One peer answering with invented
// addresses therefore took the entire sweep budget for as long as it liked,
// and the proven ones aged out of the served set while the book still held
// them: a seed that listens, accepts and answers, and answers nothing.
//
// That is exactly what the batch order set out to avoid, and its own comment
// warned against: ordering on the last success alone would put an address that
// never answered in front of everything for ever. The key chosen to avoid it
// had the same hole one step earlier, and the code contradicted its comment.
func (r *bookRecord) lastNews() time.Time {
	latest := r.lastSeen

	if r.lastTry.After(latest) {
		latest = r.lastTry
	}

	if r.lastOK.After(latest) {
		latest = r.lastOK
	}

	return latest
}

// SeedBook is the address book of a TM2 seed: the addresses it knows and what
// it has learned about each of them. Safe for concurrent use.
type SeedBook struct {
	mtx sync.RWMutex

	logger   *slog.Logger
	filePath string
	self     p2ptypes.NetAddress
	maxPeers int

	// strict refuses addresses that are not routable, honouring
	// addr_book_strict. It is held here rather than in the reactor because
	// the book has four ways in, and a rule held anywhere else has to be
	// remembered at each of them.
	strict bool

	// saveMtx serialises the writes. Save releases the state lock while it
	// marshals and writes, so without this two writers could reach the file
	// out of order and leave the older snapshot behind.
	saveMtx sync.Mutex

	dirty      bool
	generation uint64

	peers map[string]*bookRecord
}

// NewSeedBook opens the book at the given path, creating nothing if the file
// is absent. self is this node's own address and is never stored.
func NewSeedBook(
	filePath string,
	self p2ptypes.NetAddress,
	strict bool,
	logger *slog.Logger,
) (*SeedBook, error) {
	book := &SeedBook{
		logger:   logger,
		filePath: filePath,
		self:     self,
		maxPeers: defaultBookMaxPeers,
		strict:   strict,
		peers:    make(map[string]*bookRecord),
	}

	if err := book.load(); err != nil {
		return nil, err
	}

	return book, nil
}

// AddPeers records addresses as known. It does not judge them: an address
// enters the book on hearsay and leaves the verification to say more.
func (b *SeedBook) AddPeers(addrs ...*p2ptypes.NetAddress) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	now := time.Now()

	for _, addr := range addrs {
		if !b.admissible(addr) || addr.Same(b.self) {
			continue
		}

		key := addr.String()

		if record, exists := b.peers[key]; exists {
			// Marked changed on purpose: eviction drops the oldest seen
			// first, so a refresh that never reaches the file would have
			// the book evict on stale dates after a restart.
			record.lastSeen = now
			b.dirty = true
			b.generation++

			continue
		}

		b.peers[key] = &bookRecord{addr: addr, lastSeen: now}
		b.dirty = true
		b.generation++
	}

	b.evict()
}

// MarkSuccess records that this address answered.
func (b *SeedBook) MarkSuccess(addr *p2ptypes.NetAddress) {
	if addr == nil {
		return
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	now := time.Now()

	record := b.record(addr, now)
	if record == nil {
		return
	}

	record.lastTry = now
	record.lastOK = now
	record.fails = 0

	b.dirty = true
	b.generation++
}

// admissible reports whether an address may enter the book at all.
//
// There are four ways in, not three: hearsay, a success, an attempt, and the
// file on disk. An attempt creates an entry as surely as hearsay does, so a
// rule applied at three of them is a rule that comes back through the fourth.
// Held here, an address that is not admissible is one the book cannot contain,
// whichever way it arrives and whoever wrote the file it arrives from.
//
// One rule is not held here and the sentence above does not cover it: this
// node's own address is refused where hearsay and the file come in, and
// nowhere else, so a success or an attempt on it would store it. Nothing
// reaches those two paths with it today, since the switch does not connect a
// node to itself and an attempt only revisits what the book already holds.
func (b *SeedBook) admissible(addr *p2ptypes.NetAddress) bool {
	if addr == nil {
		return false
	}

	if err := addr.Validate(); err != nil {
		return false
	}

	if b.strict && !addr.Routable() {
		return false
	}

	return true
}

// record returns the entry for an address, creating it if needed. It returns
// nil for an address the book may not hold, so a caller that marks an
// unknown address does not store it by the act of marking it.
// The caller holds the lock.
func (b *SeedBook) record(addr *p2ptypes.NetAddress, now time.Time) *bookRecord {
	if !b.admissible(addr) {
		return nil
	}

	key := addr.String()

	record, exists := b.peers[key]
	if !exists {
		record = &bookRecord{addr: addr, lastSeen: now}
		b.peers[key] = record
	}

	return record
}

// GetPeers returns every address in the book, copied.
func (b *SeedBook) GetPeers() []*p2ptypes.NetAddress {
	return b.collect(func(*bookRecord) bool { return true })
}

// Verified returns only the addresses that have answered at least once.
func (b *SeedBook) Verified() []*p2ptypes.NetAddress {
	return b.collect(func(record *bookRecord) bool { return !record.lastOK.IsZero() })
}

// Fresh returns the addresses that answered within the given window.
//
// A success does not hold forever. A node that answered an hour ago may be
// gone, and a seed that keeps handing it out is doing exactly what this seed
// exists not to do: repeating something it has not checked. A window of zero
// disables the ageing and returns every address that ever answered, which is
// what an operator who set peer_check_period to zero asked for.
func (b *SeedBook) Fresh(window time.Duration) []*p2ptypes.NetAddress {
	if window <= 0 {
		return b.Verified()
	}

	cutoff := time.Now().Add(-window)

	return b.collect(func(record *bookRecord) bool {
		return !record.lastOK.IsZero() && record.lastOK.After(cutoff)
	})
}

// Stale returns the addresses worth trying again: never reached, or last
// reached longer ago than the window.
func (b *SeedBook) Stale(window time.Duration) []*p2ptypes.NetAddress {
	cutoff := time.Now().Add(-window)

	return b.collect(func(record *bookRecord) bool {
		return record.lastOK.IsZero() || record.lastOK.Before(cutoff)
	})
}

// MarkAttempt records that this address is about to be tried.
//
// It is also where a failure is counted, and it is counted here rather than on
// a failure callback because the switch does not report one: a dial that never
// connects produces nothing this reactor can hear. So the count is deduced,
// which is exact for what it claims: if the previous attempt was never
// followed by a success, that attempt failed.
func (b *SeedBook) MarkAttempt(addr *p2ptypes.NetAddress) {
	if addr == nil {
		return
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	now := time.Now()

	record := b.record(addr, now)
	if record == nil {
		return
	}

	if !record.lastTry.IsZero() && record.lastTry.After(record.lastOK) {
		record.fails++
	}

	record.lastTry = now

	b.dirty = true
	b.generation++
}

// DropFailing removes the addresses that have failed this many times in a row,
// and returns how many were removed.
//
// An address that never answers costs an outbound slot at every sweep and a
// line in every answer this seed refuses to give. Keeping it forever would let
// a dead network's addresses outlive the network.
func (b *SeedBook) DropFailing(limit int) int {
	if limit <= 0 {
		return 0
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	dropped := 0

	for key, record := range b.peers {
		if record.fails >= limit {
			delete(b.peers, key)
			dropped++
		}
	}

	if dropped > 0 {
		b.dirty = true
		b.generation++
	}

	return dropped
}

// StaleProvenBatch returns at most limit addresses this seed has reached at
// least once and whose proof has expired, oldest news first.
//
// It exists apart from the exploration set because the two compete for one
// budget and they are not worth the same. Renewing an address this seed has
// reached keeps a promise it already makes; trying one it was merely told
// about may prove nothing. When they share a queue the second always wins,
// having no news at all, and the seed stops renewing what it serves.
func (b *SeedBook) StaleProvenBatch(window time.Duration, limit int) []*p2ptypes.NetAddress {
	cutoff := time.Now().Add(-window)

	return b.collectSorted(
		func(record *bookRecord) bool {
			return !record.lastOK.IsZero() && record.lastOK.Before(cutoff)
		},
		func(left, right *bookRecord) bool {
			return left.lastNews().Before(right.lastNews())
		},
		limit,
	)
}

// StaleUnprovenBatch returns at most limit addresses this seed has never
// reached, oldest news first.
func (b *SeedBook) StaleUnprovenBatch(limit int) []*p2ptypes.NetAddress {
	return b.collectSorted(
		func(record *bookRecord) bool {
			return record.lastOK.IsZero()
		},
		func(left, right *bookRecord) bool {
			return left.lastNews().Before(right.lastNews())
		},
		limit,
	)
}

// Knows reports whether the book already holds this address.
func (b *SeedBook) Knows(addr *p2ptypes.NetAddress) bool {
	if addr == nil {
		return false
	}

	b.mtx.RLock()
	defer b.mtx.RUnlock()

	_, found := b.peers[addr.String()]

	return found
}

// FreshBatch returns at most limit addresses that answered within the window,
// the most recently proven first. A limit of zero returns all of them.
//
// The limit is what freshness costs. Fresh means proven recently, so a seed
// cannot claim it for more addresses than it is able to prove again inside the
// window. What is above the limit stays held and unserved, waiting its turn to
// be proven, rather than being handed out on a proof that has expired. The
// other stack already serves a subset of a larger book; this is the same
// arrangement, not a new one.
func (b *SeedBook) FreshBatch(window time.Duration, limit int) []*p2ptypes.NetAddress {
	keep := func(record *bookRecord) bool {
		return !record.lastOK.IsZero()
	}

	if window > 0 {
		cutoff := time.Now().Add(-window)
		keep = func(record *bookRecord) bool {
			return !record.lastOK.IsZero() && record.lastOK.After(cutoff)
		}
	}

	return b.collectSorted(
		keep,
		func(left, right *bookRecord) bool {
			return left.lastOK.After(right.lastOK)
		},
		limit,
	)
}

// collectSorted is collect with an order and a ceiling.
func (b *SeedBook) collectSorted(
	keep func(*bookRecord) bool,
	less func(left, right *bookRecord) bool,
	limit int,
) []*p2ptypes.NetAddress {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	records := make([]*bookRecord, 0, len(b.peers))

	for _, record := range b.peers {
		if keep(record) {
			records = append(records, record)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return less(records[i], records[j])
	})

	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}

	addrs := make([]*p2ptypes.NetAddress, 0, len(records))

	for _, record := range records {
		// Copied, IP included, for the reason collect gives.
		copied := *record.addr
		copied.IP = append(net.IP(nil), record.addr.IP...)
		addrs = append(addrs, &copied)
	}

	return addrs
}

func (b *SeedBook) collect(keep func(*bookRecord) bool) []*p2ptypes.NetAddress {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	addrs := make([]*p2ptypes.NetAddress, 0, len(b.peers))

	for _, record := range b.peers {
		if !keep(record) {
			continue
		}

		// Copied, IP included: the caller must not be able to reach into
		// the book through the slice it was handed.
		copied := *record.addr
		copied.IP = append(net.IP(nil), record.addr.IP...)
		addrs = append(addrs, &copied)
	}

	return addrs
}

// Size returns how many addresses the book holds.
func (b *SeedBook) Size() int {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	return len(b.peers)
}

// VerifiedSize returns how many of them have answered at least once.
func (b *SeedBook) VerifiedSize() int {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	count := 0
	for _, record := range b.peers {
		if !record.lastOK.IsZero() {
			count++
		}
	}

	return count
}

// Save writes the book to disk, atomically. It does nothing when nothing has
// changed since the last successful save.
func (b *SeedBook) Save() error {
	b.saveMtx.Lock()
	defer b.saveMtx.Unlock()

	b.mtx.Lock()
	if !b.dirty {
		b.mtx.Unlock()
		return nil
	}

	saved := b.generation
	entries := make([]bookEntry, 0, len(b.peers))

	for _, record := range b.peers {
		entries = append(entries, bookEntry{
			Addr:     record.addr.String(),
			LastSeen: record.lastSeen.Unix(),
			Fails:    record.fails,
			LastTry:  unixOrZero(record.lastTry),
			LastOK:   unixOrZero(record.lastOK),
		})
	}
	b.mtx.Unlock()

	data, err := json.MarshalIndent(bookFile{Peers: entries}, "", "\t")
	if err != nil {
		return fmt.Errorf("unable to marshal address book, %w", err)
	}

	if err := osm.WriteFileAtomic(b.filePath, data, 0o644); err != nil {
		return fmt.Errorf("unable to write address book, %w", err)
	}

	// Only clear dirty when nothing changed while we were writing, so the
	// next save picks up whatever arrived in between.
	b.mtx.Lock()
	if b.generation == saved {
		b.dirty = false
	}
	b.mtx.Unlock()

	return nil
}

// Flush writes the book whether it changed or not.
func (b *SeedBook) Flush() error {
	b.mtx.Lock()
	b.dirty = true
	b.mtx.Unlock()

	return b.Save()
}

// evict drops entries until the book fits, the never reached ones first. The
// caller holds the lock.
//
// The order is what makes the ceiling safe. Dropping on the last mention
// alone let hearsay push out what this seed had reached: an address nobody
// can dial, mentioned a second ago, is newer than one that answered an hour
// ago and has not been mentioned since, so the population this seed exists to
// serve was the first to go. A stranger able to send addresses could empty
// the served set by filling the book, in one message, and what was left was
// written to disk within the minute and dialled again at the next start.
//
// So the never reached go first, oldest mention first among them, and only
// once they are exhausted do the reached go, least recently proven first.
func (b *SeedBook) evict() {
	if len(b.peers) <= b.maxPeers {
		return
	}

	records := make([]*bookRecord, 0, len(b.peers))
	for _, record := range b.peers {
		records = append(records, record)
	}

	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]

		leftReached := !left.lastOK.IsZero()
		rightReached := !right.lastOK.IsZero()

		if leftReached != rightReached {
			return rightReached
		}

		if leftReached {
			return left.lastOK.Before(right.lastOK)
		}

		return left.lastSeen.Before(right.lastSeen)
	})

	for i := range len(b.peers) - b.maxPeers {
		delete(b.peers, records[i].addr.String())
	}

	b.dirty = true
}

// load reads the book from disk. A missing file is not an error. A corrupt
// file is copied beside itself and treated as empty, rather than stopping the
// seed. The original is left where it is, and the first save overwrites it.
func (b *SeedBook) load() error {
	if _, err := os.Stat(b.filePath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	data, err := os.ReadFile(b.filePath)
	if err != nil {
		return fmt.Errorf("unable to read address book, %w", err)
	}

	var raw bookFile

	if err := json.Unmarshal(data, &raw); err != nil {
		copyPath := b.filePath + ".corrupt"

		if copyErr := os.WriteFile(copyPath, data, 0o644); copyErr != nil {
			b.logger.Warn("corrupt address book, copy failed",
				"file", b.filePath, "err", err, "copy_err", copyErr)
		} else {
			b.logger.Warn("corrupt address book, copy kept",
				"file", b.filePath, "copy", copyPath, "err", err)
		}

		return nil
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	now := time.Now()

	for _, entry := range raw.Peers {
		addr, err := p2ptypes.NewNetAddressFromString(entry.Addr)
		if err != nil {
			// One unreadable address does not cost the whole book.
			continue
		}

		if !b.admissible(addr) || addr.Same(b.self) {
			// A book written by a core node, or written here before the
			// key was set, does not put back what the key refuses.
			continue
		}

		lastSeen := time.Unix(entry.LastSeen, 0)
		if entry.LastSeen == 0 {
			lastSeen = now
		}

		b.peers[addr.String()] = &bookRecord{
			addr:     addr,
			lastSeen: lastSeen,
			fails:    entry.Fails,
			lastTry:  zeroOrUnix(entry.LastTry),
			lastOK:   zeroOrUnix(entry.LastOK),
		}
	}

	return nil
}

func unixOrZero(moment time.Time) int64 {
	if moment.IsZero() {
		return 0
	}
	return moment.Unix()
}

func zeroOrUnix(seconds int64) time.Time {
	if seconds == 0 {
		return time.Time{}
	}
	return time.Unix(seconds, 0)
}
