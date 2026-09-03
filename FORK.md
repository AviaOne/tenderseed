# Why this fork exists

This document holds the evidence behind every claim made in the README. The
short version lives there; the measurements live here.

Everything below was measured on source code, on command output, or on a running
process. Where something is a judgement rather than a measurement, it says so.

---

## 1. The state of the field

Every count in this section was taken on 2026-09-03 and describes repositories
that go on living. Re-measure before quoting it.

### 1.1 Upstream

[binaryholdings/tenderseed](https://github.com/binaryholdings/tenderseed) at
commit `10a64d5`: 13 commits, last one February 2023, no tag, no release, no
test. Five Go files, 388 lines. It targets Tendermint `v0.34.22`, a line that
has reached end of life.

### 1.2 The forks

Upstream has 48 forks. Comparing each against its own default branch rather than
assuming `master`, **37 carry no commit of their own** and 11 diverge. None of
the 11 goes beyond CometBFT `v0.37.2`. Nobody reached 0.38, 0.39 or 0.40.

Defects measured inside those forks, none of which are reproduced here:

- **Peer limits that are declared but never applied**, in four projects. Each
  starts from `config.DefaultP2PConfig()`, logs the values from its own
  configuration, then passes the untouched default to the transport and the
  `Switch`. The effective limits stay at 40 inbound and 10 outbound everywhere.
  The log makes the bug invisible, because it prints the intended value.
- **A multi-chain rewrite that cannot run as published**: its `main()` starts one
  goroutine per chain and then returns, which ends the process; and its only
  source of chain data is a domain that no longer resolves.
- **A peer checker that bans healthy peers.** It calls
  `Switch.DialPeerWithAddress` directly. That call returns
  `ErrCurrentlyDialingOrExistingAddress` when the peer is already connected, and
  the checker counts it as a failure, so a perfectly reachable peer is marked bad
  on the second tick.
- **A startup fix that never fires**, because its test looks at the address book
  *path*, which the default configuration always populates.
- **A multi-chain build that shares a single address book** across every chain it
  claims to serve.

One of the eleven is a useful precedent rather than a defect:
[BitCannaGlobal/tenderseed](https://github.com/BitCannaGlobal/tenderseed) ported
the project to CometBFT `v0.37.2` in nine lines outside `go.mod`. That measured
the cost of the migration and showed it was mechanical, not structural. It is
credited for the method; none of its code is used here, since this fork targets
`v0.40.x`.

### 1.3 Dedicated seeds outside the fork tree

- [voluzi/cosmoseed](https://github.com/voluzi/cosmoseed): the technical state of
  the art, 32 commits, 21 tags. Pinned to a CometBFT v2 release candidate that
  upstream withdrew, so it sits outside every supported family. Its design is
  transposable; its code is not, because the v2 API differs.
- `kwilteam/cometseed`: a dedicated seed on CometBFT `v0.38.7`, ten commits, no
  tag, last touched May 2024. No peer qualification.
- `HighStakesSwitzerland/multiseed`: built on a private fork of Tendermint 0.35,
  a discontinued line.
- `ovrclk/tenderseed`: the only project in the lineage with a real release
  pipeline. Last commit February 2021.

**The gap this fork fills** is not the idea, it is the maintenance: every serious
attempt anchored itself to a dead version line.

---

## 2. The three measured problems

### 2.1 The stack is end of life

Tendermint `v0.34.22` receives no fix of any kind. Nothing further to add.

### 2.2 A seed never qualifies its address book

`MarkGood` moves an address into an *old* bucket, and the PEX reactor applies a
70% bias toward *old* buckets when answering a request. In a full node, the only
production callers of `MarkGood` are in the consensus reactor.

**A seed does not run a consensus reactor.** So `MarkGood` is never called, the
book stays entirely in *new* buckets, and the 70% bias has nothing to select. The
legacy p2p specification states that defining vetting is out of scope for the
p2p package and left to the integrator, and the upstream documentation says a
seed should answer with an upper percentile of the best peers it knows. The code
cannot honour that requirement on its own.

This fork supplies the missing part.

### 2.3 Crawled peers are dropped before they answer

`SeedDisconnectWaitPeriod` is never set by upstream, so it is zero, and every
non-persistent outbound peer is disconnected on the first crawl round.

CometBFT hardcodes 28 hours for a full node. That number derives from the time a
peer needs to become "good" through the consensus reactor. A seed has no
consensus reactor, so **the justification for 28 hours is empty here**.

Values found across the ecosystem: 28h hardcoded upstream, 3 minutes in
`cometseed`, 5 seconds and 1 second in builds that do not work, zero by omission
in Tenderseed. **Nobody justifies their value beyond a copied comment.**

Five minutes is this fork's default. It is a judgement, and it was checked at
runtime: over 12 hours, outbound connections split into two clean populations,
536 verification dials closed inside one second, and 113 crawl connections with
a median lifetime of 360 seconds and a maximum of 476, consistent with a 300
second setting enforced at the next crawl tick.

---

## 3. How address verification is built

### 3.1 Composing rather than duplicating

Two designs were possible: duplicate the reactor's dial logic, or compose the
PEX reactor inside a local type.

`dialPeer` carries the exponential backoff, `maxAttemptsToDial`,
`markAddrInBookBasedOnErr`, and it treats
`ErrCurrentlyDialingOrExistingAddress` as benign by returning without marking the
book. It is **not exported**, so it cannot be called from another package, not
even through embedding. Duplicating it would mean reimplementing upstream code
that is covered by nightly fuzzing, and resynchronising it on every version
family.

**Composition was chosen.** The embedded reactor's `OnStart` keeps its crawl
routine running, which performs full `dialPeer` work; our own logic runs
alongside it.

### 3.2 Two sources of truth, both required

`cosmoseed` verifies addresses only as they arrive. That is not enough.

`MarkGood` promotes an address into an *old* bucket and nothing ever demotes it.
An address qualified today and dead tomorrow would be served **preferentially and
indefinitely**, which would make a fork *worse* than the original.

So there are two sources, both mandatory:

- verification of addresses as `PexAddrs` messages arrive, for freshness;
- a periodic sweep of the served selection, for staleness.

### 3.3 Verification connections are closed immediately

A choice specific to this fork. `cosmoseed` and the NibiruChain checker both
leave their verification connections open.

Consequences: no accumulation of outbound connections, no saturation guard
needed, and a clean separation of roles, since the upstream crawl collects while
this reactor judges.

Accepted trade-off: if the crawl opens the same address between our
`IsDialingOrExistingAddress` check and our close, we close its connection. The
window is a few milliseconds and it reopens on the next tick.

### 3.4 Four defects of cosmoseed that are avoided here

All four measured in its reactor source.

1. **It overrides `Start`, not `OnStart`.** The PEX constructor fixes the service
   implementation on the upstream reactor, and `BaseService.Start` calls
   `bs.impl.OnStart()`, so an `OnStart` defined on the outer type would never
   run.
2. **`Receive` never delegates upstream.** It intercepts `PexAddrs` without
   passing them on, so the upstream handler is never reached, its pending-request
   map is never purged, and further requests to that peer stay blocked while it
   remains connected.
3. **`AddAddress` after `MarkGood`.** `MarkGood` returns immediately when the ID
   is absent from the book, so marking before adding is a no-op for an unknown
   inbound peer. The order here is add, then mark.
4. **`IsDialingOrExistingAddress` before every dial**, which avoids the
   already-connected-counted-as-failure defect described in section 1.2.

### 3.5 An unsolicited address list is not verified

This fork delegates every message to the upstream reactor first. For a list of
addresses nobody asked for, that reactor returns `ErrUnsolicitedList`, no address
reaches the book, and the sender is stopped and banned before the call returns.
The ban carries a 24-hour expiry that never fires on a seed, see 5.3.
Verifying that payload afterwards would have let an unsolicited peer get
addresses of its choosing dialled and promoted, which is exactly the
control that was just applied. The batch is dropped instead.

The signal is the sender being stopped, not the book. `MarkBad` only records an
address the book already holds, so an inbound peer the seed never dialled is
never marked banned and asking the book would have filtered nothing. A sender
stopped for an unrelated reason costs that one batch, which the periodic sweep
picks up again.

---

### 3.6 Verification remembers its own verdicts

Verification used to be stateless, and that cost twice.

A dead address in the served selection was re-dialled at every sweep, with no
spacing at all, for as long as the upstream crawler took to evict it. That
eviction is real and, on a seed, final: `dialPeer` gives up once an address has
failed more than `maxAttemptsToDial` times, that is on the eighteenth call after
seventeen recorded failures, spread over roughly 35 hours, then bans it. That is
what section 5.3 measures as the book falling from 1263 entries to 20 between 24
and 55 hours. The ban carries a 24-hour expiry, but nothing on a seed ever
constates it, see 5.3, so the address is gone until the process restarts.
Bounded in count is not free in traffic: on a full book, a selection of 250
addresses of which most are unreachable meant on the order of a thousand futile
connections an hour, each paying a connect timeout, for the whole of that
window. The sweep never removed anything itself; it only added traffic.

An address just verified could also be dialled again straight away as soon as
another peer mentioned it, for a second full handshake that taught nothing. A
popular address is mentioned by many peers, so the addresses re-dialled most were
the ones whose reachability was least in doubt.

Both come from the same gap, so one structure closes both: the last verdict on
each node identity, when it was reached, and how many failures preceded it. One
map, one lock, two policies that never mix.

The key is the node identity, not the full address, and that is deliberate. The
upstream book is itself keyed that way: `addrLookup` is indexed by ID, `MarkGood`
takes an ID as its argument, and `MarkAttempt`, `IsGood` and `AddAddress` all
reach their entry through `addr.ID`. A verdict applies to a book entry, and a
book entry is an identity. Keying on the full address would hold two verdicts
for one entry, which is a divergence rather than a refinement. The consequence
is that an identity reappearing at a new host inherits the verdict of the old
one, for at most the freshness window or the current backoff; the book behaves
the same way, since `AddAddress` refuses to change the address of an identity it
already holds as old.

**On failure**, the next attempt is spaced by 2^n seconds, capped at 4 hours.
The formula is the upstream one used in `dialPeer`, deliberately: both
accountings then live on the same scale and remain comparable. It is not
expressed in multiples of `peer_check_period`, which would have coupled two
unrelated effects, since shortening the period to refresh the book faster would
also have hardened the punishment of failures. The cap keeps this reactor on the
timescale of the upstream crawl rather than adding a third one, so the two
accountings stay comparable. It was once justified as sitting below an upstream
ban that would expire; that justification does not hold, see 5.3. There is no
permanent give-up: the crawler alone bans,
and an address it has evicted is no longer in the book, so no selection can
return it anyway.

**The sweep samples what is served, not the book.** A seed answers a request
for addresses with `GetSelectionWithBias`, which draws most of its result from
the old buckets, that is from the addresses this reactor promoted. Sweeping the
unbiased `GetSelection` sampled the book uniformly, where promoted addresses are
a small minority: on a book of a thousand entries holding thirty promoted ones,
a promoted address came up in roughly one sweep out of five, and it is precisely
the address served first. Since nothing ever demotes a `MarkGood`, that
population is the only one the sweep exists for. The new buckets are not left
uncovered: they are what the arrival path verifies, and the biased selection
still draws from them. The bias never shortens the selection either: when the
old buckets cannot supply their share, `numRequiredNewAdd` claims the difference
from the new ones, so an early book with few promoted addresses yields a full
selection that happens to contain all of them.

**On success**, re-verification is suppressed for a window derived from the
period rather than exposed as a key: `min(period / 2, period - 5 minutes)`,
disabled outright when that is not positive. The window must stay strictly below
the period, and by more than the worst case time an address spends in the queue,
which is 32 dials deep whatever the worker count and at most 7 seconds each. Were
it equal to the period, a sweep would re-queue an address exactly as its verdict
expired and the queue traversal alone would decide whether it was re-verified or
skipped: an arbitrary, unreproducible share of the re-verifications would vanish
silently. Below the margin the window collapses to zero and everything is
re-verified, because re-judging what is served is the point of this fork and the
saving is only a by-product.

**Interaction with the address book counter.** `MarkAttempt` increments the
`Attempts` field of a book entry, and both the upstream crawler, through
`markAddrInBookBasedOnErr`, and this reactor feed the same field. That counter
only reaches `isBad()`, read by `expireNew` when a new bucket is full, and it
stops mattering past three failures on an address that never succeeded; a
promoted address is out of scope entirely, since `MarkGood` zeroes the counter
and moves the entry to an old bucket. So the sweep never made an address
evictable meaningfully sooner. What it did was make the counter unreadable, and
that is now bounded by the backoff. The reverse accounting stays separate: the
`attemptsToDial` map inside the upstream reactor is unexported, invisible from
here, and this reactor's dials do not feed it.

**A collision is not a verdict.** A dial can fail for a reason that says nothing
about the address: a dial already under way, or a peer that connected to us while
we were dialling it, which both the transport and the switch report as a
duplicate rejection. Marking an attempt there would penalise a live address, on a
counter shared with upstream. Those two shapes are filtered; every other
rejection, an incompatible network or a failed authentication, remains a verdict
about the address and is marked.

**What earns a counter.** One outcome per behaviour whose value can be
interpreted, never one outcome per branch of the code. `skipped_local` groups
the exits that dial nothing and learn nothing, our own address, a banned one, a
connection already open or under way, and the defensive nil guards at the top of
verify: none of them is a behaviour this fork changed, telling them apart would
answer no question anyone has asked, and the debug line covers the day one is. `skipped_collision` is separate
because a dial did happen and taught nothing about the address, and because it
is the only one of the four this version changed: its value is the number of
unfair marks avoided on live addresses. By the same rule `skipped_backoff` and
`skipped_fresh` stay apart, being two distinct policies rather than two
branches. The stage label follows the same logic: a skip in the queue and a skip
before a dial bound the saved traffic from opposite sides.

**The map is bounded.** An entry whose address has left the book can never be
served or queued again and is dropped, but only once it has come out of its wait,
so that dropping it does not hand back a free dial to an address that was told to
wait. Anything older than twice the cap goes regardless.

---

## 4. What the defaults are anchored to

| key | default | anchor |
|---|---|---|
| `seed_disconnect_wait_period` | `5m` | judgement, checked at runtime, see 2.3 |
| `peer_check_workers` | 8 | a selection returns at most 250 addresses and a dial costs at most 7 seconds, 1s to connect then two consecutive 3s handshake deadlines, so a sequential sweep takes about 29 minutes and an 8-worker sweep about 3m40 |
| `peer_check_period` | `10m` | five times the minimum interval between crawls, and about 2.7 times the duration of one sweep. It also sets the window during which a successful verdict is trusted, see 3.6. `0` disables |
| `allow_duplicate_ip` | `true` | the value upstream hardcoded |
| `metrics_listen_addr` | empty | disabled by default, so nothing changes for an existing user |
| `metrics_namespace` | `cometbft` | the upstream default, so validator dashboards apply unchanged |
| `moniker` | empty | empty means `<chain_id>-seed`, the original behaviour |

---

## 5. Measured results

### 5.1 Address qualification

Same duration, same seeds, same chain, 180 seconds:

| binary | book total | *old* bucket, i.e. `MarkGood` | *new* bucket |
|---|---|---|---|
| before verification, i.e. the behaviour of the original and of all 11 forks | 1189 | **0** | 1189 |
| with verification | 1184 | **45** | 1139 |

Qualified addresses went from zero to 45, that is 45 out of the 1184 addresses
held in the book, without slowing collection down. Against a different
denominator, roughly 12% of the addresses actually dialled answered, which is
itself a useful figure: most addresses circulating on a network are not
reachable.

Collection is also faster, because verification dials run in parallel while the
upstream crawl is sequential.

### 5.2 A twelve-hour production-scale run

On a live chain, on a dedicated port, alongside an untouched production seed:

| | after 2h02 | after 12h34 |
|---|---|---|
| address book | 1262 addresses, 592 KB | 602 KB |
| resident memory | 29452 KB | 31344 KB |
| threads | 26 | 28 |
| open file descriptors | 16 | 15 |
| panics | 0 | 0 |

No descriptor leak, no memory leak. On `SIGTERM` the address book is written to
disk **before** the switch stops, and the process exits without a goroutine dump.

An inbound peer connected to it received **611 addresses in 90 seconds**.

### 5.3 The before and after that started this

Two production seeds ran the upstream binary for years and held **4 and 6
addresses**, barely more than the seeds listed in their own configuration.

Only the binary was replaced. Same machine, same two chains, same node identity,
same configuration values, same address book files left in place. Four hours
later:

| chain | before | after 4 hours | promoted |
|---|---|---|---|
| first chain | 4 | **1263** | 28 |
| second chain | 6 | **568** | 40 |

The promotion rate, promoted addresses over the book total of the same seed,
differs widely between the two, 28 out of 1263 and 40 out of 568, that is 2.2
and 7.0 percent, stated without an explanation because none was measured.

**Stated honestly**: this is a before and after on the same seeds, not a
controlled benchmark. `addr_book_strict` also rejects addresses and may account
for part of the gap.

#### What happens after those four hours

Both books were emptied on purpose and the two seeds were then left alone, to
measure a whole cycle rather than its first hours. First chain then second:

| age | book total | verified |
|---|---|---|
| 4 min | 947 and 460 | 13 and 26 |
| 24 h | 1263 and 568 | 39 and 54 |
| 55 h | 20 and 23 | 19 and 20 |
| 8 days | 10 and 15 | 10 and 14 |

The climb is reproducible: two independent cycles started from an empty book
reached the same totals. So is the fall that follows, and it is not a regression
of this fork. It is upstream eviction. On a seed the crawl routine calls
`dialPeer`, which once an address has failed more than `maxAttemptsToDial`
times, that is on the eighteenth call after seventeen recorded failures, calls
`MarkBad` and removes the address. Those attempts take about 35 hours, and the
drop happens between 24 and 55 hours. `MarkAttempt` deletes nothing on its own;
its counter only feeds `isBad()`, read by `expireNew` when a *new* bucket is
full.

**That ban never expires on a seed.** `MarkBad` records a 24-hour expiry, but
the only production call that reinstates a banned address sits inside
`ensurePeers`, which belongs to the routine a node runs when it is *not* a seed.
A seed starts the crawl routine instead, so nothing ever reinstates anything: a
banned address is gone until the process restarts. It is not persisted either,
so a restart clears the ban list rather than restoring it. This is inherited
upstream behaviour and it is left as it is, since diverging from upstream
without a measured reason is not what this fork does.

What survives is almost entirely verified: 10 of 10 and 14 of 15 in the *old*
bucket after eight days. Verified addresses fall too, because an address promoted
once and dead later is evicted like any other, `MarkBad` does not look at the
bucket.

**So 1263 and 568 measure the first hours, not the regime.** In regime this fork
holds ten to twenty addresses, nearly all verified reachable, against 4 and 6
addresses of which zero were verified for the upstream binary on the same seeds.

### 5.4 What a new node actually collects

The size of a book on disk is not what an operator gets out of a seed. What
matters is how many addresses a fresh node collects when it starts from that
seed alone, and that is directly measurable by anyone.

Method: a throwaway home directory, an empty address book, a dedicated port, one
single seed configured, the chain that seed serves, 90 seconds, then count the
entries in `data/addrbook.json`. One pass per seed, the same 90 seconds for each,
the same day.

On one Cosmos chain, against the eight public seeds listed for it, this one
included, a new node collected **473 addresses from this seed**. The best of the
eight returned 781, and **four of the eight returned nothing at all**. Two
further passes were interrupted and are excluded rather than reported.

Stated as a limit: one chain, one pass per seed, one day, and no attempt to
explain what the seeds returning nothing were doing at that moment. It places
this seed in the leading group for the only thing a seed exists for, which is
bootstrapping a new node. It is not a ranking.

---

## 6. The second stack: TM2

The code in this section was read in the gno source at the commit this fork
pins, `2ed70a2`, and in this fork's own source. The figures come from
measurements run on pearl, the gno.land test network, and are gathered in 6.7.

**Section 5 measures the Cosmos stack only and none of its numbers transposes
here**: they are anchored on CometBFT buckets, on `MarkGood`, and on chains this
stack does not serve.

### 6.1 One binary, one stack per process

The stack is declared in `config.toml`. Absent, it is Cosmos, which is what every
version up to v2.2.2 did. This is the mechanism already used for everything that
depends on the chain rather than on the binary.

It can also be declared on the command line, and that is not a convenience.
The identity of a seed is created by the same first command that creates the
configuration file, and the identity format belongs to the stack, so an
operator with no file yet would otherwise have no way to say which one they
serve: they would be handed a Cosmos identity and would have to delete it. The
flag settles the stack before anything is written. A file that already exists
is never rewritten, so this affects a first run and nothing else.

An unrecognised **value** is refused at start, where an unrecognised **key** is
only reported. The asymmetry is deliberate: a misspelled key costs one setting,
while a misspelled stack would start the network code of the wrong chain and fail
far from its cause, in the handshake, against addresses that will never answer.

### 6.2 What the TM2 core does with peer addresses

The discovery reactor picks a random connected peer every three seconds and asks
it for peers. Answers are added to a **persistent store** and handed to the
switch for dialling. A freshly dialled outbound peer is asked immediately rather
than waiting for the tick.

The store keeps up to 1000 addresses on disk, keyed by the full `ID@host:port`
string, each with the moment it was last seen. It is written atomically, reloaded
at start, drops the oldest entries when full, never stores the node's own
address, and treats a corrupt file as empty after copying it aside.

**But the store is never consulted when answering.** A discovery request is
served from `Switch.Peers().List()`, that is from the connections open at that
instant. Private peers and peers whose advertised dial address does not validate
are removed, the rest is shuffled, and at most **30** are returned. A node
holding a thousand stored addresses still answers with at most thirty of the ones
it happens to be talking to.

Routability is deliberately not applied to what is shared. The upstream comment
gives the reason: a `Routable()` constraint would filter the loopback addresses
that local and test deployments advertise.

Nothing in that path ever closes a connection.

### 6.3 The three gaps that matter for a seed

1. **A seed answers from its connections, not from what it knows.** The one node
   on the network whose entire purpose is to hold addresses is the one that
   cannot serve them.
2. **Nothing is ever verified.** The stored `last seen` records when an address
   was last *mentioned* by someone, not when it was last *reached*. An address
   that has never answered and one that answered a second ago are stored alike.
   Measured, that gap is 28 per cent of the network: see 6.7.
   This is the same gap as section 2.2 on the Cosmos side, reached by a different
   route: there, qualification exists and is never called; here, it does not
   exist.
3. **Nothing hangs up.** A seed that never closes an idle connection fills its
   own inbound slots with peers it has already served, and stops being reachable
   for the next new node. On the Cosmos side the equivalent is section 2.3, where
   the value exists and is left at zero.

### 6.4 What this fork adds on TM2

- **A book that is served.** Up to 250 verified addresses, the selection size the
  Cosmos side of this same binary already serves, against the core's 30 drawn
  from connections.
- **Verification state on top of the core's own file.** The same JSON shape and
  the same 1000-address ceiling, plus three optional fields: consecutive
  failures, last attempt, last success. Anything that does not know them ignores
  them, so one file holds one state and either side can read it. No buckets, no
  promotion, no new-versus-old bias: the primitives the Cosmos policy is built
  on do not exist here, so the policy is transposed and not the mechanism.
- **A hang-up after answering**, on `seed_disconnect_wait_period`, the key the
  Cosmos side already uses for the same decision. A peer that asks repeatedly
  gets no longer a stay than one that asks once.
- **A hang-up when there is nothing to serve.** An empty book is the state where
  an unanswered peer costs the most, since it holds the slot the next one needs.
- **`addr_book_strict` honoured**, on the way in as well as on the way out, so an
  unroutable address is not stored, not dialled, and does not return after a
  restart. A public seed that kept the core's behaviour would hand out addresses
  nobody can dial.
- **A sweep on `peer_check_period`.** Stale addresses are handed to the switch
  rather than dialled directly, so one dialler keeps holding the outbound limit
  and the duplicate-IP rule. Five consecutive failures evict an address. A
  success is trusted for three periods, so a sweep may miss once without
  emptying what the seed can answer.
- **Counters**, on the shape of the Cosmos ones: one series for the decisions, by
  outcome and by the stage that took them, one for the size of the book and of
  its servable part, every reachable pair published at zero so a share can be
  read from the first scrape. Nothing upstream reports any of this.

### 6.5 Two constraints found in the core, not chosen

**The announced channels come only from the registered reactors.** A connection's
channel descriptors are assembled from what the reactors declare, so announcing a
channel no reactor serves makes the peer send on it and the connection stop on
the first unknown channel. The seed therefore announces the discovery channel and
nothing else. This is a constraint, not a design preference.

**The switch has one exported way to drop a peer, and reports every use of it as
a failure.** On a seed the main path is not a failure, and a journal whose normal
traffic looks like failure stops being read. The line is filtered down to
informational, keyed on the error value rather than on its wording, so a reworded
message upstream cannot silently disable the filter.

### 6.6 Identity and handshake

The two stacks share no identity format. The node ID, the key file and the
address string all differ, so **a `seeds` list written for one stack is not
transposable to the other**, and a home directory belongs to one stack.

One value in the handshake belongs to the chain rather than to the binary: the
`app` entry of the announced version set. It has no equivalent on the Cosmos side
of this binary, where the comparable value is fixed by the stack. It is exposed
as `app_version`, empty by default, and a chain that announces a non-empty
application version needs it set to match.

### 6.7 The measured baseline

Measured on pearl, the gno.land test network, in September 2026. These are the
figures a TM2 change is compared against; **none of the Cosmos numbers in
section 5 transposes to them.**

**What a fresh node collects from the two public pearl seeds and nothing else.**
67 addresses, reached at fifteen seconds and unchanged for the following three
minutes, of which 65 are routable and 2 are loopback. Of those 67, four
connections held. Reproduced three times, twice under a different identity, with
the same four peers holding every time. It is a plateau, not a progression.

**28 per cent of what the network announces is reachable by nobody.** A fresh
seed left for 120 seconds from the same two seeds finished with 65 addresses of
which 47 answered. A seed that repeats what it was told hands out that 28 per
cent as if it were peers, and every node starting from it spends outbound slots
on them. This one figure is what the TM2 layer exists for.

**Loopback addresses circulate on the public network.** The probe received and
dialled two of them, and kept retrying them for three minutes. The core shares
them on purpose, since it does not apply routability to what it answers.

**A real node holds more than it can serve.** One pearl node holding 75 peers
could offer 51 of them: 24 announce an unspecified address and are filtered out
of every discovery answer. Those 51 are then capped at 30 per answer.

**Holding a connection is the exception on this network.** Four out of 67, three
times over. A design that composes, verifies and hangs up does not need to hold,
which is the one thing this measurement settles in its favour.

### 6.8 What is not measured

Named here so that nothing above is read as covering it:

- the memory cost of a TM2 seed holding many inbound connections at once;
- how fast a TM2 seed's inbound slots fill when nothing hangs up;
- compile duration, which the Go cache would falsify without a full clean;
- running behaviour beyond startup, on either stack.

---

## 7. Compatibility contract

This fork is a drop-in replacement. The following is preserved deliberately,
because a public Ansible role deploys Tenderseed in production and would break
otherwise:

1. the binary is named `tenderseed`;
2. top-level flags `-home`, `-config`, `-chain-id`, `-seeds`, with `-stack`
   added;
3. subcommands `start` and `show-node-id`, with `version` added;
4. the home layout: `config/config.toml`, `config/node_key.json`, and the book at
   `data/addrbook.json`. **The layout is preserved, the file contents are not.**
   A home belongs to one stack: the node key and the address book have no format
   in common between Cosmos and TM2, and a `seeds` list written for one stack is
   not transposable to the other;
5. **a partial `config.toml` is still accepted.** Decoding starts from the
   defaults, so any key left out keeps its default value;
6. **an unrecognised key is reported, never refused.** A configuration written
   for a later version still starts on an earlier binary, minus the settings that
   binary does not know;
7. the `TENDERSEED_CHAIN_ID` and `TENDERSEED_SEEDS` environment variables, with
   flags taking priority;
8. **a `config.toml` written for a v1 or a v2 keeps working when only the binary
   is replaced.** One exception, introduced in v3.0.0 and named in its release
   notes: a configuration that enables the metrics endpoint while leaving the
   namespace empty used to start and now refuses to.

---

## 8. What this fork does not do

Decided and assumed:

- **no multiple chains inside one process.** One process serves one chain, which
  is what Tenderseed, cometseed and cosmoseed all do. Several homes on several
  ports serve several chains;
- **no two stacks inside one process.** A process serves Cosmos or TM2,
  declared in its configuration. The two stacks share a binary, not a run;
- **no chain discovery through a registry**;
- **no front end, no dashboard, no HTTP peer-list endpoint.**

---

## 9. Other changes

- **Every panic is gone.** Errors are printed to standard error with a non-zero
  exit code, which a unit file or a script can act on. A `Restart=always` unit
  facing a bad configuration now loops on a readable message instead of a
  goroutine dump.
- **The container workflow never publishes an intermediate state.** Upstream
  pushed a public image on every push to its default branch, with no build check,
  no lint and no test. Here it fires on a version tag, and a manual run started
  from anything other than a tag is skipped, so `:latest` can only ever point at
  a released tag.
- **Every Go file passes `gofmt`**, which was true of none of them upstream.
- **The seed registers its own address in its book**, and the verification path
  skips any address carrying its own node ID, so verification never dials the
  seed itself. Upstream does this for a full node but Tenderseed never did. The
  book compares the full address string when checking for itself, so an
  unspecified `laddr` could never match the address peers advertise for the
  seed. Such an address is therefore not registered at all, and the node ID
  check is what actually holds.
