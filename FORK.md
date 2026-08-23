# Why this fork exists

This document holds the evidence behind every claim made in the README. The
short version lives there; the measurements live here.

Everything below was measured on source code, on command output, or on a running
process. Where something is a judgement rather than a measurement, it says so.

---

## 1. The state of the field

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
  the art, 28 commits, 21 tags. Pinned to a CometBFT v2 release candidate that
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

---

## 4. What the defaults are anchored to

| key | default | anchor |
|---|---|---|
| `seed_disconnect_wait_period` | `5m` | judgement, checked at runtime, see 2.3 |
| `peer_check_workers` | 8 | a selection returns at most 250 addresses and a dial costs at most 4 seconds, so a sequential sweep takes about 17 minutes and an 8-worker sweep about 2 |
| `peer_check_period` | `10m` | five times the minimum interval between crawls, and five times the duration of one sweep. `0` disables |
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
for part of the gap. What is measured is two nearly empty books on one side and
more than eighteen hundred addresses across two chains on the other.

---

## 6. Compatibility contract

This fork is a drop-in replacement. The following is preserved deliberately,
because a public Ansible role deploys Tenderseed in production and would break
otherwise:

1. the binary is named `tenderseed`;
2. top-level flags `-home`, `-config`, `-chain-id`, `-seeds`;
3. subcommands `start` and `show-node-id`;
4. the home layout: `config/config.toml`, `config/node_key.json`, and the book at
   `data/addrbook.json`;
5. **a partial `config.toml` is still accepted.** Decoding starts from the
   defaults, so any key left out keeps its default value;
6. the `TENDERSEED_CHAIN_ID` and `TENDERSEED_SEEDS` environment variables, with
   flags taking priority.

---

## 7. What this fork does not do

Decided and assumed:

- **no multiple chains inside one process.** One process serves one chain, which
  is what Tenderseed, cometseed and cosmoseed all do. Several homes on several
  ports serve several chains;
- **no chain discovery through a registry**;
- **no front end, no dashboard, no HTTP peer-list endpoint.**

---

## 8. Other changes

- **Every panic is gone.** Errors are printed to standard error with a non-zero
  exit code, which a unit file or a script can act on. A `Restart=always` unit
  facing a bad configuration now loops on a readable message instead of a
  goroutine dump.
- **The container workflow was neutralised.** Upstream pushed a public image on
  every push to its default branch, with no build check, no lint and no test. It
  is now triggered manually.
- **Every Go file passes `gofmt`**, which was true of none of them upstream.
- **The seed registers its own address in its book**, and the verification path
  skips any address carrying its own node ID, so verification never dials the
  seed itself. Upstream
  does this for a full node but Tenderseed never did. The book compares the full
  address string when checking for itself, so with the default `laddr` of
  `0.0.0.0` that comparison cannot match the address peers advertise for the
  seed; the node ID check is what actually holds.
