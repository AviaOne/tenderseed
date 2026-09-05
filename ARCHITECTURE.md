# Architecture

Where things live in this repository, and what is true of the ground under
them. The README is how to run a seed. FORK.md holds the evidence and the
anchors behind every claim made about the code. The release notes carry the
history, one file per tag. This file carries none of those: it is the map and
the terrain, for whoever picks the repository up next.

One binary serves two p2p families. **TM1** is CometBFT and the Cosmos chains.
**TM2** is the p2p code of gno.land. One process serves one chain of one
family, declared in its configuration; a home directory belongs to one family
and cannot be moved to the other.

---

## 1. Repository map

```
cmd/tenderseed/main.go      flags, environment, subcommands
internal/cmd/               the three subcommands
internal/tenderseed/        everything the seed is
docs/release-notes/         one file per tag
.github/workflows/          go, release, docker
Makefile Dockerfile .golangci.yml scripts/bench.sh
```

`cmd/tenderseed/main.go` parses the top level flags, reads the two environment
variables, loads or creates the configuration, and registers the subcommands.
It settles the family before a configuration file can be created, because the
same first command creates the node identity and the identity format belongs to
the family. It imports neither core.

`internal/cmd/` holds `start`, `show-node-id` and `version`. None of the three
imports a core; each one delegates.

`internal/tenderseed/` holds the rest, in three groups. The grouping is not a
declaration, it is what an import survey of the package returns:

| file | group | what it is |
|---|---|---|
| `node.go` | **boundary** | the interface, the constructor, the identity. The only file importing both cores |
| `seed.go` | TM1 | the seed instance: transport, book, PEX reactor in seed mode, switch |
| `seedreactor.go` | TM1 | the reactor that verifies the addresses the seed hands out |
| `seedtm2.go` | TM2 | the seed instance: transport, book, discovery reactor, switch |
| `seedreactortm2.go` | TM2 | the reactor that answers from the book and hangs up |
| `seedbook.go` | TM2 | the address book and its verification state |
| `seedquiet.go` | see below | the log filter for the seed's own hang up |
| `config.go` | common | the keys, their defaults, validation, unknown keys, the generated file |
| `metrics.go` | see below | the metrics server, and the TM1 counters |
| `metricstm2.go` | see below | the TM2 counters |

Three files carry no core import and are placed by content instead. The two
metrics files import Prometheus alone, which belongs to both families.
`seedquiet.go` imports neither core either: it recognises the error value the
TM2 reactor records when the seed hangs up, and that value lives in the same
package. Every other line of the table is what its imports say it is, and an
import survey is what checks it.

Tests sit beside their subject: `config_test.go` on the common side,
`seedreactor_test.go` on TM1, `seedbook_test.go` and
`seedreactortm2_test.go` on TM2.

**How the two cores are pinned, which is not symmetric.** `go.mod` requires
CometBFT at a released version and gno at a pseudo-version, that is at a
commit. The TM2 side therefore has no version line to follow: raising it means
choosing a commit, and the choice has to be remeasured on both families, since
one binary carries both. The Go directive follows whatever that graph
requires, and the Dockerfile pins the Go image to one minor line, so the first
dependency asking for the next one forces an edit there.

---

## 2. The boundary

`Node` is an interface of four methods, `Start`, `Wait`, `Stop`,
`TrapSignal`. `NewNode` reads the configured family and returns the matching
implementation. `NodeID` does the same for the identity. Both live in
`node.go`, and that file is the single place where the two branches meet.

Above the boundary nothing is duplicated: the flags, the environment
variables, the configuration keys with their validation, the whole Prometheus
surface, the version subcommand.

Below it everything is per family: transport, address book, reactor, switch,
and the logger they use.

`TrapSignal` belongs to the interface for one reason. The shutdown handler
logs while it shuts down, and the logger type is the one thing the two
families do not share, `log.Logger` against `*slog.Logger`. Leaving the trap
to the family is what keeps `internal/cmd/start.go` free of any core import.

**The rule to keep.** A new file belongs to one of the three groups, and only
`node.go` may sit in two. An import survey of the package is how that is
checked, not a reading.

---

## 3. The two cores, side by side

The same question asked of both, which is what no other document does: FORK.md
answers each side in its own section, and the two are far apart. Evidence and
line anchors are there, TM1 in sections 2.2, 2.3 and 3.6, TM2 in 6.2, 6.3 and
6.5. What follows is the state of both at once.

| | TM1 | TM2 |
|---|---|---|
| address book | buckets, persistent, keyed by node identity | one flat store, a thousand entries, keyed by the full address string |
| qualification of an address | exists, and on a seed nothing calls it: only the consensus reactor promotes, and a seed runs none | does not exist. The stored *last seen* dates a mention, never a contact |
| what a seed answers from | its book, through a biased selection | the peers it is connected to at that instant, capped at thirty, whatever the store holds |
| closing an idle connection | a served inbound peer is closed on the spot, and every other non-persistent one on a configured delay, inbound peers that never asked included | nothing closes anything. Pings keep an idle peer alive indefinitely |
| routability of what is shared | applied, and configurable | deliberately skipped, so that loopback addresses stay usable in local clusters |
| dropping a peer from outside the core | a graceful path exists | one exported path, which reports every use of itself as a failure |
| refusing itself | the transport refuses its own identity | the transport does not. Only the switch and the store step around themselves |
| identity | a twenty byte address in hexadecimal | a bech32 string prefixed `g1` |
| key file, address string, `seeds` list | one format | another. Nothing transposes |
| wire format | protobuf, on the PEX channel | amino, on the discovery channel |
| the one handshake value that belongs to the chain and not to the binary | the block protocol version, taken from the pinned core | the `app` entry of the version set, which is why `app_version` is a key |

**Both cores leave the same hole, reached from opposite directions.** On TM1
the qualification is written and never called on a seed; on TM2 it was never
written. Either way a stock seed hands out addresses it has never reached.
That hole is what this fork fills, and it fills it twice.

### The keys, on both families

FORK.md section 4 anchors the default values, and it was written for Cosmos
chains alone. What those same keys are worth on the other side:

- **Same meaning on both**: `laddr`, `chain_id`, `seeds`, `log_level`,
  `moniker`, `node_key_file`, `addr_book_file`, `max_num_inbound_peers`,
  `max_num_outbound_peers`, `max_packet_msg_payload_size`,
  `allow_duplicate_ip`, `metrics_listen_addr`, `metrics_namespace`,
  `seed_disconnect_wait_period`.
- **`peer_check_period`** means the same thing on both, and carries one more
  consequence on TM2: it bounds how many addresses may be called fresh, since
  a seed can only promise fresh what it is able to prove again inside the
  window. Shortening it there buys freshness by serving fewer addresses.
- **`addr_book_strict`** has no equivalent in the TM2 core, which has no notion
  of strict routability at all. This layer honours it there anyway, on the way
  in as well as on the way out.
- **`peer_check_workers` has no effect on TM2.** The sweep hands its addresses
  to the switch in one call instead of dialling them itself, so the
  parallelism belongs to the switch. The key stays accepted and stays
  meaningful on TM1.
- **`app_version` is TM2 only**, and empty on TM1 by construction: the
  equivalent value there is a constant of the pinned core.
- **`stack`** is the one key that decides which of the two columns above
  applies. Absent, it is TM1, which is what every version up to v2.2.2 did.

---

## 4. Constraints that are fatal if ignored

Each of these was paid once. Six of them are stated in the code at the point
where they apply, and FORK.md carries the evidence; the seventh shows only in
the way the TM2 tests build an identity. The list is here so that none of them
has to be found again.

1. **Announced channels may not exceed the registered reactors.** A
   connection's channel descriptors come only from the reactors, on both
   families. Announce more and a peer will send on a channel the receive loop
   cannot find, which stops the connection. Measured on TM2: a probe
   announcing the seven channels of a full node held zero peers for three
   minutes, where the same probe announcing the discovery channel alone held
   its usual few. A seed announces one channel because it serves one.

2. **Node info reaches the transport by value, and the handshake uses that
   copy.** The channel list must be complete before the transport is built. A
   reactor registered afterwards never reaches what peers are told. True on
   both families, and it is why the construction order in both seed
   constructors is not free.

3. **TM1: override `Start`, not `OnStart`.** The upstream PEX constructor pins
   the service implementation to the reactor it returns, so `BaseService.Start`
   would only ever call that one.

4. **TM1: hand every message to the upstream reactor first.** Its receive path
   is what clears the pending request state for a peer. Skip it and the seed
   never asks that peer for addresses again while the connection lasts.

5. **TM2: an empty answer is not sendable.** The receiving side refuses a
   response carrying no peer. A seed that has verified nothing yet stays
   silent and hangs up, rather than sending an empty list.

6. **`-stack` settles the family when the configuration is created, and may
   afterwards confirm it but never contradict it.** The family has a shape on
   disk, the key file and the book. Measured: a home whose key file had been
   removed, which is what an identity rotation does, was given a key of the
   flagged family while its configuration still named the other, and the next
   start failed. Changing the family of an established home is a new home, not
   a flag.

7. **A TM2 identity is not a free string.** It is validated by being decoded as
   an address, so a test or a tool builds one through the crypto package.
   Building one as forty hexadecimal characters, by analogy with TM1, produces
   a test that passes or fails for the wrong reason.

---

## 5. Known limits

Each of these is either measured on a running seed or read in the core source
at the version pinned here. Two are read only, and stay read only because the
default never exercises them: the cost of turning duplicate IP off, and the
absence of jitter in the backoff.

### TM1

- **A ban never expires on a seed, and does not survive a restart.** The only
  production caller that reinstates a banned address belongs to the routine a
  node runs when it is *not* a seed. Nothing reinstates anything here, and the
  list is not persisted, so a restart clears it rather than restoring it.
- **The book settles at ten to twenty addresses in regime**, past roughly
  thirty-five hours, by upstream eviction. That is the regime and not a
  defect. The first day figures are much larger, and quoting them alone is
  what misleads.
- **`allow_duplicate_ip = false` costs more than the key suggests.** The
  already connected test then resolves the address of every peer at every dial
  decision, through a cache that never holds because its receiver is a value,
  and it panics if a resolution fails. The default is `true` and no duplicate
  IP filter is registered, so the path is not taken in production. It stays
  exposed and documented as adjustable.
- **The backoff has no jitter**, unlike the upstream formula whose scale it
  shares, so retries on one address are perfectly periodic where upstream
  spreads them.

### TM2

- **`peer_check_workers` has no effect**, see section 3.
- **The core logs two error lines per disconnection**, saying it closed what
  was already closed. Only the seed's own hang up is filtered down to
  information, because it is the only one recognisable by an error value
  rather than by the wording of a message. The rest is what any TM2 node
  produces and does not belong to this project.
- **A peer that does not drain the discovery channel is served nothing.** The
  answer is sent from the receive loop of that same connection, so a blocking
  send would stop this seed from reading that peer for as long as it refused to
  take the answer, once per request, on a channel where requests are not rate
  limited. The send is therefore non blocking, and such a peer is hung up on
  rather than waited for. The trade is deliberate: the alternative is a reading
  loop a third party can freeze on demand.
- **Holding a connection is the exception on this network.** A node announcing
  the discovery channel alone keeps a handful of connections against dozens of
  known addresses, reproduced three times, twice under a different identity. A
  design that dials, verifies and hangs up does not need to hold. A design
  that answers from its connections does, which is exactly why this one
  answers from its book.

### Both

- **The default metrics prefix is `cometbft` on both families.** On TM2 the
  name is inherited rather than accurate. Changing it would rename every
  series of every existing Cosmos seed, breaking dashboards that work today
  for no gain, and a key whose default depended on the family would be one key
  meaning two things. It is configurable.
- **A process serving one family still carries the other, and that costs
  memory.** Both families live in one package, so both dependency graphs are
  linked into the binary and initialised in every process, whichever family is
  configured. Measured on two versions running side by side on the same chain
  for half a day: the resident memory of the one carrying both graphs sits
  about a third higher, and most of that is pages of the binary itself, which
  is about a third larger. What the Go runtime holds from the operating system
  is the same on both, and does not climb, so this is a fixed cost rather than
  something that grows. It changes no sizing decision: the dominant cost of a
  seed is still its per-peer buffers, and the whole gap is worth a few dozen
  connections. Worth re-measuring the day the pinned commit of the second
  family moves, since that is what sets it.

---

## 6. How things are measured here

- **Compare two builds side by side, never one after the other.** Same
  machine, same chain, same instant, distinct ports and homes. Two successive
  windows, or a comparison against an instance running elsewhere, measure the
  network as much as they measure the binary.
- **Build the binary inside the same command as the measurement, and read the
  startup line as the control.** Compiling the packages proves the code builds
  and produces nothing to run, so a measurement taken with a stale binary
  looks exactly like a valid one. The startup line lists every effective
  value, which is what says who answered.
- **What a seed is worth is what a fresh node collects from it.** A throwaway
  home, an empty book, one single seed configured, a fixed duration, then
  count the book. It is the one figure anyone else can reproduce.
- **A network can be measured without running a node on it.** A minimal
  client, transport and switch and the discovery reactor and one announced
  channel, reaches the network and reports what it is told. Constraint 1 above
  applies to it in full.
- **Say what a measurement will not prove before running it, and keep that
  beside the result.** Startup is not service. A run of minutes is not a run
  of days.
- **Never state a figure compared across the two families before reading the
  same layers on both.** A per peer cost read on one layer of one family and
  on two of the other has been stated wrongly here twice, each time as a
  finding rather than as a guess.

---

## 7. Repository rules

**The announced version.** It is a variable in the binary, overridden at build
time from an exact Git tag with its leading `v` removed. The Makefile supplies
it, the CI and the Dockerfile pass it explicitly. Off a tag the value compiled
into the source stands, so that value is raised in the source with the
release: a binary built from a working copy announces it to its peers.

**Release notes.** One file per tag under `docs/release-notes/`. The release
workflow uses it when it exists and generates notes when it does not, so a
missing file is silent rather than an error.

**What triggers what.** The `go` workflow runs on pushes to `master` and on
pull requests, so work living on a long branch is not seen by the CI until a
pull request opens. `release` and `docker` fire on a `v*` tag only, and both
skip a manual run started from anything else. `:latest` only ever moves
forward: it is pushed only when the tag being built is the newest one.

**The linter is pinned.** The workflow installs one exact version of the
tool; `.golangci.yml` declares the configuration format that file is written
in. The two numbers are not the same kind of thing, and what has to stay in
step is the major: a tool whose configuration format the file does not declare
will not read it. The point of pinning at all is that the CI cannot turn red
on untouched code because the tool released a new major.

**What is never done here.**

- **No figure that depends on the current commit enters a published
  document.** Binary sizes, build durations and counts of anything have to be
  remeasured, corrected and argued at every release, and no reader acts on
  them. What is published is the order of magnitude and the reason for it,
  both of which stay true from one version to the next.
- **The README and FORK.md are claims to check against the code, never
  sources.** Several of theirs have been taken in default, and one of them was
  a justification that outlived the change which made it false. If a fact is
  taken from either, it is verified in the code before being written down
  again.
- **An unrecognised configuration key is reported, never refused**, so a file
  written for a later binary still starts on an earlier one. An unrecognised
  family value *is* refused, because it would start the network code of the
  wrong chain and fail far from its cause.
- **The repository is in English, comments say why rather than what, and there
  are no em dashes.**
