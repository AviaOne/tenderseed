# Tenderseed

A lightweight seed node for **CometBFT** and **Tendermint2** p2p networks.

Maintained fork of [binaryholdings/tenderseed](https://github.com/binaryholdings/tenderseed)
by [AviaOne.com](https://aviaone.com), rebuilt on CometBFT `v0.40.x`, taught to
verify the addresses it hands out, and since v3.0.0 able to serve gno.land as
well.

One binary serves either family. Which one it serves is declared once, at
install time, and everything else in this document is the same for both.

Released under semantic tags, starting at **v2.0.0**. Upstream carries no tag at
all and has not moved since February 2023, so the major version marks the break:
everything before this is Tenderseed v1. The binary, the flags and the file
layout are unchanged, so it is a drop-in replacement. The current release is on
the [releases page](https://github.com/AviaOne/tenderseed/releases).

A seed node keeps an address book of live peers and hands out addresses to nodes
that ask for them. It does not relay or store blocks or transactions, so it costs
almost nothing to run.

---

## Already running Tenderseed? Read this first

If you operate a seed built from the upstream project, three measured problems
affect you right now. Everything in this section was measured on Cosmos chains,
which is the only family upstream ever served.

| Problem in upstream Tenderseed | What it means for you | Fixed here |
|---|---|---|
| Built on Tendermint `v0.34.22`, end of life | No security fixes, no compatibility with current chains | Runs on CometBFT `v0.40.x`, an actively supported family |
| The address book is never qualified. `MarkGood` is never called, so every entry stays in a *new* bucket and the 70% bias toward *old* buckets has nothing to select | Your seed hands out addresses that may be long dead. It serves noise instead of peers | Addresses are dialled before being served, and the served selection is re-checked on a timer |
| `SeedDisconnectWaitPeriod` is never set, so it is zero. Every crawled peer is dropped on the first crawl round, often before it has answered | Your address book barely grows | The value is exposed, documented, and defaults to 5 minutes |

**Where it starts.** Two production seeds ran the upstream binary for years and
held **4 and 6 addresses**, not one of which had ever been verified. Only the
binary was replaced: same machine, same configuration, same address book files,
same chains.

**What follows has two regimes**, and quoting only the first would be
misleading:

- **Inside the first day**, a book started from empty climbs to about **1263 and
  568** addresses on those two chains. Reproduced over two independent cycles.
- **In steady state**, past roughly 35 hours, the book settles at **ten to twenty
  addresses, nearly all of them verified reachable**: 10 of 10 and 14 of 15 after
  eight days. Upstream evicts an address after seventeen failed dials, and on a
  seed that eviction is permanent until the process restarts, which is where the
  rest goes. A book of ten live addresses is not a smaller book, it is the same
  book without the dead entries.

**The figure that matters to you is what a new node gets from the seed**, and it
is the one you can reproduce: start a node with an empty address book and one
single seed, wait 90 seconds, count what it collected. From this seed, a new node
collected **473 addresses**. Of the eight public seeds tested the same way on the
same chain, the best returned 781 and four returned nothing at all. Method and
limits in [FORK.md](FORK.md), section 5.4.

---

## What this fork changes

Fourteen changes, all measured against upstream. See [FORK.md](FORK.md) for the
evidence behind each one.

1. **Supported p2p stack.** CometBFT `v0.40.0`, pinned, instead of Tendermint
   `v0.34.22` which is end of life.
2. **Project identity.** Detached repository, semantic tags, published releases.
   Upstream has no tag at all and has not moved since February 2023.
3. **Served peers are qualified.** Addresses are dialled, then `AddAddress`
   followed by `MarkGood` on success, `MarkAttempt` on failure. Verification
   dials run in parallel through a worker pool, and remember their verdicts:
   a failing address is re-tried on the upstream exponential schedule instead
   of at every sweep, and one just verified is not dialled again immediately.
4. **Crawl connection lifetime.** `seed_disconnect_wait_period` is exposed and
   documented, default 5 minutes.
5. **p2p configuration surface.** Parameters are actually wired through to the
   `Switch`, not merely logged. `allow_duplicate_ip` is configurable instead of
   hardcoded.
6. **Startup.** A seed with a populated address book starts without any seed
   configured. Upstream panics.
7. **Observability.** Prometheus metrics on a dedicated address, off by default,
   including counters for the verification itself, which nothing upstream
   reports. Configurable moniker.
8. **Code structure.** An explicit seed instance type replaces a 163-line
   monolith, so the construction order (channels, transport, reactors,
   `SetNodeInfo`) is explicit and testable.
9. **Tests.** Unit tests, including one that loads a real partial `config.toml`.
   Upstream has none, although its Makefile declares a `test` target.
10. **Continuous integration.** Build, `vet`, tests, `gofmt` and a pinned
    linter run on every push. Upstream only pushed a container image, with no
    verification of any kind.
11. **Publication.** Semantic tags and releases with binaries.
12. **Documentation.** An accurate README. Upstream still advertises limits its
    own code does not apply, and describes itself as a fork of a different
    project.
13. **Self-reference fix.** The seed registers its own address in its book when
    `laddr` names a real address, and the verification path skips any address
    carrying its own node ID, so verification never dials the seed itself. An
    unspecified `laddr` such as `0.0.0.0` is not registered, because the book
    compares full address strings and such an entry could never match.
14. **A second family of chains.** Since v3.0.0 the same binary also serves
    Tendermint2, the p2p code of gno.land, where it answers from a verified
    address book instead of from the connections it happens to hold, and closes
    every connection that has lasted long enough, so its slots keep turning
    over. Neither exists in that stack. One process serves one family, declared
    at install time.

---

## Installation on Linux (Ubuntu / Debian)

### Requirements

- A Linux server. Tested on Ubuntu 22.04 and 24.04, x86_64.
- **Go 1.25 or later.** Go is needed to build the binary. Once built, the binary
  is self-contained and Go is no longer required to run it, so you may build on
  one machine and copy the result to another.
- One open TCP port per chain served. The default is 26656.

First check whether a usable Go is already installed:

```bash
go version
```

If it prints `go1.25.0` or later, skip to step 1. Otherwise install Go into its
own versioned directory. **Nothing existing is removed**, so any Go already
present on the machine, and anything depending on it, keeps working:

```bash
GO_VER=1.25.0
curl -fsSL https://go.dev/dl/go${GO_VER}.linux-amd64.tar.gz -o /tmp/go.tgz
sudo mkdir -p /usr/local/go${GO_VER}
sudo tar -C /usr/local/go${GO_VER} --strip-components=1 -xzf /tmp/go.tgz
/usr/local/go${GO_VER}/bin/go version
```

Use it for this session only, without touching any profile file:

```bash
export PATH=/usr/local/go${GO_VER}/bin:$PATH
go version
```

If you want it permanently for your own account, append that `export` line to
your `~/.bash_profile`. Do not write it into `/etc/profile.d/`, which would
change the Go version for every user and every service on the machine.

### Step 1 - Set the chain you are serving

Every command below uses two shell variables, so you can copy and paste them
without editing anything. Set them once, in the terminal you are working in:

```bash
export CHAIN_ID=cosmoshub-4
export SEED_PORT=26656
export STACK=cosmos
```

Replace `cosmoshub-4` with the chain you serve, and pick a free port if 26656 is
already taken on your machine.

`STACK` is the p2p family the chain belongs to: `cosmos` for a CometBFT chain,
`tm2` for gno.land. Nothing in a chain identifier says which one it is, on
either side, so it cannot be guessed and you have to state it. It decides the
format of your seed identity and of your address book, so **set it before step
4**, where the identity is created. A home directory belongs to one family and
cannot be moved to the other.

Two things to keep in mind:

- **These variables only live in the terminal you set them in.** If you close it
  or open a new one, run the two `export` lines again before continuing.
- **They are how you serve several chains.** One process serves one chain. To add
  a second chain, come back to this step, set `CHAIN_ID` and `SEED_PORT` to the
  new values, and run steps 1 to 7 again. Each chain then has its own home
  directory, its own port, its own identity, its own address book and its own
  systemd service, all built from the same commands.

If you use UFW, open the port your seed will listen on:

```bash
sudo ufw allow ${SEED_PORT}/tcp comment "tenderseed ${CHAIN_ID}"
```

### Step 2 - Create a dedicated system user

A dedicated user isolates the seed from the rest of the machine. Run this from
an account with sudo access:

```bash
sudo useradd -r -m -d /home/tenderseed -s /bin/bash tenderseed
```

### Step 3 - Build and install the binary

```bash
sudo -u tenderseed env PATH="$PATH" bash -c 'cd ~ && git clone https://github.com/AviaOne/tenderseed && cd tenderseed && make build'
sudo install -m 0755 /home/tenderseed/tenderseed/build/tenderseed /usr/local/bin/tenderseed
tenderseed --help
```

The binary is now at `/usr/local/bin/tenderseed`.

### Step 4 - Create the home directory for one chain

One process serves one chain. Each chain gets its own home directory, its own
port, and its own systemd service.

```bash
sudo -u tenderseed mkdir -p /home/tenderseed/.tenderseed/${CHAIN_ID}
sudo -u tenderseed tenderseed -home /home/tenderseed/.tenderseed/${CHAIN_ID} -stack ${STACK} show-node-id
```

That command does two things: it creates `config/config.toml` and
`config/node_key.json` if they do not exist, and it prints your node identity.
`-stack` is what tells it which identity format to create, and it is recorded in
the configuration, so you do not have to pass it again.

The identity looks like `0123456789abcdef0123456789abcdef01234567` on a Cosmos
chain, and like `g1lhfv35wyvr9ggtnvjluwvsujnazeqjs050tgek` on gno.land. Other
operators need it to reach your seed, in the form
`<node-id>@<your-host>:<port>`.

> **Keep `config/node_key.json` safe.** It *is* your seed's identity. If you
> delete or regenerate it, your seed address changes for everyone who
> referenced it. Back it up.

### Step 5 - Configure

The first run wrote a complete `config.toml` containing every option, its
default value, and a comment describing it. What you may have to change sits at
the top, under a banner; everything below it has a working default. Open it:

```bash
sudo -u tenderseed nano /home/tenderseed/.tenderseed/${CHAIN_ID}/config/config.toml
```

The two settings you must fill in:

```toml
# network identifier of the chain this seed serves, the value of CHAIN_ID
chain_id = "cosmoshub-4"
# seed nodes we can use to discover peers
seeds = "<node-id>@<host>:<port>,<node-id>@<host>:<port>"
```

Seed addresses carry the identity format of their own family, so a list written
for one is unusable on the other. Get the ones for your chain from its Chain
Registry entry, or from [ABS](https://aviaone.com/blockchains-service/).

On gno.land only, one further key may need a value:

```toml
# tm2 only: value announced for the "app" entry of the version set
app_version = ""
```

It belongs to the chain rather than to this binary, and empty matches what
gno.land announces today.

The listening port, which must match the `SEED_PORT` you opened in UFW:

```toml
# Address to listen for incoming connections
laddr = "tcp://0.0.0.0:26656"
```

Every other key already has a working default. The full reference is in the
[Configuration](#configuration) section below.

Once the address book is populated, `seeds` may be emptied: the seed will start
from its own book.

### Step 6 - Create the systemd service

Create the unit file. This writes it for you, filled in with your chain:

```bash
sudo tee /etc/systemd/system/tenderseed-${CHAIN_ID}.service > /dev/null <<EOF
[Unit]
Description=tenderseed - seed node for ${CHAIN_ID}
After=network-online.target
Wants=network-online.target

[Service]
User=tenderseed
Group=tenderseed
Type=simple
ExecStart=/usr/local/bin/tenderseed -home /home/tenderseed/.tenderseed/${CHAIN_ID} start
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
```

For reference, this is what it contains:

```ini
[Unit]
Description=tenderseed - seed node for cosmoshub-4
After=network-online.target
Wants=network-online.target

[Service]
User=tenderseed
Group=tenderseed
Type=simple
ExecStart=/usr/local/bin/tenderseed -home /home/tenderseed/.tenderseed/cosmoshub-4 start
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Then enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tenderseed-${CHAIN_ID}
sudo systemctl status tenderseed-${CHAIN_ID}
```

### Step 7 - Check that it works

Watch the log. You should see peers being dialled within seconds:

```bash
sudo journalctl -u tenderseed-${CHAIN_ID} --no-hostname -f
```

Check that the address book is filling up. After a few minutes it should hold
far more than a handful of entries:

```bash
# cosmos
sudo -u tenderseed python3 -c "import json;print(len(json.load(open('/home/tenderseed/.tenderseed/${CHAIN_ID}/data/addrbook.json'))['addrs']))"

# tm2
sudo -u tenderseed python3 -c "import json;print(len(json.load(open('/home/tenderseed/.tenderseed/${CHAIN_ID}/data/addrbook.json'))['peers']))"
```

The two families write the same file under two shapes, which is why the command
differs.

Verify the port is reachable from outside your machine, from another host:

```bash
nc -vz your-seed-host.example.com ${SEED_PORT}
```

### Serving more chains

Go back to **step 1**, set `CHAIN_ID` and `SEED_PORT` to the new chain and a free
port, then run steps 1 to 7 again in the same terminal. Nothing else changes: the
commands are identical, and the variables take care of every path, every port and
every service name.

Each chain then runs as its own process, with its own address book and its own
node identity.

### Service management

```bash
# Start
sudo systemctl start tenderseed-${CHAIN_ID}

# Stop
sudo systemctl stop tenderseed-${CHAIN_ID}

# Restart
sudo systemctl restart tenderseed-${CHAIN_ID}

# Status
sudo systemctl status tenderseed-${CHAIN_ID}

# Live logs
sudo journalctl -u tenderseed-${CHAIN_ID} --no-hostname -f
```

### Updating to a newer version

Only the binary is replaced. Your node identity, your configuration and your
address book are files on disk that the update never touches, so your seed
keeps the same address and the same peers.

```bash
export CHAIN_ID=cosmoshub-4
sudo -u tenderseed bash -c 'cd ~/tenderseed && git fetch --tags && git checkout "$(git tag --sort=-v:refname | head -n1)" && make build'
sudo systemctl stop tenderseed-${CHAIN_ID}
sudo install -m 0755 /home/tenderseed/tenderseed/build/tenderseed /usr/local/bin/tenderseed
sudo systemctl start tenderseed-${CHAIN_ID}
tenderseed version
```

That checks out the newest tag. Check out a specific one instead if you pin
versions, and repeat the two systemctl lines for every chain you serve. The
last command prints the tag the new binary was built from, so it confirms the
update landed.

Left untouched by the update, on every chain home:

- `config/node_key.json`, so your seed keeps the identity other operators
  reference;
- `config/config.toml`, which is only generated when it does not exist, so your
  settings survive. New keys added by a release fall back to their default
  until you add them by hand;
- `data/addrbook.json`, so the seed restarts with the peers it already knew.

---

## Installation with Docker

### Requirements

- Docker installed.
- One open TCP port per chain served.

### Step 1 - Get the image

Pull the published image, which is built for amd64 and arm64:

```bash
docker pull ghcr.io/aviaone/tenderseed:latest
docker tag ghcr.io/aviaone/tenderseed:latest tenderseed:latest
```

Pin a version instead of `latest` if you prefer, using a tag from the
[releases page](https://github.com/AviaOne/tenderseed/releases).

Or build it yourself from source:

```bash
git clone https://github.com/AviaOne/tenderseed && cd tenderseed
docker build -t tenderseed:latest .
```

### Step 2 - Create the data directory and generate the identity

The container stores everything under `/data`. Pass `--user` so that the files
it writes belong to you: the image user is a system account whose uid does not
match yours, and Docker never changes the ownership of a bind mount.

```bash
export CHAIN_ID=cosmoshub-4
export SEED_PORT=26656
export STACK=cosmos
mkdir -p ~/tenderseed-data/${CHAIN_ID}
docker run --rm --user "$(id -u):$(id -g)" \
  -v ~/tenderseed-data/${CHAIN_ID}:/data tenderseed:latest -stack ${STACK} show-node-id
```

This writes `config/config.toml` and `config/node_key.json` into
`~/tenderseed-data/${CHAIN_ID}` and prints your node identity.

### Step 3 - Configure

```bash
nano ~/tenderseed-data/${CHAIN_ID}/config/config.toml
```

Set `chain_id` and `seeds` as described in the Linux section. `stack` is
already recorded from step 2.

### Step 4 - Run

```bash
docker run -d --name tenderseed-${CHAIN_ID} \
  --restart unless-stopped \
  --user "$(id -u):$(id -g)" \
  -p ${SEED_PORT}:26656 \
  -v ~/tenderseed-data/${CHAIN_ID}:/data \
  tenderseed:latest
```

### Docker management

```bash
# Logs
docker logs -f --tail 50 tenderseed-${CHAIN_ID}

# Stop
docker stop tenderseed-${CHAIN_ID}

# Start
docker start tenderseed-${CHAIN_ID}

# Remove and rebuild after an update
docker rm -f tenderseed-${CHAIN_ID}
docker build -t tenderseed:latest .
```

### Updating to a newer version

Pull the new image, drop the container, start it again on the same bind
mount. The mounted directory holds `config/node_key.json`,
`config/config.toml` and `data/addrbook.json`, none of which the update
touches, so the seed keeps its identity, its settings and its peers.

```bash
docker pull ghcr.io/aviaone/tenderseed:latest
docker tag ghcr.io/aviaone/tenderseed:latest tenderseed:latest
docker rm -f tenderseed-${CHAIN_ID}
docker run --rm tenderseed:latest version
```

Then run the container again with the command from step 4.

---

## Configuration

Every key has a working default, and decoding starts from those defaults, so a
partial `config.toml` remains valid: any key you delete keeps its default value.

### Keys inherited from upstream

| key | default | what it does |
|---|---|---|
| `laddr` | `tcp://0.0.0.0:26656` | address and port to listen on for incoming connections |
| `chain_id` | empty | network identifier of the chain this seed serves |
| `stack` | `cosmos` | p2p family of that chain, `cosmos` or `tm2`. Empty means `cosmos`, so a file written before this key existed keeps its behaviour. An unknown value refuses to start |
| `app_version` | empty | gno.land only: value announced for the `app` entry of the version set. It belongs to the chain, not to this binary |
| `seeds` | empty | comma-separated `<node-id>@<host>:<port>` list used to bootstrap discovery. May be emptied once the address book is populated |
| `log_level` | `info` | `debug`, `info`, `warn`, `error` or `none`. It applies to the seed own lines as well, so `none` leaves only the startup banner |
| `node_key_file` | `config/node_key.json` | path to the node identity, relative to the home directory or absolute |
| `addr_book_file` | `data/addrbook.json` | path to the address book, relative to the home directory or absolute |
| `addr_book_strict` | `true` | strict routability rules. Set `false` for private or local networks, otherwise non-routable addresses are rejected |
| `max_num_inbound_peers` | `100` | how many nodes may be connected to your seed at once |
| `max_num_outbound_peers` | `60` | how many peers the seed dials while crawling |
| `max_packet_msg_payload_size` | `1024` | maximum message packet payload, in bytes |

### Keys added by this fork

| key | default | what it does |
|---|---|---|
| `seed_disconnect_wait_period` | `5m` | how long a connection may last before the seed closes it. Every connection, not only the peers it dialled: an inbound peer that never asks for anything holds a slot just as long, and a peer already served has no reason to stay. Upstream leaves this at zero, which drops peers on the first crawl round, often before they have answered. Too short and the book stays empty; too long and slots stop turning over. Both families apply it the same way, the Cosmos side through the core and gno.land through this seed |
| `peer_check_period` | `10m` | how often the addresses the seed would serve are re-verified. Shorter means a fresher book at the cost of more outbound traffic. `0` disables verification entirely, which restores upstream behaviour. On gno.land it also bounds how many addresses the seed may call fresh, since it can only promise fresh what it is able to prove again inside the window: a much shorter period there buys freshness by serving fewer addresses |
| `peer_check_workers` | `8` | how many verification dials run in parallel. A sweep of 250 addresses takes about 29 minutes sequentially and about 3m40 with 8 workers. Lower it on a constrained machine. It has no effect on gno.land, where the sweep hands its addresses to the switch in one call instead of dialling them itself |
| `allow_duplicate_ip` | `true` | allow several peers behind a single IP address. Setting it to `false` also changes the meaning of "already connected", so it interacts with verification |
| `metrics_listen_addr` | empty | address to serve Prometheus metrics on, for example `127.0.0.1:26660`. Empty disables the endpoint. A port already taken is logged and the seed keeps serving peers, unlike an unusable `metrics_namespace` which refuses to start: the first can resolve itself, the second never will |
| `metrics_namespace` | `cometbft` | prefix of every exported series. Matches the upstream default, so dashboards written for a full node work unchanged. The same prefix is used on gno.land, where the name is inherited rather than accurate |
| `moniker` | empty | name announced to peers. Empty means `<chain_id>-seed` |

### Flags and environment variables

Command-line flags override the file, and environment variables override the
file but not the flags.

```
-home       home directory (default ~/.tenderseed)
-config     path to config.toml, relative to home or absolute
-chain-id   overrides chain_id
-seeds      overrides seeds
-stack      overrides stack, and sets it when config.toml is created
```

```
TENDERSEED_CHAIN_ID
TENDERSEED_SEEDS
```

Subcommands: `start`, `show-node-id` and `version`.

### Version announced to peers

The seed announces its software version during the p2p handshake. Read what a
binary announces with:

```bash
tenderseed version
```

A release binary announces the tag it was built from, without the leading
`v`: a `v2.1.0` tag produces a binary announcing `2.1.0`. A binary built
straight from a working copy announces the value compiled into the source.
Override it at build time if you package your own build:

```bash
go build -ldflags "-X github.com/AviaOne/tenderseed/internal/tenderseed.Version=2.0.1-mybuild" ./cmd/tenderseed
```

### Metrics

Set `metrics_listen_addr`, restart, and scrape it. `metrics_namespace` is the
prefix of every series, `cometbft` by default on both families:

```bash
curl -s 127.0.0.1:26660/metrics
```

The series differ by family, because the two seeds take different decisions.
What follows is the whole of what each exports. A Cosmos seed also carries the
p2p series of its core, which the gno.land core does not have.

#### On a Cosmos seed

Verification reports its own work, which nothing upstream counts. One series,
two labels, ten reachable pairs, and they always sum to the number of decisions
taken:

```bash
curl -s 127.0.0.1:26660/metrics | grep '^cometbft_seed_verify_dials_total'
```

| outcome | stages | meaning |
|---|---|---|
| `success` | verify | the address answered and was promoted |
| `answered_unlisted` | verify | the address answered but the book refused to hold it, so nothing was promoted. Strict routability is the usual cause |
| `failure` | verify | the dial failed and the attempt was marked against the book |
| `skipped_backoff` | enqueue, verify | a previously failing address is still inside its backoff |
| `skipped_fresh` | enqueue, verify | the address was verified recently enough to be trusted |
| `skipped_local` | verify | nothing was dialled and nothing learned: our own address, a banned one, a connection already open or under way, or a defensive guard |
| `skipped_collision` | verify | a dial happened and taught nothing: a peer connected to us while we were dialling it. This is the count of unfair marks avoided on live addresses |
| `dropped_full` | enqueue | the queue was full, the address was dropped |

The `stage` label says where the decision was taken, and it is what bounds the
traffic actually saved. A skip at `verify` is a dial that would certainly have
happened, since the address had already taken a queue slot: those are the lower
bound. A skip at `enqueue` only avoided an offer to the queue, and whether that
offer would have become a dial depends on an occupancy no counter can
reconstruct: the total of both stages is the upper bound.

`skipped_backoff` rising while `failure` falls is the backoff working.
`dropped_full` rising is not: it means the queue is saturated and the sweep is
no longer covering the selection. Without a Prometheus setup the same figures
appear once per sweep in the logs, as a `verification sweep` line at info level.

#### On a gno.land seed

Two series, under the same rule: one pair per behaviour an operator can act on,
never one per branch of the code, and every reachable pair published at zero so
a share can be read from the first scrape.

```bash
curl -s 127.0.0.1:26660/metrics | grep '^cometbft_seed_tm2_decisions_total'
```

| outcome | stage | meaning |
|---|---|---|
| `served` | serve | a request was answered with addresses |
| `empty` | serve | a request arrived and the seed had no fresh address to give. This is the series that matters most, because a seed serving nothing looks healthy from the outside: it still listens, accepts and answers |
| `failed` | serve | the peer that asked did not take the answer, so it was hung up on instead of being waited for |
| `accepted` | learn | an address announced by a peer was kept |
| `rejected` | learn | an address announced by a peer was refused, as invalid or as unroutable under `addr_book_strict` |
| `retried` | sweep | a stale address was handed to the switch to be dialled again |
| `dropped` | sweep | an address left the book after five consecutive failures |
| `skipped_connected` | sweep | a stale address was not tried because this seed already holds a connection to it. Nothing is counted against it: the switch skips an address it is already connected to, so no attempt takes place and none is claimed |
| `skipped_budget` | sweep | a stale address was not tried because the switch could not have taken it within this period, on free slots or on dialling rate. It comes first at the next sweep, being then the oldest news |
| `cycled` | cycle | a connection was closed for having lasted longer than `seed_disconnect_wait_period` |

The second series is the book:

```bash
curl -s 127.0.0.1:26660/metrics | grep '^cometbft_seed_tm2_book_addresses'
```

`known` is every address held, `fresh` is the part of it the seed may serve.
`known` standing above `fresh` is the normal state and not a fault: an address
is only called fresh while the seed is able to prove it again inside the
freshness window, and what lies above that waits its turn rather than being
handed out on an expired proof. `empty` rising while `known` is large means the
seed knows addresses it has not been able to reach, not that it has none.

---

## About AviaOne

[AviaOne](https://aviaone.com) has been a professional Cosmos validator for over
three years, and builds tooling for the ecosystem rather than for itself alone.

Its main contribution is
**[ABS, AviaOne BlockChains Service](https://aviaone.com/blockchains-service/)**,
a live directory covering more than 350 blockchains. ABS turns raw Chain Registry
data into something operators can rely on: RPC endpoints tested every three hours
and ranked by latency, block explorers rechecked daily so dead links disappear,
the binary version genuinely running on each network compared against what the
registry claims, and live network status.

This fork comes from the same place. Seed nodes are shared infrastructure: every
chain relies on them to bootstrap, yet the tooling behind them has been
unmaintained for years. Fixing that helps everyone running a Cosmos network, not
just us.

## Credits

- **Original**: [tenderseed](https://github.com/binaryholdings/tenderseed) by
  binaryholdings, itself a fork of the polychainlabs project, originally written
  by Roman Shtylman.
- **Porting precedent**:
  [BitCannaGlobal/tenderseed](https://github.com/BitCannaGlobal/tenderseed)
  ported the project to CometBFT `v0.37.2` in nine lines outside `go.mod`,
  which showed the migration was mechanical rather than structural and shaped
  the approach taken here. No code was taken from it.
- **Address verification design**: follows
  [voluzi/cosmoseed](https://github.com/voluzi/cosmoseed), which solved the same
  problem on the CometBFT v2 API. The design is credited; no code was copied, as
  the v2 API is incompatible.
- **Fork**: [AviaOne.com](https://aviaone.com).

## Disclaimer

This fork is maintained by AviaOne.com alone, with no funding or sponsorship.
We aim for quality but cannot promise the response times of a funded project.
If you hit a bug, open an issue and we will do our best.

## License

Blue Oak Model License 1.0.0, unchanged from upstream. See
[LICENSE.md](LICENSE.md).
