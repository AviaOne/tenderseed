# Tenderseed

A lightweight seed node for **CometBFT** p2p networks.

Maintained fork of [binaryholdings/tenderseed](https://github.com/binaryholdings/tenderseed)
by [AviaOne.com](https://aviaone.com), rebuilt on CometBFT `v0.40.x` and taught to
verify the addresses it hands out.

Released as **v2.0.0**. Upstream carries no tag at all and has not moved since
February 2023, so the version marks the break: everything before this is
Tenderseed v1. The binary, the flags and the file layout are unchanged, so it is
a drop-in replacement.

A seed node keeps an address book of live peers and hands out addresses to nodes
that ask for them. It does not relay or store blocks or transactions, so it costs
almost nothing to run.

---

## Already running Tenderseed? Read this first

If you operate a seed built from the upstream project, three measured problems
affect you right now.

| Problem in upstream Tenderseed | What it means for you | Fixed here |
|---|---|---|
| Built on Tendermint `v0.34.22`, end of life | No security fixes, no compatibility with current chains | Runs on CometBFT `v0.40.x`, an actively supported family |
| The address book is never qualified. `MarkGood` is never called, so every entry stays in a *new* bucket and the 70% bias toward *old* buckets has nothing to select | Your seed hands out addresses that may be long dead. It serves noise instead of peers | Addresses are dialled before being served, and the served selection is re-checked on a timer |
| `SeedDisconnectWaitPeriod` is never set, so it is zero. Every crawled peer is dropped on the first crawl round, often before it has answered | Your address book barely grows | The value is exposed, documented, and defaults to 5 minutes |

**The measurement that matters.** Two production seeds ran the upstream binary
for years and held **4 and 6 addresses**. Only the binary was replaced with this
fork: same machine, same configuration, same address book files, same chains.
Four hours later those two seeds held **1263 and 568 addresses**, of which 28 and
40 were verified reachable and promoted.

That is a before and after on the same seeds rather than a benchmark, and one
reservation remains: `addr_book_strict` also rejects entries, so it may account
for part of the gap. What is measured is stated; nothing more is claimed.

---

## What this fork changes

Thirteen changes, all measured against upstream. See [FORK.md](FORK.md) for the
evidence behind each one.

1. **Supported p2p stack.** CometBFT `v0.40.0`, pinned, instead of Tendermint
   `v0.34.22` which is end of life.
2. **Project identity.** Detached repository, semantic tags, published releases.
   Upstream has no tag at all and has not moved since February 2023.
3. **Served peers are qualified.** Addresses are dialled, then `AddAddress`
   followed by `MarkGood` on success, `MarkAttempt` on failure. Verification
   dials run in parallel through a worker pool.
4. **Crawl connection lifetime.** `seed_disconnect_wait_period` is exposed and
   documented, default 5 minutes.
5. **p2p configuration surface.** Parameters are actually wired through to the
   `Switch`, not merely logged. `allow_duplicate_ip` is configurable instead of
   hardcoded.
6. **Startup.** A seed with a populated address book starts without any seed
   configured. Upstream panics.
7. **Observability.** Prometheus metrics on a dedicated address, off by default.
   Configurable moniker.
8. **Code structure.** An explicit seed instance type replaces a 163-line
   monolith, so the construction order (channels, transport, reactors,
   `SetNodeInfo`) is explicit and testable.
9. **Tests.** Unit tests, including one that loads a real partial `config.toml`.
   Upstream has none, although its Makefile declares a `test` target.
10. **Continuous integration.** Build, `vet` and tests run on every push.
    Upstream only pushed a container image, with no verification of any kind.
11. **Publication.** Semantic tags and releases with binaries.
12. **Documentation.** An accurate README. Upstream still advertises limits its
    own code does not apply, and describes itself as a fork of a different
    project.
13. **Self-reference fix.** The seed registers its own address in its book, and
    the verification path skips any address carrying its own node ID, so
    verification never dials the seed itself.

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
```

Replace `cosmoshub-4` with the chain you serve, and pick a free port if 26656 is
already taken on your machine.

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
sudo -u tenderseed tenderseed -home /home/tenderseed/.tenderseed/${CHAIN_ID} show-node-id
```

That command does two things: it creates `config/config.toml` and
`config/node_key.json` if they do not exist, and it prints your node identity.

The identity looks like `0123456789abcdef0123456789abcdef01234567`. Other
operators need it to reach your seed, in the form
`<node-id>@<your-host>:<port>`.

> **Keep `config/node_key.json` safe.** It *is* your seed's identity. If you
> delete or regenerate it, your seed address changes for everyone who
> referenced it. Back it up.

### Step 5 - Configure

The first run wrote a complete `config.toml` containing every option, its
default value, and a comment describing it. Open it:

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

Get seed addresses for your chain from its Chain Registry entry, or from
[ABS](https://aviaone.com/blockchains-service/).

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
sudo -u tenderseed python3 -c "import json;print(len(json.load(open('/home/tenderseed/.tenderseed/${CHAIN_ID}/data/addrbook.json'))['addrs']))"
```

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

---

## Installation with Docker

### Requirements

- Docker installed.
- One open TCP port per chain served.

### Step 1 - Build the image

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
mkdir -p ~/tenderseed-data/${CHAIN_ID}
docker run --rm --user "$(id -u):$(id -g)" \
  -v ~/tenderseed-data/${CHAIN_ID}:/data tenderseed:latest show-node-id
```

This writes `config/config.toml` and `config/node_key.json` into
`~/tenderseed-data/${CHAIN_ID}` and prints your node identity.

### Step 3 - Configure

```bash
nano ~/tenderseed-data/${CHAIN_ID}/config/config.toml
```

Set `chain_id` and `seeds` as described in the Linux section.

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

---

## Configuration

Every key has a working default, and decoding starts from those defaults, so a
partial `config.toml` remains valid: any key you delete keeps its default value.

### Keys inherited from upstream

| key | default | what it does |
|---|---|---|
| `laddr` | `tcp://0.0.0.0:26656` | address and port to listen on for incoming connections |
| `chain_id` | empty | network identifier of the chain this seed serves |
| `seeds` | empty | comma-separated `<node-id>@<host>:<port>` list used to bootstrap discovery. May be emptied once the address book is populated |
| `log_level` | `info` | `info`, `debug`, `error` or `none` |
| `node_key_file` | `config/node_key.json` | path to the node identity, relative to the home directory or absolute |
| `addr_book_file` | `data/addrbook.json` | path to the address book, relative to the home directory or absolute |
| `addr_book_strict` | `true` | strict routability rules. Set `false` for private or local networks, otherwise non-routable addresses are rejected |
| `max_num_inbound_peers` | `100` | how many nodes may be connected to your seed at once |
| `max_num_outbound_peers` | `60` | how many peers the seed dials while crawling |
| `max_packet_msg_payload_size` | `1024` | maximum message packet payload, in bytes |

### Keys added by this fork

| key | default | what it does |
|---|---|---|
| `seed_disconnect_wait_period` | `5m` | how long a crawled peer stays connected before the PEX reactor disconnects it. Upstream leaves this at zero, which drops peers on the first crawl round, often before they have answered. Too short and the book stays empty; too long and outbound connections pile up |
| `peer_check_period` | `10m` | how often the addresses the seed would serve are re-verified. Shorter means a fresher book at the cost of more outbound traffic. `0` disables verification entirely, which restores upstream behaviour |
| `peer_check_workers` | `8` | how many verification dials run in parallel. A sweep of 250 addresses takes about 17 minutes sequentially and about 2 minutes with 8 workers. Lower it on a constrained machine |
| `allow_duplicate_ip` | `true` | allow several peers behind a single IP address. Setting it to `false` also changes the meaning of "already connected", so it interacts with verification |
| `metrics_listen_addr` | empty | address to serve Prometheus metrics on, for example `127.0.0.1:26660`. Empty disables the endpoint |
| `metrics_namespace` | `cometbft` | prefix of every exported series. Matches the upstream default, so dashboards written for a full node work unchanged |
| `moniker` | empty | name announced to peers. Empty means `<chain_id>-seed` |

### Flags and environment variables

Command-line flags override the file, and environment variables override the
file but not the flags.

```
-home       home directory (default ~/.tenderseed)
-config     path to config.toml, relative to home or absolute
-chain-id   overrides chain_id
-seeds      overrides seeds
```

```
TENDERSEED_CHAIN_ID
TENDERSEED_SEEDS
```

Subcommands: `start` and `show-node-id`.

### Version announced to peers

The seed announces its software version during the p2p handshake. A binary
built from this repository announces `2.0.0`. Override it at build time if you
package your own build:

```bash
go build -ldflags "-X github.com/AviaOne/tenderseed/internal/tenderseed.Version=2.0.1-mybuild" ./cmd/tenderseed
```

### Metrics

Set `metrics_listen_addr`, restart, and scrape it:

```bash
curl -s 127.0.0.1:26660/metrics | grep '^cometbft_p2p'
```

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
