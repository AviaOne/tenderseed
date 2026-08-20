# tenderseed

A lightweight seed node for CometBFT p2p networks.

A seed node keeps an address book of live peers and hands out addresses to
nodes that ask. It does not relay or store blocks or transactions, so it costs
almost nothing to run.

## Lineage

This is a fork of [binaryholdings/tenderseed](https://github.com/binaryholdings/tenderseed)
at commit `10a64d5`, itself a fork of the original polychainlabs project. The
upstream has been unmaintained since February 2023 and still targets
Tendermint v0.34, which reached end of life.

## What this fork changes

- **CometBFT v0.40.x** instead of Tendermint v0.34.22.
- **Served addresses are verified.** Upstream never marks any address as good,
  so a seed hands out whatever it was told, alive or not. This fork dials
  addresses before serving them and re-checks its selection periodically.
- **`seed_disconnect_wait_period` is configurable.** Upstream leaves it at zero
  in a third-party binary, which drops crawled peers on the first crawl round.
- **Starts from an existing address book**, with no seeds configured.
- **Prometheus metrics**, off by default.
- **No panics**: errors are reported and the process exits with a status code.
- **Tests**, where the original has none.

## Quickstart

```bash
make build
./build/tenderseed -home ~/.tenderseed/cosmoshub-4 -chain-id cosmoshub-4 \
  -seeds "<id>@<host>:<port>" start
```

The first run writes a config.toml holding every option and its default.
Edit it and restart. Flags and the TENDERSEED_CHAIN_ID and TENDERSEED_SEEDS
environment variables override the file.

Show the node identity, which is what other operators need from you:

```bash
./build/tenderseed -home ~/.tenderseed/cosmoshub-4 show-node-id
```

One process serves one chain. Run several homes on several ports to serve
several chains.

## Configuration worth knowing about

Every key has a working default, so a partial config.toml is valid.

| key | default | what it does |
|---|---|---|
| seed_disconnect_wait_period | 5m | how long a crawled peer stays connected. Upstream leaves this at zero, which drops peers on the first crawl round |
| peer_check_period | 10m | how often served addresses are re-verified. 0 disables verification |
| peer_check_workers | 8 | verification dials in parallel |
| allow_duplicate_ip | true | several peers behind one IP |
| metrics_listen_addr | empty | Prometheus endpoint, empty disables it |
| metrics_namespace | cometbft | prefix of exported series |
| moniker | empty | name announced to peers, empty means chain-id-seed |

## About AviaOne

[AviaOne](https://aviaone.com) has been a professional Cosmos validator for
over three years, and builds tooling for the ecosystem rather than for itself
alone.

Its main contribution is **[ABS, AviaOne BlockChains Service](https://aviaone.com/blockchains-service/)**, a live
directory covering more than 350 blockchains. ABS turns raw Chain Registry
data into something operators can actually rely on: RPC endpoints tested every
three hours and ranked by latency, block explorers rechecked daily so dead
links disappear, the binary version genuinely running on each network compared
against what the registry claims, and live network status.

This fork comes from the same place. Seed nodes are shared infrastructure:
every chain relies on them to bootstrap, yet the tooling behind them has been
unmaintained for years. Fixing that helps everyone running a Cosmos network,
not just us.

## License

Blue Oak Model License 1.0.0, unchanged from upstream. See LICENSE.md.

## Credits

binaryholdings and polychainlabs for the original tenderseed. The address
verification design follows voluzi/cosmoseed, which solved the same problem
on the CometBFT v2 API.
