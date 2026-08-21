#!/usr/bin/env bash
# One-shot bench: start a seed, let it run, report what it actually did.
#
# Usage: scripts/bench.sh <home> <seconds> [binary]
#
# Reads nothing but the home directory given, writes nothing outside it and
# /tmp. Meant to run beside a production seed, on a different port.
set -euo pipefail

home=${1:?usage: bench.sh <home> <seconds> [binary]}
seconds=${2:?usage: bench.sh <home> <seconds> [binary]}
binary=${3:-./build/tenderseed}
log=$(mktemp)

book="${home}/data/addrbook.json"
count() { grep -o -- "$1" "${book}" 2>/dev/null | wc -l || true; }
before_addrs=$(count '"addr"')
before_good=$(count '"bucket_type": *2')

echo "bench: ${binary} -home ${home} for ${seconds}s"
timeout -s TERM "${seconds}" "${binary}" -home "${home}" start > "${log}" 2>&1 || true

addrs=$(count '"addr"')
good=$(count '"bucket_type": *2')

echo
echo "log:            ${log}"
echo "addresses:      ${addrs} (+$((addrs - before_addrs)) this run)"
echo "verified good:  ${good} (+$((good - before_good)) this run)"
echo "peers dialled:  $(grep -c 'Starting Peer service' "${log}" || true)"
echo "peers dropped:  $(grep -c 'Stopping Peer service' "${log}" || true)"
echo "failed checks:  $(grep -c 'address failed verification' "${log}" || true)"
echo "panics:         $(grep -c 'panic' "${log}" || true)"
echo "errors:         $(grep -cE '^E\[' "${log}" || true)"
