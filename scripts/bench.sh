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
# Verification figures come from the "verification sweep" line, which is emitted
# at info level once per peer_check_period. A run shorter than one period
# produces no such line and these totals stay at zero. Reading the per-address
# debug lines instead would require log_level = "debug", which changes what is
# being measured.
sum_field() { grep -o -- "$1=[0-9]*" "${log}" | cut -d= -f2 | awk '{s+=$1} END {print s+0}'; }
echo "sweeps:         $(grep -c 'verification sweep' "${log}" || true)"
echo "checks passed:  $(sum_field verify_success)"
echo "failed checks:  $(sum_field verify_failure)"
echo "checks skipped: $(sum_field verify_skipped_backoff)"
echo "panics:         $(grep -c 'panic' "${log}" || true)"
echo "errors:         $(grep -cE '^E\[' "${log}" || true)"
