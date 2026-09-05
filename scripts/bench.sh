#!/usr/bin/env bash
# One-shot bench: start a seed, let it run, report what it actually did.
#
# Usage: scripts/bench.sh <home> <seconds> [binary]
#
# Reads nothing but the home directory given, writes nothing outside it and
# /tmp. Meant to run beside a production seed, on a different port.
#
# Both stacks, and not by accident. The first version read the Cosmos book
# format, the Cosmos log format and the Cosmos sweep fields, so on a TM2 home
# it reported zeros and said nothing about it. Zeros that mean "not measured"
# are worse than no line at all.
set -euo pipefail

home=${1:?usage: bench.sh <home> <seconds> [binary]}
seconds=${2:?usage: bench.sh <home> <seconds> [binary]}
binary=${3:-./build/tenderseed}
log=$(mktemp)

book="${home}/data/addrbook.json"
config="${home}/config/config.toml"

# The stack decides every pattern below. An absent key means cosmos, which is
# what the binary itself does.
stack=cosmos
if [ -f "${config}" ]; then
  declared=$(sed -n 's/^[[:space:]]*stack[[:space:]]*=[[:space:]]*"\{0,1\}\([a-z0-9]*\).*/\1/p' "${config}" | head -1)
  if [ -n "${declared}" ]; then
    stack=${declared}
  fi
fi

count() { grep -o -- "$1" "${book}" 2>/dev/null | wc -l || true; }

# A verified address is one the seed reached itself. Cosmos records that as a
# bucket type, this seed's own book as a non zero last success.
if [ "${stack}" = "tm2" ]; then
  good_pattern='"last_ok": *[1-9]'
  error_pattern='level=ERROR'
else
  good_pattern='"bucket_type": *2'
  error_pattern='^E\['
fi

before_addrs=$(count '"addr"')
before_good=$(count "${good_pattern}")

echo "bench: ${binary} -home ${home} for ${seconds}s, stack ${stack}"
timeout -s TERM "${seconds}" "${binary}" -home "${home}" start > "${log}" 2>&1 || true

addrs=$(count '"addr"')
good=$(count "${good_pattern}")

echo
echo "log:            ${log}"
echo "stack:          ${stack}"
echo "addresses:      ${addrs} (+$((addrs - before_addrs)) this run)"
echo "verified good:  ${good} (+$((good - before_good)) this run)"

# Verification figures come from the "verification sweep" line, which both
# stacks emit at info level once per peer_check_period. A run shorter than one
# period produces no such line and these totals stay at zero. Reading the per
# address debug lines instead would require log_level = "debug", which changes
# what is being measured.
sum_field() { grep -o -- "$1=[0-9]*" "${log}" | cut -d= -f2 | awk '{s+=$1} END {print s+0}'; }

echo "sweeps:         $(grep -c 'verification sweep' "${log}" || true)"

if [ "${stack}" = "tm2" ]; then
  echo "addresses tried:  $(sum_field tried)"
  echo "already connected:$(sum_field connected)"
  echo "over budget:      $(sum_field over_budget)"
  echo "dropped:          $(sum_field dropped)"
  echo "answers served:   $(grep -c 'served addresses' "${log}" || true)"
  echo "answers refused:  $(grep -c 'no verified address to serve' "${log}" || true)"
else
  echo "peers dialled:  $(grep -c 'Starting Peer service' "${log}" || true)"
  echo "peers dropped:  $(grep -c 'Stopping Peer service' "${log}" || true)"
  echo "checks passed:  $(sum_field verify_success)"
  echo "failed checks:  $(sum_field verify_failure)"
  # Only the backoff skips of the verify stage. The enqueue stage skips are a
  # different decision and are not summed in here.
  echo "checks skipped: $(sum_field verify_skipped_backoff)"
fi

echo "panics:         $(grep -c 'panic' "${log}" || true)"
echo "errors:         $(grep -cE "${error_pattern}" "${log}" || true)"
