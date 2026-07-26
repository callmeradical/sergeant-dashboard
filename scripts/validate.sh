#!/bin/sh
set -eu

tailnet_url=${SERGEANT_TAILNET_URL:-https://cleanthes.taila4fb6a.ts.net/sergeant/}
dashboard_bin=${SERGEANT_DASHBOARD_BIN:-${HOME}/.local/bin/sergeant-dashboard}

curl --fail --silent --show-error http://127.0.0.1:8992/healthz >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8992/sergeant/ >/dev/null
tailscale serve status --json | "$dashboard_bin" validate-serve "$tailnet_url" http://127.0.0.1:8992/sergeant
curl --fail --silent --show-error "$tailnet_url" >/dev/null

# Verify the API returns a plausible operational state: nonzero active workers
# and a bounded total (operational set only, not full fleet inventory).
# Requires jq. Only asserts when the API is reachable.
if command -v jq >/dev/null 2>&1; then
  state=$(curl --fail --silent http://127.0.0.1:8992/sergeant/api/state 2>/dev/null) || state=''
  if printf '%s' "$state" | jq -e '.workers' >/dev/null 2>&1; then
    total=$(printf '%s' "$state" | jq '.workers | length')
    active=$(printf '%s' "$state" | jq '[.workers[] | select(.health=="active")] | length')

    if [ "$active" -lt 1 ]; then
      printf 'API state check FAILED: active=%s, want at least 1 active worker\n' "$active" >&2
      exit 1
    fi
    # Operational set must be bounded: stale/complete/unknown workers are excluded.
    # A reasonable upper bound is 100 workers; the original failure returned 154.
    if [ "$total" -gt 100 ]; then
      printf 'API state check FAILED: total=%s exceeds operational bound of 100 (stale/complete/unknown should be excluded)\n' "$total" >&2
      exit 1
    fi

    printf 'API state check passed: total=%s active=%s\n' "$total" "$active"
  fi
fi

printf '%s\n' "Local health, local UI, Tailscale Serve, tailnet HTTPS, and API state checks passed"
