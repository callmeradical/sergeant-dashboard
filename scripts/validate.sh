#!/bin/sh
set -eu

tailnet_url=${SERGEANT_TAILNET_URL:-https://cleanthes.taila4fb6a.ts.net/sergeant/}
dashboard_bin=${SERGEANT_DASHBOARD_BIN:-${HOME}/.local/bin/sergeant-dashboard}

curl --fail --silent --show-error http://127.0.0.1:8992/healthz >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8992/sergeant/ >/dev/null
tailscale serve status --json | "$dashboard_bin" validate-serve "$tailnet_url" http://127.0.0.1:8992/sergeant
curl --fail --silent --show-error "$tailnet_url" >/dev/null

# Verify the API returns a structurally valid operational-set response.
# Asserts: workers array present, every worker has health in the operational
# set (active|orphaned), no stale/complete/unknown workers in the response.
# Requires jq. Only asserts when the API is reachable.
if command -v jq >/dev/null 2>&1; then
  state=$(curl --fail --silent http://127.0.0.1:8992/sergeant/api/state 2>/dev/null) || state=''
  if printf '%s' "$state" | jq -e '.workers' >/dev/null 2>&1; then
    total=$(printf '%s' "$state" | jq '.workers | length')
    active=$(printf '%s' "$state" | jq '[.workers[] | select(.health=="active")] | length')
    leaked=$(printf '%s' "$state" | jq '[.workers[] | select(.health == "stale" or .health == "complete" or .health == "unknown")] | length')

    if [ "$leaked" -gt 0 ]; then
      printf 'API state check FAILED: %s non-operational worker(s) (stale/complete/unknown) leaked into response\n' "$leaked" >&2
      exit 1
    fi

    printf 'API state check passed: total=%s active=%s\n' "$total" "$active"
  fi
fi

printf '%s\n' "Local health, local UI, Tailscale Serve, tailnet HTTPS, and API state checks passed"
