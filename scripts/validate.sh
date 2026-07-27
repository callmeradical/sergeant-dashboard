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
api_state_result='API state check skipped (jq unavailable or API unreachable)'
if command -v jq >/dev/null 2>&1; then
  state=$(curl --fail --silent http://127.0.0.1:8992/sergeant/api/state 2>/dev/null) || state=''
  if [ -n "$state" ]; then
    if ! printf '%s' "$state" | jq -e '.workers | type == "array"' >/dev/null 2>&1; then
      printf 'API state check FAILED: .workers must be an array\n' >&2
      exit 1
    fi

    total=$(printf '%s' "$state" | jq '.workers | length')
    active=$(printf '%s' "$state" | jq '[.workers[] | select(.health=="active")] | length')
    invalid=$(printf '%s' "$state" | jq '[.workers[] | select(.health != "active" and .health != "orphaned")] | length')

    if [ "$invalid" -gt 0 ]; then
      printf 'API state check FAILED: %s non-operational worker(s) leaked into response\n' "$invalid" >&2
      exit 1
    fi

    api_state_result='API state check passed'
  fi
fi

printf '%s\n' "Local health, local UI, Tailscale Serve, and tailnet HTTPS checks passed"
printf '%s\n' "$api_state_result"
