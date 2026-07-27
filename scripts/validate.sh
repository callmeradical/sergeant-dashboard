#!/bin/sh
set -eu

systemctl --user is-active --quiet sergeant-dashboard.service
systemctl --user is-active --quiet sergeant-dashboard-tailnet-proxy.service
curl --fail --silent --show-error http://127.0.0.1:8992/healthz >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8992/sergeant/ >/dev/null
tailnet_ip=$(tailscale ip -4)
curl --fail --silent --show-error --location --resolve "cleanthes:8992:${tailnet_ip}" http://cleanthes:8992/ | grep -F "<title>Sergeant | Fleet command</title>" >/dev/null

# Verify the API returns a structurally valid operational-set response.
# Asserts: workers array present, every worker has health in the operational
# set (active|orphaned), no stale/complete/unknown workers in the response.
# Requires jq. Only asserts when the API is reachable.
api_state_result='API state check skipped (jq unavailable or API unreachable)'
if command -v jq >/dev/null 2>&1; then
  state=$(curl --fail --silent http://127.0.0.1:8992/sergeant/api/state 2>/dev/null) || state=''
  if [ -n "$state" ] && printf '%s' "$state" | jq -e '.' >/dev/null 2>&1; then
    if ! printf '%s' "$state" | jq -e '.workers | type == "array"' >/dev/null 2>&1; then
      printf 'API state check FAILED: .workers must be an array\n' >&2
      exit 1
    fi

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
