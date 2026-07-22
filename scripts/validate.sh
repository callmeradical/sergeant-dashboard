#!/bin/sh
set -eu

tailnet_url=${SERGEANT_TAILNET_URL:-https://cleanthes.taila4fb6a.ts.net/sergeant/}

curl --fail --silent --show-error http://127.0.0.1:8992/healthz >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8992/sergeant/ >/dev/null
serve_status=$(tailscale serve status --json)
printf '%s\n' "$serve_status" | grep -F '127.0.0.1:8992/sergeant' >/dev/null
printf '%s\n' "$serve_status" | grep -F '/sergeant' >/dev/null
curl --fail --silent --show-error "$tailnet_url" >/dev/null

printf '%s\n' "Local health, local UI, Tailscale Serve, and tailnet HTTPS checks passed"
