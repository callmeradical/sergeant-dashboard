#!/bin/sh
set -eu

systemctl --user is-active --quiet sergeant-dashboard.service
systemctl --user is-active --quiet sergeant-dashboard-tailnet-proxy.service
curl --fail --silent --show-error http://127.0.0.1:8992/healthz >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8992/sergeant/ >/dev/null
tailnet_ip=$(tailscale ip -4)
curl --fail --silent --show-error --location --resolve "cleanthes:8992:${tailnet_ip}" http://cleanthes:8992/ | grep -F "<title>Sergeant | Fleet command</title>" >/dev/null

printf '%s\n' "Dashboard, user-service proxy, local UI, and tailnet route checks passed"
