#!/bin/sh
set -eu

repo_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
bin_dir=${HOME}/.local/bin
unit_dir=${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user

mkdir -p "$bin_dir" "$unit_dir"
go build -trimpath -o "$bin_dir/sergeant-dashboard" "$repo_dir/cmd/sergeant-dashboard"
install -m 0644 "$repo_dir/deploy/sergeant-dashboard.service" "$unit_dir/sergeant-dashboard.service"
systemctl --user daemon-reload
systemctl --user enable --now sergeant-dashboard.service

printf '%s\n' "Installed sergeant-dashboard at http://127.0.0.1:8992/sergeant/"
