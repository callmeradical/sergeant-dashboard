#!/bin/sh
set -eu

bin_dir=${HOME}/.local/bin
unit_dir=${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user

systemctl --user disable --now sergeant-dashboard.service 2>/dev/null || true
rm -f "$unit_dir/sergeant-dashboard.service" "$bin_dir/sergeant-dashboard"
systemctl --user daemon-reload

printf '%s\n' "Uninstalled sergeant-dashboard"
