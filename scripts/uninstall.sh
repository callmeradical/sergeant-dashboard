#!/bin/sh
set -eu

bin_dir=${HOME}/.local/bin
unit_dir=${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user

if ! systemctl --user disable --now sergeant-dashboard.service 2>/dev/null; then
	if systemctl --user is-active --quiet sergeant-dashboard.service; then
		printf '%s\n' "Could not stop sergeant-dashboard; installation preserved" >&2
		exit 1
	fi
fi
if systemctl --user is-active --quiet sergeant-dashboard.service; then
	printf '%s\n' "sergeant-dashboard is still active; installation preserved" >&2
	exit 1
fi
rm -f "$unit_dir/sergeant-dashboard.service" "$bin_dir/sergeant-dashboard"
systemctl --user daemon-reload

printf '%s\n' "Uninstalled sergeant-dashboard"
