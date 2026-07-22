#!/bin/sh
set -eu

if [ "$(uname -s)" != "Linux" ]; then
	printf '%s\n' "sergeant-dashboard systemd uninstallation supports Linux only" >&2
	exit 1
fi

bin_dir=${HOME}/.local/bin
unit_dir=${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user

disable_status=0
if ! systemctl --user disable --now sergeant-dashboard.service 2>/dev/null; then
	disable_status=1
fi
if systemctl --user is-active --quiet sergeant-dashboard.service; then
	if [ "$disable_status" -eq 0 ]; then
		printf '%s\n' "sergeant-dashboard is still active; installation preserved" >&2
	else
		printf '%s\n' "Could not stop sergeant-dashboard; installation preserved" >&2
	fi
	exit 1
else
	active_status=$?
	if [ "$active_status" -ne 3 ] && [ "$active_status" -ne 4 ]; then
		printf '%s\n' "Could not verify sergeant-dashboard is inactive; installation preserved" >&2
		exit 1
	fi
fi
rm -f "$unit_dir/sergeant-dashboard.service" "$bin_dir/sergeant-dashboard"
systemctl --user daemon-reload

printf '%s\n' "Uninstalled sergeant-dashboard"
