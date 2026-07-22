# sergeant-dashboard

Read-only operational dashboard for Sergeant fleets, delivery gates, and project activity. The Go binary embeds its responsive frontend and projects source state without changing Sergeant, td, GitHub, no-mistakes, Graphify, oc-inject, repositories, or worker processes.

The collector deliberately reads only whitelisted scalar metadata. The HTTP boundary emits opaque task/project aliases, validated td IDs and PR links, enumerated lifecycle/tool states, and body-free file metadata. Worker-message presence and timestamps are visible, but source names, branches, worktree paths, message contents, prompt bodies, injected message bodies, logs, command output, tokens, environment values, secrets, and credentials are neither rendered nor persisted.

## Build and run

Requires Go 1.24 or newer.

```sh
go build ./cmd/sergeant-dashboard
go run ./cmd/sergeant-dashboard
```

The server always binds to `127.0.0.1:8992`. Open <http://127.0.0.1:8992/sergeant/>. The health endpoint is <http://127.0.0.1:8992/healthz> and projected JSON is available at <http://127.0.0.1:8992/sergeant/api/state>.

The fleet root defaults to `$XDG_DATA_HOME/sergeant/fleet`, or `$HOME/.local/share/sergeant/fleet` when `XDG_DATA_HOME` is unset. `SERGEANT_FLEET_ROOT` can select a different read-only source, and `SERGEANT_STALE_AFTER` controls the default `30m` stale threshold.

## User service

Install and start the restart-on-failure systemd user service:

```sh
./scripts/install.sh
systemctl --user status sergeant-dashboard.service
```

Remove it with `./scripts/uninstall.sh`. The service unit uses a read-only home and system filesystem, private temporary storage, and no new privileges.

## Tailscale Serve

Publish only the dashboard subpath through the existing tailnet:

```sh
tailscale serve --bg --set-path /sergeant http://127.0.0.1:8992/sergeant
tailscale serve status
```

The expected endpoint is <https://cleanthes.taila4fb6a.ts.net/sergeant/>. This does not make the loopback listener public; Tailscale remains the HTTPS access boundary.

Validate local health, the local UI, Serve configuration, and tailnet HTTPS after installation:

```sh
./scripts/validate.sh
```

Set `SERGEANT_TAILNET_URL` to validate a different tailnet hostname. It must be a canonical HTTPS URL on port 443 with exactly the `/sergeant/` path and no userinfo, query, or fragment; the validator derives the Serve host and path from this one URL.

## Rollback

Remove only the dashboard Serve route, then stop and remove the user service:

```sh
tailscale serve --https=443 --set-path=/sergeant off
./scripts/uninstall.sh
```

Verify rollback with `tailscale serve status` and `systemctl --user status sergeant-dashboard.service`. Do not use `tailscale serve reset`, because it also removes unrelated routes on the node.
If the service cannot be stopped, uninstall exits without removing the unit or binary so the installation can be recovered and retried.

## Development

The offline frontend behavior test requires Node.js 22 or newer and Chrome or Chromium; the application binary has no browser or Node.js runtime dependency.

```sh
go test ./...
go test -race ./...
go vet ./...
```

This project is licensed under the MIT License.
