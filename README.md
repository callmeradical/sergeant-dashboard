# sergeant-dashboard

Read-only operational dashboard for Sergeant fleets, delivery gates, and project activity. The Go binary embeds its responsive frontend and projects source state without changing Sergeant, td, GitHub, no-mistakes, Graphify, oc-inject, repositories, or worker processes.

The dashboard is a trusted single-operator projection for local and tailnet use. Compact cards and search use configured project/repository identity plus bounded task status summaries. Selecting a card fetches branches, worktrees, operational messages, diagnostics, logs, handoffs, td context, GitHub pull requests/checks/comments, no-mistakes output, Graphify reports, and oc-inject transport metadata on demand.

All projected text passes through one bounded, deterministic redaction boundary before HTTP serialization. Credential-bearing URLs, authorization values, tokens, passwords, secret assignments, sensitive environment values, control bytes, and repository secret files are removed or left unread. Prompt, injected response, and oc-inject transport bodies are never collected. Missing, unreadable, corrupt, oversized, or timed-out sources are reported without substituting stale fleet state. The dashboard remains strictly read-only and exposes no mutation controls.

## Build and run

Requires Go 1.24 or newer.

```sh
go build ./cmd/sergeant-dashboard
go run ./cmd/sergeant-dashboard
```

The server always binds to `127.0.0.1:8992`. Open <http://127.0.0.1:8992/sergeant/>. The health endpoint is <http://127.0.0.1:8992/healthz> and projected JSON is available at <http://127.0.0.1:8992/sergeant/api/state>. That API returns the operational worker set only: supervisor-verified live workers as `active` plus actionable non-terminal records as `orphaned`. Stale, complete, and malformed historical records stay out of the overview and are summarized in warnings instead. The UI obtains one redacted worker detail at `/sergeant/api/workers/{task}/{repository}` only when its drawer opens.

The fleet root defaults to `$XDG_DATA_HOME/sergeant/fleet`, or `$HOME/.local/share/sergeant/fleet` when `XDG_DATA_HOME` is unset. The project registry defaults to `$HOME/.config/sergeant` and follows `SERGEANT_CONFIG`. `SERGEANT_FLEET_ROOT` can select a different read-only source, and `SERGEANT_STALE_AFTER` controls the default `30m` stale threshold.

## Linux user service

The systemd lifecycle scripts support Linux only.

Install and start the restart-on-failure systemd user service:

```sh
./scripts/install.sh
systemctl --user status sergeant-dashboard.service
```

Remove it with `./scripts/uninstall.sh`. The service unit uses a read-only home and system filesystem, private temporary storage, and no new privileges.

## macOS operation

macOS does not include launchd integration. Build and run the binary directly from a terminal:

```sh
go build -o sergeant-dashboard ./cmd/sergeant-dashboard
./sergeant-dashboard
```

Stop it with Control-C. Remove the local `sergeant-dashboard` binary to uninstall it. The Linux `scripts/install.sh` and `scripts/uninstall.sh` lifecycle scripts are not supported on macOS.

## Tailnet Access

The dashboard remains bound to loopback at `127.0.0.1:8992`. The existing `sergeant-dashboard-tailnet-proxy.service` user service exposes it tailnet-only at <http://cleanthes:8992/> without changing the Dashboard listener. Validation resolves that hostname through `tailscale ip -4` so a local hosts-file entry cannot bypass the tailnet proxy.

Validate both user services, local health/UI, and the exact tailnet route after installation:

```sh
./scripts/validate.sh
```

When `jq` is available and the API is reachable, the validator also checks that `/sergeant/api/state` returns a structurally valid operational-set response and leaks no `stale`, `complete`, or `unknown` workers. Set `SERGEANT_TAILNET_URL` to validate a different tailnet hostname. It must be a canonical HTTPS URL on port 443 with exactly the `/sergeant/` path and no userinfo, query, or fragment; the validator derives the Serve host and path from this one URL.


## Rollback

Stop and remove the Linux Dashboard user service:

```sh
./scripts/uninstall.sh
```

Verify rollback with `systemctl --user status sergeant-dashboard.service`. The separately managed `sergeant-dashboard-tailnet-proxy.service` may remain installed, but its upstream will be unavailable while Dashboard is stopped.
If the service cannot be stopped, uninstall exits without removing the unit or binary so the installation can be recovered and retried.

On macOS, stop the foreground process with Control-C and remove the binary.

## Development

The offline frontend behavior test requires Node.js 22 or newer and Chrome or Chromium; the application binary has no browser or Node.js runtime dependency.

```sh
go test ./...
go test -race ./...
go vet ./...
```

### Graphite Flight Deck

The embedded UI uses a dark-only inspection language optimized for calm density and signal over decoration.

- Tokens: canvas `#0B0E11`, panel `#11161B`, raised `#171D23`, border `#29323A`, text `#E7ECEF`, and muted `#8C98A3`. Semantic color is limited to active, done, needs-input, blocked, failed, orphaned, stale, and focus signals.
- Card anatomy: a 3px semantic rail; configured project/repository identity; lifecycle badge; two-line 16px title; one agent/updated/PR row; and at most one highest-priority attention line. Cards are uniformly 208px at the default text size, may grow for text zoom, and contain no drawer-only detail.
- Drawer anatomy: a 560px desktop drawer and full-screen mobile sheet with sticky identity header and read-only footer. One scrolling surface contains Overview, Attention, Timeline, Delivery, and Files sections; paths use bounded monospace blocks.
- Typography and spacing: system sans for titles/prose, monospace for identifiers, paths, branches, timestamps, and metrics; a 4px spacing grid; 6px corners; restrained shadows; and no gradients or decorative motion.
- Interaction: cards support click, Enter, and Space. The modal drawer traps focus, makes the page inert, closes by Escape/backdrop/button, restores card focus, clears fetched detail, and honors reduced-motion preferences.

This project is licensed under the MIT License.
