# Servitor

Servitor is a Slack Socket Mode front end for ICT. It provisions at most one temporary IBM Cloud cluster for each Slack user. It is fail-closed: only the configured channel can start or stop a lifecycle, and only the lifecycle owner can act on that owner's ICT workspace.

## Install the Slack app

Enable Socket Mode. The minimum scopes and event subscriptions are:

- App-level: `connections:write`.
- Bot: `chat:write`, `im:history`, and `channels:history` for a public configured channel and/or `groups:history` for a private configured channel.
- Events: `message.im` plus `message.channels` for public-channel support and/or `message.groups` for private-channel support.

Invite the bot only to the configured channel. Do not add `app_mentions:read`, reaction scopes, profile scopes such as `users:read`, conversation-membership scopes, `im:read`, `im:write`, or other IM scopes. Servitor does not use them.

Keep tokens out of YAML and source control:

```sh
export SLACK_BOT_TOKEN='...'
export SLACK_APP_TOKEN='...'
```

## Configure and run

Install [ICT](https://github.com/bevicted/ict) from its public release path:

```sh
go install github.com/bevicted/ict@latest
```

Copy `config.example.yaml` to a private operator location. The template is intentionally incomplete: fill every empty field, including Slack IDs, ICT paths, runtime paths, and all defaults, before startup. Configuration lookup order is:

1. `-config PATH`
2. `SERVITOR_CONFIG`
3. `$XDG_CONFIG_HOME/servitor/config.yaml` (or the platform user config directory, normally `~/.config/servitor/config.yaml`)

Secrets are environment-only. The selected non-secret configuration path is recorded in the private startup log. Restrict the configuration file and keep `paths.state`, `paths.logs`, ICT workspaces, Terraform plans, state, and logs outside the repository.

```sh
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/servitor"
cp config.example.yaml "${XDG_CONFIG_HOME:-$HOME/.config}/servitor/config.yaml"
chmod 600 "${XDG_CONFIG_HOME:-$HOME/.config}/servitor/config.yaml"
SLACK_BOT_TOKEN='...' SLACK_APP_TOKEN='...' go run ./cmd/servitor
```

`defaults.version` makes bare `create` valid. `lifecycle.lease` is the initial lease and must be a whole number of hours from `1h` through `24h`. `extend` adds from the current expiry, but remaining time is always capped at 24 hours; total cluster lifetime is not capped. IBM VPC cluster creation is allowed up to 90 minutes.

Servitor passes `--prefix servitor` to ICT. Generated cloud resource names therefore begin `servitor-`; the ICT workspace remains the caller's Slack ID.

## Commands and routing

DM commands are bare. Configured-channel root commands require exactly one leading authenticated bot mention, for example `@servitor create`. Ordinary channel conversation, a mention in the middle of text, a wrong bot mention, and unmentioned root text are ignored. `help` and `list` are available in both DMs and the configured channel.

```text
DM
  help [command]          print help
  list                    list clusters

Configured channel
  @servitor help [command]  print help
  @servitor create [flags]  provision a new cluster
  @servitor done            release your resources
  @servitor extend [N[h]]   extend your lease
  @servitor list            list clusters

Lifecycle thread
  yes                       approve the cluster plan
  no                        reject the cluster plan
  done                      release your resources
  extend [N[h]]             extend your lease
```

`destroy` is an accepted, silent alias for `done`; it is intentionally not shown in help. Root `help` and unknown commands produce useful command help. `help create`, `help extend`, `help done`, and `help list` provide command-specific syntax. The configured maintainer's DM help additionally shows `status`, `pause`, `unpause`, and `stop`; other users are not told about those commands.

Root commands promptly report one of `Command accepted.\nPlanning...`, `Command rejected.`, or `Command unknown.`. A create acceptance must be delivered before Servitor starts ICT. Creation planning has no periodic heartbeat.

### Create, review, and readiness

Use bare create for configured defaults, or supply safe typed flags:

```text
@servitor create
@servitor create --version 1.36
@servitor create --version 4.22 --provider classic --datacenter dal10
```

The safe flags are:

```text
Common
  --target --provider --platform --version --resource-group --name --worker-count

VPC Gen 2
  --zone --flavor --vpc-id --subnet-id --public-gateway-id

Classic
  --datacenter --machine-type --public-vlan-id --private-vlan-id

Satellite
  --satellite-zone --satellite-managed-from --satellite-location-id
  --satellite-host-image --satellite-host-profile --satellite-ssh-key-id
  --satellite-worker-instance-id --satellite-worker-operating-system
```

Servitor immediately sends planning feedback, then posts a sanitized, tabular review with normalized target, platform/version, provider, location, resource group, worker and network choices, plus resource action counts. Only the initiating user may approve by replying with the exact, case-sensitive raw text `yes` in that initiating lifecycle thread within five minutes. Exact `no` declines. Unknown replies, replies in other threads, other users, and root confirmations are ignored.

After durable approval, Servitor immediately posts:

```text
Plan approved.
Creating... This may take 30m-90m.

Diagnostic ID: `ID`
```

A second `yes` during apply reports progress but cannot start another apply. Ready output uses separate, readable Created and Reused tables, shows the UTC expiry, and says how to use `done`. Lease expiry timestamps in ready, list, existing-allocation, and extension responses use `YYYY-MM-DD HH:MM:SS UTC (~Nh)`, with remaining hours rounded to the nearest hour. Slack output contains only whitelisted, sanitized metadata; it never includes Terraform plan/state, filesystem paths, credentials, subprocess output, or private logs. Long help, review, ready, and list output is split at logical boundaries with balanced code fences.

### List, cleanup, and extension

`list` is a Slack code-block table of known lifecycle records with cluster, state, location, and expiry. It never resolves Slack identities and marks only the caller's row with `*`; unavailable values are `-`. An empty list contains the table header only.

`done` in the active lifecycle thread, or `@servitor done` in the configured channel, requests cleanup only for the caller's dedicated Slack-ID workspace. Cleanup starts asynchronously, retries after 1, 5, and 15 minutes, and never removes unrelated ICT workspaces such as `default`. A final cleanup failure remains unresolved for maintainer investigation.

`extend`, only for a ready lifecycle owned by the caller, accepts bare `extend`, `extend N`, or `extend Nh`, where `N` is an integer from 1 through 24. Bare form adds the configured initial lease. On success, Slack uses an aligned code block for the previous expiry, new expiry, and actual added duration. Exact-hour additions use `Nh`; a clamped addition below one hour uses `<1h`; other partial-hour additions use rounded `~Nh`. Remaining lease time is capped at 24 hours.

## Lifecycle state and restart behavior

The authoritative record is `.servitor-lifecycle.json` inside the caller's Servitor-owned ICT workspace. ICT-local records are removed with a successful workspace destroy. `ict list --output json` is used privately to discover workspace paths; paths are never sent to Slack.

On restart, Servitor cleans up interrupted review or apply instead of resuming it, restores ready and extended-ready lease deadlines, starts cleanup for an expired lease, resumes cleanup retries, and retains final cleanup failures as unresolved. A missing workspace while resources may still exist is unresolved, not proof of remote deletion. A record-less Slack-ID workspace is conservatively cleaned up; unrelated workspaces such as `default` are retained.

## Operations and diagnostics

Startup writes progress to stderr and private `paths.logs/servitor.log`: configuration load, Slack authentication, reconciliation, and `ready; accepting Socket Mode events`. The log correlates safe event and lifecycle identifiers, admission outcomes, diagnostic IDs, and private diagnostic locations. Private diagnostic output is logical-line framed, ANSI-free, and rotated between records at `logs.max_size_bytes`; each complete logical output line is reconstructable across rotation, including long JSON lines.

Each lifecycle has an opaque diagnostic ID. Slack diagnostic references render the ID as inline code. On the authenticated host, inspect `paths.logs/<diagnostic-id>/`; never copy these logs, plans, state, or workspace paths into Slack or source control. Resolved lifecycle and recordless diagnostics are retained for `logs.resolved_retention` (720 hours in the example). Active and unresolved diagnostics are retained conservatively.

The maintainer uses bare DM commands:

- `status`: show admission mode and activity counts.
- `pause`: immediately pause new modifying commands and decline pending reviews. Existing apply and cleanup continue.
- `unpause`: reopen admission only from paused.
- `stop`: immediately enter `draining-to-stop`, decline pending reviews, wait without an automatic deadline for applies and cleanup/retry chains, notify the maintainer, then exit.

While paused or draining, users may only use `help` and `list`; automatic expiry cleanup continues. A future ready lease does not block graceful stop and is restored at the next start. Use `stop` for normal maintenance. SIGINT and SIGTERM are emergency interruption paths, not a replacement for graceful drain.

For unresolved cleanup, inspect the private diagnostics and `ict list`, then perform required local ICT remediation for the caller's Slack-ID workspace. Preserve unrelated `default` and any other non-Servitor workspace.
