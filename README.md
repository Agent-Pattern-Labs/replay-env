# Replay Env

Replay Env is a generic pre-staging environment for AI-assisted product repair.

It turns production history into a scoped replay capsule, materializes that
capsule into a local or staging-like target, runs frontend/backend probes,
captures traces, judges the outcome, and promotes only when the evidence is
good enough.

The loop is:

```text
Production history -> Replay capsule -> Local materialization -> Probe -> Trace -> Judge -> Repair -> Gate
```

## Why This Exists

Staging proves that a deployed build works in a clean environment.

Replay proves that the build survives the messy history that got the product to
this point: real users, cross-user interactions, old data shapes, background
jobs, AI outputs, receipts, retries, comments, reactions, and support/incident
patterns.

Every escaped bug should become a replay. Every excellent outcome should become
a golden trace.

## Install

Install directly from GitHub:

```bash
cargo install --git https://github.com/Agent-Pattern-Labs/replay-env --tag v0.2.0
```

Or install from a local checkout:

```bash
cargo install --path .
```

For development from a checkout, this wrapper also works:

```bash
bin/replay-env --help
```

Installed usage assumes the `replay-env` command is on `PATH`:

```bash
replay-env --help
```

Replay Env is implemented in Rust so the installed CLI starts quickly, keeps
runtime dependencies low, and has a clear path to single-binary distribution.

## Core Idea

Replay Env does not need one app-specific adapter per repo. Each repo gets an app
manifest under `config/apps/` or in the target repository. The manifest declares:

- subject identity keys, such as `tenant_id`, `user_id`, `account_id`, or `workspace_id`
- graph expansion SQL for related users/accounts/projects
- tables to export
- deletion predicates for local materialization
- redaction rules for safe capsules
- optional local subject mapping for dev auth

The generic engine reads that manifest and performs the export/materialization.

See [docs/app-manifest.md](docs/app-manifest.md).

## Commands

Export a scoped production subject graph:

```bash
replay-env export-postgres \
  --app config/apps/<app>.json \
  --subject tenant_id=<tenant-id> \
  --subject user_id=<user-id> \
  --since-days 180 \
  --out "capsules/<app>/<subject>-$(date -u +%Y%m%dT%H%M%SZ).json"
```

Inspect a capsule:

```bash
replay-env inspect capsules/<app>/<capsule>.json
```

Materialize into a local replay database:

```bash
replay-env materialize-postgres \
  --app config/apps/<app>.json \
  --db-url "<local-replay-database-url>" \
  --capsule "capsules/<app>/<capsule>.json"
```

If the app has a fixed local/dev-auth identity declared in its manifest:

```bash
replay-env materialize-postgres \
  --app config/apps/<app>.json \
  --db-url "<local-replay-database-url>" \
  --capsule "capsules/<app>/<capsule>.json" \
  --use-local-subject
```

Print the app-specific playbook generated from the manifest:

```bash
replay-env playbook --app config/apps/<app>.json
```

The CLI uses `psql` by default. If `psql` is not installed on the host, pass a
Postgres client command explicitly:

```bash
REPLAY_ENV_PSQL_COMMAND="docker run --rm -i postgres:16 psql" \
replay-env export-postgres ...
```

## Agent Harness Usage

Replay Env is intentionally callable from Codex or any other coding agent that
can run shell commands. The stable interface is the installed CLI:

```bash
replay-env
```

The recommended pattern is:

1. Add an app manifest under the target repo or pass a manifest path.
2. Ask the agent to inspect the manifest and target repo.
3. Export or receive a scoped replay capsule.
4. Materialize the capsule into the target app's local replay database.
5. Run the app's normal backend/frontend tests or browser probes.
6. Use the replay evidence to repair the app.
7. Store the escaped failure as a regression scenario.

For Codex, use this prompt shape from the target app repo:

```text
Use the installed `replay-env` CLI.

Target app manifest:
<path-to-app-manifest>.json

Goal:
Reproduce and repair the issue using production-shaped replay data before
promoting the fix to staging.

Rules:
- Do not read .env files unless I explicitly approve.
- Do not export production data unless I provide the subject and DB URL.
- Do not commit replay capsules, logs, or raw production artifacts.
- Use safe redaction by default.
- Materialize only into a local or staging-like replay database.
- After materialization, run the target app's normal tests and browser/API probes.
- Report the capsule path, row counts, commands run, trace evidence, failures,
  repair made, and whether the replay gate should block, warn, or continue.
```

Codex can call Replay Env directly:

```bash
replay-env playbook --app <path-to-app-manifest>.json
replay-env inspect <path-to-capsule>.json
replay-env materialize-postgres \
  --app <path-to-app-manifest>.json \
  --db-url "<local-replay-database-url>" \
  --capsule "<path-to-capsule>.json"
```

If the target app has a fixed local/dev-auth identity, let the agent use the
manifest's local subject mapping:

```bash
replay-env materialize-postgres \
  --app <path-to-app-manifest>.json \
  --db-url "<local-replay-database-url>" \
  --capsule "<path-to-capsule>.json" \
  --use-local-subject
```

To make a target repo reuse the same coding plan every time, add a short
`AGENTS.md` section to that repo:

```md
## Replay Env

When a bug may depend on production-shaped user/account/workspace history:

- Use the installed `replay-env` CLI.
- Prefer a scoped replay capsule over synthetic fixtures.
- Use this repo's app manifest.
- Never export production data without an explicit subject and database URL.
- Never commit capsules, raw traces, logs, or secrets.
- Materialize into local/staging replay databases only.
- After replay, capture commands, row counts, API responses, screenshots or
  browser errors, the repair, and the gate decision.
```

For automation, `codex exec` can run the same plan non-interactively from the
target repo. Pipe logs or capsule summaries into the prompt when useful:

```bash
cd /path/to/target-repo
replay-env inspect <path-to-capsule>.json \
  | codex exec "Use this Replay Env capsule summary to plan the smallest safe repair. Do not edit yet."
```

At this stage, Replay Env gives agents a reusable data-replay entrypoint. The
agent still needs to run the target app's tests/browser checks and make the
code repair. Replay Env does not yet replace a full E2E harness or release
gate by itself.

## Example Manifest

`config/apps/profilescribe.json` is only an example app manifest. It is not a
special adapter path. Other repos should add their own manifest with their own
subject keys, graph edges, tables, and redaction rules.

## Capsule Shape

A replay capsule is app-neutral:

```json
{
  "schemaVersion": 1,
  "app": "my-app",
  "adapter": "postgres-subject-graph.v1",
  "subject": { "tenant_id": "...", "user_id": "..." },
  "graph": [
    { "tenant_id": "...", "user_id": "...", "reasons": ["subject"] }
  ],
  "tables": {
    "profiles": [],
    "events": []
  }
}
```

## Safety Rules

- Export only an explicit subject scope.
- Store capsules outside git; `capsules/**/*.json` is ignored.
- Default `--redaction safe` applies manifest-defined redaction rules.
- Use `--redaction raw` only in a controlled internal environment with a clear
  retention policy.
- Replay targets should use local/staging credentials and mocked provider side
  effects. Do not let replay publish to real external accounts by default.

## Loop Files

The first operating loop lives in:

```text
loops/user-history-replay/
```

It defines what a good replay means, what evidence must be captured, how traces
are judged, and when a change can move from replay to staging.
