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

## Core Idea

Replay Env does not need one Python adapter per app. Each repo gets an app
manifest under `config/apps/` that declares:

- subject identity keys, such as `tenant_id`, `user_id`, `account_id`, or `workspace_id`
- graph expansion SQL for related users/accounts/projects
- tables to export
- deletion predicates for local materialization
- redaction rules for safe capsules
- optional local subject mapping for dev auth

The generic engine reads that manifest and performs the export/materialization.

See [docs/app-manifest.md](/Users/charlie/AgentPatternLabs/replay-env/docs/app-manifest.md:1).

## Commands

Export a scoped production subject graph:

```bash
bin/replay-env export-postgres \
  --app config/apps/<app>.json \
  --subject tenant_id=<tenant-id> \
  --subject user_id=<user-id> \
  --since-days 180 \
  --out "capsules/<app>/<subject>-$(date -u +%Y%m%dT%H%M%SZ).json"
```

Inspect a capsule:

```bash
bin/replay-env inspect capsules/<app>/<capsule>.json
```

Materialize into a local replay database:

```bash
bin/replay-env materialize-postgres \
  --app config/apps/<app>.json \
  --db-url "<local-replay-database-url>" \
  --capsule "capsules/<app>/<capsule>.json"
```

If the app has a fixed local/dev-auth identity declared in its manifest:

```bash
bin/replay-env materialize-postgres \
  --app config/apps/<app>.json \
  --db-url "<local-replay-database-url>" \
  --capsule "capsules/<app>/<capsule>.json" \
  --use-local-subject
```

Print the app-specific playbook generated from the manifest:

```bash
bin/replay-env playbook --app config/apps/<app>.json
```

The CLI uses `psql` by default. If `psql` is not installed on the host, pass a
Postgres client command explicitly:

```bash
REPLAY_ENV_PSQL_COMMAND="docker run --rm -i postgres:16 psql" \
bin/replay-env export-postgres ...
```

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
