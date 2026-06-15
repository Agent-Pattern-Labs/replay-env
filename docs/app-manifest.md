# App Manifest Contract

Replay Env is app-generic. Each repository gets a manifest that teaches the
generic engine how to export and materialize that app's production history.

The manifest format is:

```json
{
  "appId": "my-app",
  "adapter": "postgres-subject-graph.v1",
  "repoPath": "/path/to/repo",
  "subjectKeys": ["tenant_id", "user_id"],
  "localSubject": {
    "tenant_id": "local-dev-tenant",
    "user_id": "local-dev-user"
  },
  "postgres": {
    "prodDatabaseUrlEnv": "MY_APP_PROD_DATABASE_URL",
    "replayDatabaseUrlEnv": "MY_APP_REPLAY_DATABASE_URL",
    "graphEdges": [],
    "tableOrder": [],
    "tables": []
  },
  "commands": [
    {
      "name": "test",
      "run": "npm test",
      "timeoutSeconds": 300
    }
  ],
  "probes": [
    {
      "name": "api-health",
      "kind": "http",
      "url": "http://localhost:8080/api/health",
      "expectStatus": 200,
      "timeoutSeconds": 10
    }
  ],
  "redactionRules": []
}
```

## Subject

`subjectKeys` defines the identity columns for the app's replay graph. For many
tenant-scoped products this is `tenant_id` plus `user_id`; for other repos it
might be `account_id`, `workspace_id`, `organization_id`, `project_id`, or a
single `user_id`.

The CLI receives subject values generically:

```bash
replay-env export-postgres \
  --app config/apps/my-app.json \
  --subject account_id=acct_123 \
  --out capsules/my-app/acct_123.json
```

## Graph Edges

`graphEdges` is a list of SQL snippets. Each snippet must return the same
columns as `subjectKeys` plus a `reason` column.

The generic exporter creates:

```sql
params       -- one row with subject values and since_at
graph_keys   -- distinct subjects included in the replay graph
```

Graph edges can reference `params seed` and app tables.

## Tables

Each table declaration tells Replay Env how to export rows and how to clean old
rows before materializing:

```json
{
  "name": "events",
  "alias": "e",
  "from": "events e join graph_keys g on g.account_id = e.account_id",
  "where": "e.created_at >= (select since_at from params)",
  "orderBy": "e.created_at",
  "deletePredicate": "t.account_id = g.account_id"
}
```

`rowSql` is optional and defaults to `to_jsonb(<alias>)`. Use `rowSql` only when
an app needs a custom projection.

## Materialization

Materialization expects the local replay database schema to already exist. Start
the target app once, run migrations, or use the repo's normal local database
setup before importing a capsule.

Rows are inserted with Postgres `jsonb_populate_recordset`, so app table names
and JSON field names must match the local schema.

Use `--chunk-size` to split large table imports into smaller statements. Use
`--load-strategy copy` when a capsule is large enough that Postgres COPY is a
better fit than JSONB recordset inserts. COPY infers each chunk's column list
from object keys in the capsule rows.

Use `--dry-run-sql` to inspect the generated SQL/script. Use `--explain` to run
Postgres plan checks without exporting production data or mutating target
tables.

## Doctor

`replay-env doctor` validates the manifest and, when a replay DB URL is
provided, checks that declared tables and columns referenced by aliases/delete
predicates exist in the local schema.

```bash
replay-env doctor \
  --app config/apps/my-app.json \
  --db-url "<local-replay-database-url>"
```

Doctor intentionally treats production-looking database URLs as failures.

## Run Loop

`commands` and `probes` let `replay-env run` become the reusable agent harness
entrypoint. Commands run in manifest order. Background commands are started,
kept alive for later probes, and stopped before the run exits.

```json
{
  "commands": [
    {
      "name": "api",
      "run": "npm run dev:api",
      "background": true,
      "timeoutSeconds": 30
    },
    {
      "name": "test",
      "run": "npm test",
      "timeoutSeconds": 300
    }
  ],
  "probes": [
    {
      "name": "api-health",
      "kind": "http",
      "url": "http://localhost:8080/api/health",
      "expectStatus": 200
    },
    {
      "name": "smoke",
      "kind": "command",
      "command": "npm run test:smoke"
    }
  ]
}
```

Supported command fields:

- `name`: stable trace label.
- `run`: shell command.
- `cwd`: optional command working directory. Defaults to `repoPath` when set,
  otherwise the current directory.
- `background`: keep the process alive while probes run.
- `allowFailure`: record failure but do not block the run gate.
- `timeoutSeconds`: foreground command timeout.

Supported probe kinds:

- `http`: plain `http://` GET with `url`, optional `expectStatus`, and optional
  `timeoutSeconds`.
- `command`: shell command probe with `command`.

`replay-env run --trace-out <path>` writes a trace JSON document with command
results, probe results, timing, and a `continue` or `block` gate.

## Redaction

Safe exports apply manifest-defined redaction rules after export and before the
capsule is written:

```json
{
  "fields": ["access_token", "refresh_token"],
  "action": "empty"
}
```

Supported actions:

- `empty`: replace with an empty string.
- `null`: replace with null.
- `empty_object`: replace with `{}`.
- `empty_array`: replace with `[]`.
- `hash`: replace with a deterministic `prefix + sha256(value)[0:12]`.

Rules apply recursively by default. Set `"recursive": false` for top-level row
fields only.
