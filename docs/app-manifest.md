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
