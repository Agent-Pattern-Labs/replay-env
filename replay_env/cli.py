from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="replay-env",
        description="Export production history into replay capsules and materialize it locally.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser(
        "export-postgres",
        help="Export a subject graph using a generic app manifest.",
    )
    export_parser.add_argument("--app", required=True, help="Path to config/apps/<app>.json")
    export_parser.add_argument("--subject", action="append", default=[], help="Subject key=value. Repeatable.")
    export_parser.add_argument("--db-url", default="")
    export_parser.add_argument("--out", required=True)
    export_parser.add_argument("--since-days", type=int, default=180)
    export_parser.add_argument("--redaction", choices=["safe", "raw"], default="safe")
    export_parser.add_argument("--psql-command", default=os.environ.get("REPLAY_ENV_PSQL_COMMAND", "psql"))
    export_parser.set_defaults(func=export_postgres)

    materialize_parser = subparsers.add_parser(
        "materialize-postgres",
        help="Load a replay capsule into a local Postgres replay target.",
    )
    materialize_parser.add_argument("--app", required=True, help="Path to config/apps/<app>.json")
    materialize_parser.add_argument("--db-url", default="")
    materialize_parser.add_argument("--capsule", required=True)
    materialize_parser.add_argument(
        "--rewrite-subject",
        action="append",
        default=[],
        help="Rewrite subject key=value before materializing. Repeatable.",
    )
    materialize_parser.add_argument(
        "--use-local-subject",
        action="store_true",
        help="Rewrite the subject to the app manifest's localSubject values.",
    )
    materialize_parser.add_argument("--psql-command", default=os.environ.get("REPLAY_ENV_PSQL_COMMAND", "psql"))
    materialize_parser.set_defaults(func=materialize_postgres)

    inspect_parser = subparsers.add_parser("inspect", help="Print a replay capsule summary.")
    inspect_parser.add_argument("capsule")
    inspect_parser.set_defaults(func=inspect_capsule)

    playbook_parser = subparsers.add_parser("playbook", help="Print local replay commands for an app manifest.")
    playbook_parser.add_argument("--app", required=True, help="Path to config/apps/<app>.json")
    playbook_parser.set_defaults(func=print_playbook)

    args = parser.parse_args(argv)
    return args.func(args)


def export_postgres(args: argparse.Namespace) -> int:
    app = load_app_config(args.app)
    subject = parse_kv_pairs(args.subject)
    validate_subject(app, subject)
    db_url = args.db_url or os.environ.get(app["postgres"].get("prodDatabaseUrlEnv", ""), "")
    require_db_url(db_url, app["postgres"].get("prodDatabaseUrlEnv", "APP_PROD_DATABASE_URL"), args.psql_command)

    sql = build_postgres_export_sql(app, subject, args.since_days)
    stdout = run_psql(db_url, sql, args.psql_command)
    document = json.loads(stdout)
    if args.redaction == "safe":
        apply_redactions(document, app.get("redactionRules", []))
    document["redaction"] = args.redaction
    document["source"] = {
        "kind": "postgres",
        "databaseUrlEnv": app["postgres"].get("prodDatabaseUrlEnv", ""),
        "exportedBy": "replay-env",
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {out_path}")
    print(summary_text(document))
    return 0


def materialize_postgres(args: argparse.Namespace) -> int:
    app = load_app_config(args.app)
    db_url = args.db_url or os.environ.get(app["postgres"].get("replayDatabaseUrlEnv", ""), "")
    require_db_url(db_url, app["postgres"].get("replayDatabaseUrlEnv", "APP_REPLAY_DATABASE_URL"), args.psql_command)

    capsule = load_capsule(args.capsule)
    require_capsule_for_app(capsule, app)
    rewrite_values = parse_kv_pairs(args.rewrite_subject)
    if args.use_local_subject:
        rewrite_values.update(app.get("localSubject", {}))
    if rewrite_values:
        capsule = rewrite_subject_scope(capsule, rewrite_values)

    sql = build_postgres_materialize_sql(app, capsule)
    run_psql(db_url, sql, args.psql_command)
    print(f"materialized {args.capsule}")
    print(summary_text(capsule))
    if rewrite_values:
        print("subject rewrite: " + ", ".join(f"{key}={value}" for key, value in sorted(rewrite_values.items())))
    return 0


def inspect_capsule(args: argparse.Namespace) -> int:
    print(summary_text(load_capsule(args.capsule)))
    return 0


def print_playbook(args: argparse.Namespace) -> int:
    app = load_app_config(args.app)
    repo_path = app.get("repoPath", "<target-repo-path>")
    local_db = app.get("localDatabaseUrl", "<local-database-url>")
    app_path = str(Path(repo_path).resolve()) if repo_path != "<target-repo-path>" else repo_path
    subject_args = " ".join(f"--subject {key}=<value>" for key in app["subjectKeys"])
    local_subject = " ".join(f"--rewrite-subject {key}={value}" for key, value in app.get("localSubject", {}).items())
    if app.get("localSubject"):
        local_subject = "--use-local-subject"

    print(
        f"""Generic replay commands for {app['appId']}:

1. Export a scoped production subject graph:
   replay-env export-postgres --app {args.app} {subject_args} --out capsules/{app['appId']}/<capsule>.json

2. Start or migrate the local app database using the app's normal dev flow:
   cd {app_path}
   # use local database: {local_db}

3. Materialize the capsule into the local replay database:
   replay-env materialize-postgres --app {args.app} --db-url "{local_db}" --capsule capsules/{app['appId']}/<capsule>.json {local_subject}

4. Start the app frontend/backend normally and open its local URL.
"""
    )
    return 0


def load_app_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        app = json.load(handle)
    app["_configPath"] = str(config_path)
    app["_configDir"] = str(config_path.parent)
    validate_app_config(app)
    return app


def validate_app_config(app: dict[str, Any]) -> None:
    for field in ["appId", "adapter", "subjectKeys", "postgres"]:
        if field not in app:
            raise SystemExit(f"app manifest is missing {field}")
    if app["adapter"] != "postgres-subject-graph.v1":
        raise SystemExit(f"unsupported adapter: {app['adapter']}")
    if not app["subjectKeys"]:
        raise SystemExit("app manifest subjectKeys must not be empty")
    for key in app["subjectKeys"]:
        validate_identifier(key, "subject key")
    postgres = app["postgres"]
    for field in ["graphEdges", "tables"]:
        if field not in postgres:
            raise SystemExit(f"app manifest postgres section is missing {field}")
    table_names = [table["name"] for table in postgres["tables"]]
    if len(table_names) != len(set(table_names)):
        raise SystemExit("app manifest has duplicate table names")
    table_order = postgres.get("tableOrder", table_names)
    unknown = sorted(set(table_order) - set(table_names))
    if unknown:
        raise SystemExit(f"tableOrder references unknown tables: {', '.join(unknown)}")
    for table in postgres["tables"]:
        validate_identifier(table["name"], "table name")
        if "alias" in table:
            validate_identifier(table["alias"], "table alias")


def validate_subject(app: dict[str, Any], subject: dict[str, str]) -> None:
    missing = [key for key in app["subjectKeys"] if key not in subject or subject[key] == ""]
    if missing:
        example = " ".join(f"--subject {key}=<value>" for key in missing)
        raise SystemExit(f"missing subject values: {example}")


def build_postgres_export_sql(app: dict[str, Any], subject: dict[str, str], since_days: int) -> str:
    subject_keys = app["subjectKeys"]
    select_subject = ", ".join(f"{sql_literal(subject[key])}::text as {key}" for key in subject_keys)
    base_graph = ", ".join(f"seed.{key}" for key in subject_keys)
    graph_edges = "\n\n    union all\n".join(edge.strip() for edge in app["postgres"].get("graphEdges", []))
    graph_union = ""
    if graph_edges:
        graph_union = "\n\n    union all\n" + graph_edges
    graph_group = ", ".join(subject_keys)
    graph_join = " and ".join(f"a.{key} = g.{key}" for key in subject_keys)
    graph_not_empty = " and ".join(f"coalesce({key}, '') <> ''" for key in subject_keys)
    subject_json = ", ".join(f"'{key}', (select {key} from params)" for key in subject_keys)
    graph_order = ", ".join(subject_keys)
    table_parts = []
    for table in app["postgres"]["tables"]:
        table_parts.append(build_table_export_sql(table))
    table_json = ",\n            ".join(table_parts)
    since_days = max(1, int(since_days))
    return f"""
with
params as (
    select
        {select_subject},
        now() - interval '{since_days} days' as since_at
),
all_graph_keys as (
    select {base_graph}, 'subject'::text as reason from params seed{graph_union}
),
graph_keys as (
    select distinct {graph_group}
    from all_graph_keys
    where {graph_not_empty}
),
graph as (
    select
        {', '.join('g.' + key for key in subject_keys)},
        coalesce(jsonb_agg(distinct a.reason) filter (where a.reason is not null), '[]'::jsonb) as reasons
    from graph_keys g
    left join all_graph_keys a on {graph_join}
    group by {', '.join('g.' + key for key in subject_keys)}
),
doc as (
    select jsonb_build_object(
        'schemaVersion', 1,
        'app', {sql_literal(app['appId'])},
        'adapter', {sql_literal(app['adapter'])},
        'exportedAt', to_jsonb(now()),
        'window', jsonb_build_object('sinceDays', {since_days}, 'sinceAt', (select to_jsonb(since_at) from params)),
        'subject', jsonb_build_object({subject_json}),
        'graph', coalesce((select jsonb_agg(to_jsonb(graph) order by {graph_order}) from graph), '[]'::jsonb),
        'tables', jsonb_build_object(
            {table_json}
        )
    ) as payload
)
select payload::text from doc;
"""


def build_table_export_sql(table: dict[str, Any]) -> str:
    name = table["name"]
    alias = table.get("alias", "t")
    from_sql = table.get("from", f"{name} {alias}")
    row_sql = table.get("rowSql", f"to_jsonb({alias})")
    where_sql = table.get("where", "").strip()
    order_sql = table.get("orderBy", "").strip()
    where_clause = f"\n                where {where_sql}" if where_sql else ""
    order_clause = f"\n                order by {order_sql}" if order_sql else ""
    return f"""'{name}', coalesce((select jsonb_agg(row_json) from (
                select {row_sql} as row_json
                from {from_sql}{where_clause}{order_clause}
            ) rows), '[]'::jsonb)"""


def build_postgres_materialize_sql(app: dict[str, Any], capsule: dict[str, Any]) -> str:
    subject_keys = app["subjectKeys"]
    tables_by_name = {table["name"]: table for table in app["postgres"]["tables"]}
    table_order = app["postgres"].get("tableOrder", list(tables_by_name))
    capsule_tables = capsule.get("tables", {})
    unknown = sorted(set(capsule_tables) - set(tables_by_name))
    if unknown:
        raise SystemExit(f"capsule has tables not declared by app manifest: {', '.join(unknown)}")

    graph_rows = []
    for row in capsule.get("graph", []):
        graph_rows.append({key: row.get(key, "") for key in subject_keys})
    graph_record_cols = ", ".join(f"{key} text" for key in subject_keys)
    graph_table_cols = ", ".join(f"{key} text not null" for key in subject_keys)

    parts = [
        "begin;",
        f"create temp table replay_graph ({graph_table_cols}) on commit drop;",
        f"insert into replay_graph select * from jsonb_to_recordset({sql_json_literal(graph_rows)}::jsonb) as g({graph_record_cols});",
    ]
    for table_name in reversed(table_order):
        if table_name not in capsule_tables:
            continue
        table = tables_by_name[table_name]
        predicate = table.get("deletePredicate", "").strip()
        if not predicate:
            raise SystemExit(f"table {table_name} is missing deletePredicate in app manifest")
        parts.append(f"delete from {table_name} t using replay_graph g where {predicate};")
    for table_name in table_order:
        rows = capsule_tables.get(table_name, [])
        if not rows:
            continue
        parts.append(
            f"insert into {table_name} select * from jsonb_populate_recordset(null::{table_name}, {sql_json_literal(rows)}::jsonb);"
        )
    parts.append("commit;")
    return "\n".join(parts)


def apply_redactions(capsule: dict[str, Any], rules: list[dict[str, Any]]) -> None:
    tables = capsule.get("tables", {})
    for table_name, rows in tables.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            for rule in rules:
                if rule.get("table") and rule["table"] != table_name:
                    continue
                redact_value(row, rule, recursive=rule.get("recursive", True))


def redact_value(value: Any, rule: dict[str, Any], *, recursive: bool) -> Any:
    if isinstance(value, dict):
        fields = set(rule.get("fields", []))
        for key in list(value.keys()):
            if key in fields:
                value[key] = redacted_field_value(value[key], rule)
            elif recursive:
                redact_value(value[key], rule, recursive=recursive)
    elif isinstance(value, list) and recursive:
        for item in value:
            redact_value(item, rule, recursive=recursive)
    return value


def redacted_field_value(value: Any, rule: dict[str, Any]) -> Any:
    action = rule.get("action", "empty")
    if value in ("", None):
        return value
    if action == "empty":
        return ""
    if action == "null":
        return None
    if action == "empty_object":
        return {}
    if action == "empty_array":
        return []
    if action == "hash":
        prefix = rule.get("prefix", "replay-")
        digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
        return prefix + digest
    raise SystemExit(f"unsupported redaction action: {action}")


def rewrite_subject_scope(capsule: dict[str, Any], new_subject: dict[str, str]) -> dict[str, Any]:
    old_subject = capsule.get("subject", {})
    missing = [key for key in new_subject if key not in old_subject]
    if missing:
        raise SystemExit(f"rewrite key is not in capsule subject: {', '.join(missing)}")
    normalized_old = {normalize_key(key): str(value) for key, value in old_subject.items()}
    normalized_new = {normalize_key(key): str(value) for key, value in new_subject.items()}

    def rewrite(value: Any, key: str = "") -> Any:
        if isinstance(value, dict):
            return {child_key: rewrite(child_value, child_key) for child_key, child_value in value.items()}
        if isinstance(value, list):
            return [rewrite(item, key) for item in value]
        if isinstance(value, str):
            normalized_key = normalize_key(key)
            for subject_key, old_value in normalized_old.items():
                if subject_key in normalized_key and value == old_value:
                    return normalized_new.get(subject_key, value)
        return value

    rewritten = rewrite(capsule)
    rewritten.setdefault("materialization", {})
    rewritten["materialization"]["subjectScopeRewrite"] = {
        "from": old_subject,
        "to": new_subject,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    rewritten["subject"] = {**old_subject, **new_subject}
    return rewritten


def require_capsule_for_app(capsule: dict[str, Any], app: dict[str, Any]) -> None:
    if capsule.get("app") != app["appId"]:
        raise SystemExit(f"capsule app {capsule.get('app')} does not match manifest app {app['appId']}")
    if not isinstance(capsule.get("tables"), dict):
        raise SystemExit("capsule is missing tables")
    subject = capsule.get("subject", {})
    missing = [key for key in app["subjectKeys"] if key not in subject]
    if missing:
        raise SystemExit(f"capsule subject is missing keys: {', '.join(missing)}")


def load_capsule(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise SystemExit("capsule must be a JSON object")
    return value


def parse_kv_pairs(values: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(f"expected key=value, got {value!r}")
        key, raw = value.split("=", 1)
        key = key.strip()
        raw = raw.strip()
        validate_identifier(key, "key")
        parsed[key] = raw
    return parsed


def require_db_url(db_url: str, env_name: str, psql_command: str) -> None:
    if not db_url.strip():
        raise SystemExit(f"missing database URL: pass --db-url or set {env_name}")
    command = split_command(psql_command)
    if not command:
        raise SystemExit("missing psql command")
    if shutil.which(command[0]) is None:
        raise SystemExit(
            f"{command[0]} is not on PATH; install psql or pass --psql-command / REPLAY_ENV_PSQL_COMMAND"
        )


def run_psql(db_url: str, sql: str, psql_command: str) -> str:
    result = subprocess.run(
        split_command(psql_command) + [db_url, "-X", "-v", "ON_ERROR_STOP=1", "-q", "-A", "-t"],
        check=False,
        capture_output=True,
        input=sql,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or f"psql failed with exit code {result.returncode}")
    return result.stdout.strip()


def split_command(command: str) -> list[str]:
    import shlex

    return shlex.split(command)


def summary_text(capsule: dict[str, Any]) -> str:
    subject = capsule.get("subject", {})
    graph = capsule.get("graph", [])
    tables = capsule.get("tables", {})
    lines = [
        f"app: {capsule.get('app', 'unknown')}",
        f"adapter: {capsule.get('adapter', 'unknown')}",
        "subject: " + ", ".join(f"{key}={value}" for key, value in subject.items()),
        f"redaction: {capsule.get('redaction', 'unknown')}",
        f"graph subjects: {len(graph)}",
        "tables:",
    ]
    for table in sorted(tables):
        rows = tables[table]
        count = len(rows) if isinstance(rows, list) else 0
        lines.append(f"  {table}: {count}")
    return "\n".join(lines)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_json_literal(value: Any) -> str:
    return sql_literal(json.dumps(value, separators=(",", ":"), sort_keys=True))


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def validate_identifier(value: str, label: str) -> None:
    if not IDENTIFIER_RE.match(value):
        raise SystemExit(f"invalid {label}: {value}")


if __name__ == "__main__":
    raise SystemExit(main())
