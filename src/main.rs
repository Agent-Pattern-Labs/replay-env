use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Parser)]
#[command(
    name = "replay-env",
    about = "Export production history into replay capsules and materialize it locally."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export a subject graph using a generic app manifest.
    ExportPostgres {
        /// Path to config/apps/<app>.json.
        #[arg(long)]
        app: PathBuf,
        /// Subject key=value. Repeatable.
        #[arg(long)]
        subject: Vec<String>,
        #[arg(long, default_value = "")]
        db_url: String,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 180)]
        since_days: u32,
        #[arg(long, value_enum, default_value_t = RedactionMode::Safe)]
        redaction: RedactionMode,
        #[arg(long, env = "REPLAY_ENV_PSQL_COMMAND", default_value = "psql")]
        psql_command: String,
    },
    /// Load a replay capsule into a local Postgres replay target.
    MaterializePostgres {
        /// Path to config/apps/<app>.json.
        #[arg(long)]
        app: PathBuf,
        #[arg(long, default_value = "")]
        db_url: String,
        #[arg(long)]
        capsule: PathBuf,
        /// Rewrite subject key=value before materializing. Repeatable.
        #[arg(long)]
        rewrite_subject: Vec<String>,
        /// Rewrite the subject to the app manifest's localSubject values.
        #[arg(long)]
        use_local_subject: bool,
        #[arg(long, env = "REPLAY_ENV_PSQL_COMMAND", default_value = "psql")]
        psql_command: String,
    },
    /// Print a replay capsule summary.
    Inspect { capsule: PathBuf },
    /// Print local replay commands for an app manifest.
    Playbook {
        /// Path to config/apps/<app>.json.
        #[arg(long)]
        app: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RedactionMode {
    Safe,
    Raw,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AppConfig {
    app_id: String,
    adapter: String,
    repo_path: Option<String>,
    subject_keys: Vec<String>,
    #[serde(default)]
    local_subject: HashMap<String, String>,
    local_database_url: Option<String>,
    postgres: PostgresConfig,
    #[serde(default)]
    redaction_rules: Vec<RedactionRule>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PostgresConfig {
    prod_database_url_env: Option<String>,
    replay_database_url_env: Option<String>,
    #[serde(default)]
    graph_edges: Vec<String>,
    #[serde(default)]
    table_order: Vec<String>,
    tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TableConfig {
    name: String,
    alias: Option<String>,
    from: Option<String>,
    row_sql: Option<String>,
    #[serde(rename = "where")]
    where_sql: Option<String>,
    order_by: Option<String>,
    delete_predicate: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RedactionRule {
    table: Option<String>,
    #[serde(default)]
    fields: Vec<String>,
    action: Option<String>,
    prefix: Option<String>,
    recursive: Option<bool>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::ExportPostgres {
            app,
            subject,
            db_url,
            out,
            since_days,
            redaction,
            psql_command,
        } => export_postgres(
            &app,
            subject,
            db_url,
            &out,
            since_days,
            redaction,
            &psql_command,
        ),
        Commands::MaterializePostgres {
            app,
            db_url,
            capsule,
            rewrite_subject,
            use_local_subject,
            psql_command,
        } => materialize_postgres(
            &app,
            db_url,
            &capsule,
            rewrite_subject,
            use_local_subject,
            &psql_command,
        ),
        Commands::Inspect { capsule } => {
            let capsule = load_capsule(&capsule)?;
            println!("{}", summary_text(&capsule));
            Ok(())
        }
        Commands::Playbook { app } => print_playbook(&app),
    }
}

fn export_postgres(
    app_path: &Path,
    subject_args: Vec<String>,
    db_url_arg: String,
    out: &Path,
    since_days: u32,
    redaction: RedactionMode,
    psql_command: &str,
) -> Result<()> {
    let app = load_app_config(app_path)?;
    let subject = parse_kv_pairs(subject_args)?;
    validate_subject(&app, &subject)?;
    let env_name = app
        .postgres
        .prod_database_url_env
        .as_deref()
        .unwrap_or("APP_PROD_DATABASE_URL");
    let db_url = db_url_arg.trim().to_string().or_else_env(env_name);
    require_db_url(&db_url, env_name, psql_command)?;

    let sql = build_postgres_export_sql(&app, &subject, since_days)?;
    let stdout = run_psql(&db_url, &sql, psql_command)?;
    let mut document: Value =
        serde_json::from_str(stdout.trim()).context("parse psql export JSON output")?;

    if matches!(redaction, RedactionMode::Safe) {
        apply_redactions(&mut document, &app.redaction_rules);
    }
    object_mut(&mut document, "capsule")?.insert(
        "redaction".to_string(),
        Value::String(
            match redaction {
                RedactionMode::Safe => "safe",
                RedactionMode::Raw => "raw",
            }
            .to_string(),
        ),
    );
    object_mut(&mut document, "capsule")?.insert(
        "source".to_string(),
        json!({
            "kind": "postgres",
            "databaseUrlEnv": env_name,
            "exportedBy": "replay-env"
        }),
    );

    if let Some(parent) = out.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    fs::write(
        out,
        format!("{}\n", serde_json::to_string_pretty(&document)?),
    )
    .with_context(|| format!("write capsule {}", out.display()))?;
    println!("wrote {}", out.display());
    println!("{}", summary_text(&document));
    Ok(())
}

fn materialize_postgres(
    app_path: &Path,
    db_url_arg: String,
    capsule_path: &Path,
    rewrite_subject_args: Vec<String>,
    use_local_subject: bool,
    psql_command: &str,
) -> Result<()> {
    let app = load_app_config(app_path)?;
    let env_name = app
        .postgres
        .replay_database_url_env
        .as_deref()
        .unwrap_or("APP_REPLAY_DATABASE_URL");
    let db_url = db_url_arg.trim().to_string().or_else_env(env_name);
    require_db_url(&db_url, env_name, psql_command)?;

    let mut capsule = load_capsule(capsule_path)?;
    require_capsule_for_app(&capsule, &app)?;

    let mut rewrite_values = parse_kv_pairs(rewrite_subject_args)?;
    if use_local_subject {
        rewrite_values.extend(app.local_subject.clone());
    }
    if !rewrite_values.is_empty() {
        capsule = rewrite_subject_scope(capsule, &rewrite_values)?;
    }

    let sql = build_postgres_materialize_sql(&app, &capsule)?;
    run_psql(&db_url, &sql, psql_command)?;
    println!("materialized {}", capsule_path.display());
    println!("{}", summary_text(&capsule));
    if !rewrite_values.is_empty() {
        let mut parts: Vec<_> = rewrite_values.iter().collect();
        parts.sort_by(|a, b| a.0.cmp(b.0));
        println!(
            "subject rewrite: {}",
            parts
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

fn print_playbook(app_path: &Path) -> Result<()> {
    let app = load_app_config(app_path)?;
    let repo_path = app.repo_path.as_deref().unwrap_or("<target-repo-path>");
    let local_db = app
        .local_database_url
        .as_deref()
        .unwrap_or("<local-database-url>");
    let subject_args = app
        .subject_keys
        .iter()
        .map(|key| format!("--subject {key}=<value>"))
        .collect::<Vec<_>>()
        .join(" ");
    let local_subject = if app.local_subject.is_empty() {
        app.local_subject
            .iter()
            .map(|(key, value)| format!("--rewrite-subject {key}={value}"))
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        "--use-local-subject".to_string()
    };

    println!(
        "Generic replay commands for {app_id}:\n\n\
1. Export a scoped production subject graph:\n   \
replay-env export-postgres --app {app_path} {subject_args} --out capsules/{app_id}/<capsule>.json\n\n\
2. Start or migrate the local app database using the app's normal dev flow:\n   \
cd {repo_path}\n   # use local database: {local_db}\n\n\
3. Materialize the capsule into the local replay database:\n   \
replay-env materialize-postgres --app {app_path} --db-url \"{local_db}\" --capsule capsules/{app_id}/<capsule>.json {local_subject}\n\n\
4. Start the app frontend/backend normally and open its local URL.",
        app_id = app.app_id,
        app_path = app_path.display(),
    );
    Ok(())
}

fn load_app_config(path: &Path) -> Result<AppConfig> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read app manifest {}", path.display()))?;
    let app: AppConfig = serde_json::from_str(&text)
        .with_context(|| format!("parse app manifest {}", path.display()))?;
    validate_app_config(&app)?;
    Ok(app)
}

fn validate_app_config(app: &AppConfig) -> Result<()> {
    if app.adapter != "postgres-subject-graph.v1" {
        bail!("unsupported adapter: {}", app.adapter);
    }
    if app.subject_keys.is_empty() {
        bail!("app manifest subjectKeys must not be empty");
    }
    for key in &app.subject_keys {
        validate_identifier(key, "subject key")?;
    }
    let table_names = app
        .postgres
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<Vec<_>>();
    let unique = table_names.iter().collect::<BTreeSet<_>>();
    if table_names.len() != unique.len() {
        bail!("app manifest has duplicate table names");
    }
    let table_order = table_order(app);
    let declared = table_names.into_iter().collect::<BTreeSet<_>>();
    let unknown = table_order
        .iter()
        .filter(|name| !declared.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!(
            "tableOrder references unknown tables: {}",
            unknown.join(", ")
        );
    }
    for table in &app.postgres.tables {
        validate_identifier(&table.name, "table name")?;
        if let Some(alias) = &table.alias {
            validate_identifier(alias, "table alias")?;
        }
    }
    for rule in &app.redaction_rules {
        let action = rule.action.as_deref().unwrap_or("empty");
        if !matches!(
            action,
            "empty" | "null" | "empty_object" | "empty_array" | "hash"
        ) {
            bail!("unsupported redaction action: {action}");
        }
    }
    Ok(())
}

fn validate_subject(app: &AppConfig, subject: &HashMap<String, String>) -> Result<()> {
    let missing = app
        .subject_keys
        .iter()
        .filter(|key| {
            subject
                .get(key.as_str())
                .is_none_or(|value| value.is_empty())
        })
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let example = missing
            .iter()
            .map(|key| format!("--subject {key}=<value>"))
            .collect::<Vec<_>>()
            .join(" ");
        bail!("missing subject values: {example}");
    }
    Ok(())
}

fn build_postgres_export_sql(
    app: &AppConfig,
    subject: &HashMap<String, String>,
    since_days: u32,
) -> Result<String> {
    let subject_keys = &app.subject_keys;
    let select_subject = subject_keys
        .iter()
        .map(|key| {
            Ok(format!(
                "{}::text as {key}",
                sql_literal(
                    subject
                        .get(key)
                        .ok_or_else(|| anyhow!("missing subject {key}"))?
                )
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let base_graph = subject_keys
        .iter()
        .map(|key| format!("seed.{key}"))
        .collect::<Vec<_>>()
        .join(", ");
    let graph_edges = app
        .postgres
        .graph_edges
        .iter()
        .map(|edge| edge.trim())
        .collect::<Vec<_>>()
        .join("\n\n    union all\n");
    let graph_union = if graph_edges.is_empty() {
        String::new()
    } else {
        format!("\n\n    union all\n{graph_edges}")
    };
    let graph_group = subject_keys.join(", ");
    let graph_join = subject_keys
        .iter()
        .map(|key| format!("a.{key} = g.{key}"))
        .collect::<Vec<_>>()
        .join(" and ");
    let graph_not_empty = subject_keys
        .iter()
        .map(|key| format!("coalesce({key}, '') <> ''"))
        .collect::<Vec<_>>()
        .join(" and ");
    let subject_json = subject_keys
        .iter()
        .map(|key| format!("'{key}', (select {key} from params)"))
        .collect::<Vec<_>>()
        .join(", ");
    let graph_order = subject_keys.join(", ");
    let table_json = app
        .postgres
        .tables
        .iter()
        .map(build_table_export_sql)
        .collect::<Vec<_>>()
        .join(",\n            ");
    let since_days = since_days.max(1);

    Ok(format!(
        "\
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
        {graph_select},
        coalesce(jsonb_agg(distinct a.reason) filter (where a.reason is not null), '[]'::jsonb) as reasons
    from graph_keys g
    left join all_graph_keys a on {graph_join}
    group by {graph_group_prefixed}
),
doc as (
    select jsonb_build_object(
        'schemaVersion', 1,
        'app', {app_id},
        'adapter', {adapter},
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
",
        graph_select = subject_keys
            .iter()
            .map(|key| format!("g.{key}"))
            .collect::<Vec<_>>()
            .join(", "),
        graph_group_prefixed = subject_keys
            .iter()
            .map(|key| format!("g.{key}"))
            .collect::<Vec<_>>()
            .join(", "),
        app_id = sql_literal(&app.app_id),
        adapter = sql_literal(&app.adapter),
    ))
}

fn build_table_export_sql(table: &TableConfig) -> String {
    let name = &table.name;
    let alias = table.alias.as_deref().unwrap_or("t");
    let from_sql = table
        .from
        .clone()
        .unwrap_or_else(|| format!("{name} {alias}"));
    let row_sql = table
        .row_sql
        .clone()
        .unwrap_or_else(|| format!("to_jsonb({alias})"));
    let where_clause = table
        .where_sql
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("\n                where {value}"))
        .unwrap_or_default();
    let order_clause = table
        .order_by
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("\n                order by {value}"))
        .unwrap_or_default();
    format!(
        "'{name}', coalesce((select jsonb_agg(row_json) from (
                select {row_sql} as row_json
                from {from_sql}{where_clause}{order_clause}
            ) rows), '[]'::jsonb)"
    )
}

fn build_postgres_materialize_sql(app: &AppConfig, capsule: &Value) -> Result<String> {
    let tables_by_name = app
        .postgres
        .tables
        .iter()
        .map(|table| (table.name.as_str(), table))
        .collect::<BTreeMap<_, _>>();
    let table_order = table_order(app);
    let capsule_tables = capsule
        .get("tables")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("capsule is missing tables"))?;
    let unknown = capsule_tables
        .keys()
        .filter(|name| !tables_by_name.contains_key(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!(
            "capsule has tables not declared by app manifest: {}",
            unknown.join(", ")
        );
    }

    let graph_rows = capsule
        .get("graph")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let mut out = Map::new();
                    for key in &app.subject_keys {
                        out.insert(
                            key.clone(),
                            row.get(key)
                                .cloned()
                                .unwrap_or_else(|| Value::String(String::new())),
                        );
                    }
                    Value::Object(out)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let graph_record_cols = app
        .subject_keys
        .iter()
        .map(|key| format!("{key} text"))
        .collect::<Vec<_>>()
        .join(", ");
    let graph_table_cols = app
        .subject_keys
        .iter()
        .map(|key| format!("{key} text not null"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut parts = vec![
        "begin;".to_string(),
        format!("create temp table replay_graph ({graph_table_cols}) on commit drop;"),
        format!(
            "insert into replay_graph select * from jsonb_to_recordset({}::jsonb) as g({graph_record_cols});",
            sql_json_literal(&Value::Array(graph_rows))?
        ),
    ];
    for table_name in table_order.iter().rev() {
        if !capsule_tables.contains_key(table_name) {
            continue;
        }
        let table = tables_by_name
            .get(table_name.as_str())
            .ok_or_else(|| anyhow!("table {table_name} is missing from manifest"))?;
        let predicate = table
            .delete_predicate
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!("table {table_name} is missing deletePredicate in app manifest")
            })?;
        parts.push(format!(
            "delete from {table_name} t using replay_graph g where {predicate};"
        ));
    }
    for table_name in table_order {
        let Some(rows) = capsule_tables.get(&table_name).and_then(Value::as_array) else {
            continue;
        };
        if rows.is_empty() {
            continue;
        }
        parts.push(format!(
            "insert into {table_name} select * from jsonb_populate_recordset(null::{table_name}, {}::jsonb);",
            sql_json_literal(&Value::Array(rows.clone()))?
        ));
    }
    parts.push("commit;".to_string());
    Ok(parts.join("\n"))
}

fn apply_redactions(capsule: &mut Value, rules: &[RedactionRule]) {
    let Some(tables) = capsule.get_mut("tables").and_then(Value::as_object_mut) else {
        return;
    };
    for (table_name, rows) in tables {
        let Some(rows) = rows.as_array_mut() else {
            continue;
        };
        for row in rows {
            for rule in rules {
                if rule
                    .table
                    .as_deref()
                    .is_some_and(|table| table != table_name)
                {
                    continue;
                }
                redact_value(row, rule, rule.recursive.unwrap_or(true));
            }
        }
    }
}

fn redact_value(value: &mut Value, rule: &RedactionRule, recursive: bool) {
    match value {
        Value::Object(object) => {
            let fields = rule.fields.iter().collect::<BTreeSet<_>>();
            let keys = object.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if fields.contains(&key) {
                    if let Some(field) = object.get_mut(&key) {
                        *field = redacted_field_value(field, rule);
                    }
                } else if recursive {
                    if let Some(child) = object.get_mut(&key) {
                        redact_value(child, rule, recursive);
                    }
                }
            }
        }
        Value::Array(items) if recursive => {
            for item in items {
                redact_value(item, rule, recursive);
            }
        }
        _ => {}
    }
}

fn redacted_field_value(value: &Value, rule: &RedactionRule) -> Value {
    if value.is_null() || value.as_str().is_some_and(str::is_empty) {
        return value.clone();
    }
    match rule.action.as_deref().unwrap_or("empty") {
        "empty" => Value::String(String::new()),
        "null" => Value::Null,
        "empty_object" => Value::Object(Map::new()),
        "empty_array" => Value::Array(Vec::new()),
        "hash" => {
            let prefix = rule.prefix.as_deref().unwrap_or("replay-");
            let raw = value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            let digest = Sha256::digest(raw.as_bytes());
            let digest = format!("{digest:x}");
            Value::String(format!("{prefix}{}", &digest[..12]))
        }
        _ => value.clone(),
    }
}

fn rewrite_subject_scope(
    mut capsule: Value,
    new_subject: &HashMap<String, String>,
) -> Result<Value> {
    let old_subject = capsule
        .get("subject")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("capsule subject must be an object"))?
        .clone();
    let missing = new_subject
        .keys()
        .filter(|key| !old_subject.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "rewrite key is not in capsule subject: {}",
            missing.join(", ")
        );
    }

    let normalized_old = old_subject
        .iter()
        .map(|(key, value)| (normalize_key(key), value_to_match_string(value)))
        .collect::<HashMap<_, _>>();
    let normalized_new = new_subject
        .iter()
        .map(|(key, value)| (normalize_key(key), value.clone()))
        .collect::<HashMap<_, _>>();
    rewrite_value(&mut capsule, "", &normalized_old, &normalized_new);

    let subject = capsule
        .get_mut("subject")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("capsule subject must be an object"))?;
    for (key, value) in new_subject {
        subject.insert(key.clone(), Value::String(value.clone()));
    }

    let materialization = capsule
        .as_object_mut()
        .ok_or_else(|| anyhow!("capsule must be an object"))?
        .entry("materialization")
        .or_insert_with(|| Value::Object(Map::new()));
    let materialization = materialization
        .as_object_mut()
        .ok_or_else(|| anyhow!("capsule materialization must be an object"))?;
    materialization.insert(
        "subjectScopeRewrite".to_string(),
        json!({
            "from": Value::Object(old_subject),
            "to": new_subject,
            "at": Utc::now().to_rfc3339()
        }),
    );
    Ok(capsule)
}

fn rewrite_value(
    value: &mut Value,
    key: &str,
    normalized_old: &HashMap<String, String>,
    normalized_new: &HashMap<String, String>,
) {
    match value {
        Value::Object(object) => {
            let keys = object.keys().cloned().collect::<Vec<_>>();
            for child_key in keys {
                if let Some(child) = object.get_mut(&child_key) {
                    rewrite_value(child, &child_key, normalized_old, normalized_new);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_value(item, key, normalized_old, normalized_new);
            }
        }
        Value::String(raw) => {
            let normalized_key = normalize_key(key);
            for (subject_key, old_value) in normalized_old {
                if normalized_key.contains(subject_key) && raw == old_value {
                    if let Some(new_value) = normalized_new.get(subject_key) {
                        *raw = new_value.clone();
                    }
                }
            }
        }
        _ => {}
    }
}

fn require_capsule_for_app(capsule: &Value, app: &AppConfig) -> Result<()> {
    let app_value = capsule
        .get("app")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("capsule is missing app"))?;
    if app_value != app.app_id {
        bail!(
            "capsule app {} does not match manifest app {}",
            app_value,
            app.app_id
        );
    }
    if !capsule.get("tables").is_some_and(Value::is_object) {
        bail!("capsule is missing tables");
    }
    let subject = capsule
        .get("subject")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("capsule subject must be an object"))?;
    let missing = app
        .subject_keys
        .iter()
        .filter(|key| !subject.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!("capsule subject is missing keys: {}", missing.join(", "));
    }
    Ok(())
}

fn load_capsule(path: &Path) -> Result<Value> {
    let text =
        fs::read_to_string(path).with_context(|| format!("read capsule {}", path.display()))?;
    let value: Value =
        serde_json::from_str(&text).with_context(|| format!("parse capsule {}", path.display()))?;
    if !value.is_object() {
        bail!("capsule must be a JSON object");
    }
    Ok(value)
}

fn parse_kv_pairs(values: Vec<String>) -> Result<HashMap<String, String>> {
    let mut parsed = HashMap::new();
    for value in values {
        let Some((key, raw)) = value.split_once('=') else {
            bail!("expected key=value, got {value:?}");
        };
        let key = key.trim().to_string();
        validate_identifier(&key, "key")?;
        parsed.insert(key, raw.trim().to_string());
    }
    Ok(parsed)
}

fn require_db_url(db_url: &str, env_name: &str, psql_command: &str) -> Result<()> {
    if db_url.trim().is_empty() {
        bail!("missing database URL: pass --db-url or set {env_name}");
    }
    let command = split_command(psql_command)?;
    if command.is_empty() {
        bail!("missing psql command");
    }
    if !command_exists(&command[0]) {
        bail!(
            "{} is not on PATH; install psql or pass --psql-command / REPLAY_ENV_PSQL_COMMAND",
            command[0]
        );
    }
    Ok(())
}

fn run_psql(db_url: &str, sql: &str, psql_command: &str) -> Result<String> {
    let mut command = split_command(psql_command)?;
    if command.is_empty() {
        bail!("missing psql command");
    }
    let executable = command.remove(0);
    let mut child = Command::new(executable)
        .args(command)
        .arg(db_url)
        .args(["-X", "-v", "ON_ERROR_STOP=1", "-q", "-A", "-t"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn psql command")?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| anyhow!("open psql stdin"))?
        .write_all(sql.as_bytes())
        .context("write SQL to psql")?;
    let output = child.wait_with_output().context("wait for psql")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "{}",
            if stderr.is_empty() {
                format!("psql failed with exit code {}", output.status)
            } else {
                stderr
            }
        );
    }
    Ok(String::from_utf8(output.stdout).context("decode psql stdout as UTF-8")?)
}

fn split_command(command: &str) -> Result<Vec<String>> {
    shlex::split(command).ok_or_else(|| anyhow!("could not parse command: {command}"))
}

fn summary_text(capsule: &Value) -> String {
    let subject = capsule.get("subject").and_then(Value::as_object);
    let tables = capsule.get("tables").and_then(Value::as_object);
    let mut lines = vec![
        format!(
            "app: {}",
            capsule
                .get("app")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ),
        format!(
            "adapter: {}",
            capsule
                .get("adapter")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ),
        format!(
            "subject: {}",
            subject
                .map(|subject| {
                    subject
                        .iter()
                        .map(|(key, value)| format!("{key}={}", value_to_match_string(value)))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default()
        ),
        format!(
            "redaction: {}",
            capsule
                .get("redaction")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ),
        format!(
            "graph subjects: {}",
            capsule
                .get("graph")
                .and_then(Value::as_array)
                .map_or(0, Vec::len)
        ),
        "tables:".to_string(),
    ];
    if let Some(tables) = tables {
        let mut table_names = tables.keys().collect::<Vec<_>>();
        table_names.sort();
        for table_name in table_names {
            let count = tables
                .get(table_name)
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            lines.push(format!("  {table_name}: {count}"));
        }
    }
    lines.join("\n")
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_json_literal(value: &Value) -> Result<String> {
    Ok(sql_literal(&serde_json::to_string(value)?))
}

fn normalize_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn validate_identifier(value: &str, label: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        bail!("invalid {label}: {value}");
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        bail!("invalid {label}: {value}");
    }
    Ok(())
}

fn object_mut<'a>(value: &'a mut Value, label: &str) -> Result<&'a mut Map<String, Value>> {
    value
        .as_object_mut()
        .ok_or_else(|| anyhow!("{label} must be a JSON object"))
}

fn table_order(app: &AppConfig) -> Vec<String> {
    if app.postgres.table_order.is_empty() {
        app.postgres
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect()
    } else {
        app.postgres.table_order.clone()
    }
}

fn value_to_match_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn command_exists(command: &str) -> bool {
    let path = Path::new(command);
    if path.components().count() > 1 {
        return path.exists();
    }
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|dir| {
            let candidate = dir.join(command);
            if candidate.exists() {
                return true;
            }
            #[cfg(windows)]
            {
                let candidate = dir.join(format!("{command}.exe"));
                if candidate.exists() {
                    return true;
                }
            }
            false
        })
    })
}

trait OrElseEnv {
    fn or_else_env(self, env_name: &str) -> String;
}

impl OrElseEnv for String {
    fn or_else_env(self, env_name: &str) -> String {
        if self.trim().is_empty() {
            env::var(env_name).unwrap_or_default()
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_redaction_is_stable_and_prefixed() {
        let rule = RedactionRule {
            table: None,
            fields: vec!["external_user_id".to_string()],
            action: Some("hash".to_string()),
            prefix: Some("replay-user-".to_string()),
            recursive: None,
        };

        let redacted = redacted_field_value(&Value::String("prod-user-123".to_string()), &rule);

        assert_eq!(
            redacted,
            Value::String("replay-user-3376300c1af8".to_string())
        );
    }

    #[test]
    fn rewrite_subject_scope_updates_matching_subject_fields() {
        let capsule = json!({
            "schemaVersion": 1,
            "app": "demo",
            "adapter": "postgres-subject-graph.v1",
            "subject": {
                "tenant_id": "tenant-prod",
                "user_id": "user-prod"
            },
            "graph": [
                {
                    "tenant_id": "tenant-prod",
                    "user_id": "user-prod",
                    "reasons": ["subject"]
                }
            ],
            "tables": {
                "profiles": [
                    {
                        "tenant_id": "tenant-prod",
                        "user_id": "user-prod",
                        "display_name": "Production User"
                    }
                ]
            }
        });
        let new_subject = HashMap::from([
            ("tenant_id".to_string(), "tenant-local".to_string()),
            ("user_id".to_string(), "user-local".to_string()),
        ]);

        let rewritten = rewrite_subject_scope(capsule, &new_subject).unwrap();

        assert_eq!(rewritten["subject"]["tenant_id"], "tenant-local");
        assert_eq!(rewritten["subject"]["user_id"], "user-local");
        assert_eq!(rewritten["graph"][0]["tenant_id"], "tenant-local");
        assert_eq!(rewritten["graph"][0]["user_id"], "user-local");
        assert_eq!(
            rewritten["tables"]["profiles"][0]["display_name"],
            "Production User"
        );
        assert!(rewritten["materialization"]["subjectScopeRewrite"].is_object());
    }
}
