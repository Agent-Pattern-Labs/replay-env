use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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
        /// Print generated SQL and exit without connecting to Postgres.
        #[arg(long)]
        dry_run_sql: bool,
        /// Run EXPLAIN for the generated SQL instead of exporting data.
        #[arg(long)]
        explain: bool,
        /// Print phase timings to stderr.
        #[arg(long)]
        profile: bool,
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
        /// Print generated SQL/script and exit without connecting to Postgres.
        #[arg(long)]
        dry_run_sql: bool,
        /// Run EXPLAIN for destructive statements inside a rollback transaction.
        #[arg(long)]
        explain: bool,
        /// Max rows per insert/COPY chunk.
        #[arg(long, default_value_t = 1000)]
        chunk_size: usize,
        /// Materialization load strategy.
        #[arg(long, value_enum, default_value_t = LoadStrategy::Jsonb)]
        load_strategy: LoadStrategy,
        /// Print phase timings to stderr.
        #[arg(long)]
        profile: bool,
    },
    /// Validate manifest shape and optionally live Postgres schema readiness.
    Doctor {
        /// Path to config/apps/<app>.json.
        #[arg(long)]
        app: PathBuf,
        /// Local replay database URL. If omitted, uses the manifest replay DB env var when set.
        #[arg(long, default_value = "")]
        db_url: String,
        #[arg(long, env = "REPLAY_ENV_PSQL_COMMAND", default_value = "psql")]
        psql_command: String,
        /// Treat warnings as failures.
        #[arg(long)]
        strict: bool,
    },
    /// Materialize a capsule and execute manifest-defined commands/probes.
    Run {
        /// Path to config/apps/<app>.json.
        #[arg(long)]
        app: PathBuf,
        /// Optional capsule to materialize before commands/probes.
        #[arg(long)]
        capsule: Option<PathBuf>,
        #[arg(long, default_value = "")]
        db_url: String,
        /// Rewrite subject key=value before materializing. Repeatable.
        #[arg(long)]
        rewrite_subject: Vec<String>,
        /// Rewrite the subject to the app manifest's localSubject values.
        #[arg(long)]
        use_local_subject: bool,
        /// Skip materialization even when --capsule is provided.
        #[arg(long)]
        skip_materialize: bool,
        #[arg(long, env = "REPLAY_ENV_PSQL_COMMAND", default_value = "psql")]
        psql_command: String,
        /// Max rows per insert/COPY chunk during materialization.
        #[arg(long, default_value_t = 1000)]
        chunk_size: usize,
        /// Materialization load strategy.
        #[arg(long, value_enum, default_value_t = LoadStrategy::Jsonb)]
        load_strategy: LoadStrategy,
        /// Write run trace JSON to this path.
        #[arg(long)]
        trace_out: Option<PathBuf>,
        /// Print phase timings to stderr.
        #[arg(long)]
        profile: bool,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LoadStrategy {
    Jsonb,
    Copy,
}

#[derive(Clone, Copy, Debug)]
enum MaterializeSqlMode {
    Execute,
    Explain,
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
    commands: Vec<RunCommandConfig>,
    #[serde(default)]
    probes: Vec<ProbeConfig>,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunCommandConfig {
    name: String,
    run: String,
    cwd: Option<String>,
    #[serde(default)]
    background: bool,
    #[serde(default)]
    allow_failure: bool,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeConfig {
    name: Option<String>,
    kind: String,
    url: Option<String>,
    command: Option<String>,
    expect_status: Option<u16>,
    #[serde(default)]
    allow_failure: bool,
    timeout_seconds: Option<u64>,
}

#[derive(Default)]
struct Profile {
    enabled: bool,
    started_at: Option<Instant>,
    events: Vec<(String, Duration)>,
}

struct ExportOptions {
    app_path: PathBuf,
    subject_args: Vec<String>,
    db_url_arg: String,
    out: PathBuf,
    since_days: u32,
    redaction: RedactionMode,
    psql_command: String,
    dry_run_sql: bool,
    explain: bool,
    profile_enabled: bool,
}

struct MaterializeOptions {
    app_path: PathBuf,
    db_url_arg: String,
    capsule_path: PathBuf,
    rewrite_subject_args: Vec<String>,
    use_local_subject: bool,
    psql_command: String,
    dry_run_sql: bool,
    explain: bool,
    chunk_size: usize,
    load_strategy: LoadStrategy,
    profile_enabled: bool,
}

struct RunOptions {
    app_path: PathBuf,
    capsule_path: Option<PathBuf>,
    db_url: String,
    rewrite_subject: Vec<String>,
    use_local_subject: bool,
    skip_materialize: bool,
    psql_command: String,
    chunk_size: usize,
    load_strategy: LoadStrategy,
    trace_out: Option<PathBuf>,
    profile_enabled: bool,
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
            dry_run_sql,
            explain,
            profile,
        } => export_postgres(ExportOptions {
            app_path: app,
            subject_args: subject,
            db_url_arg: db_url,
            out,
            since_days,
            redaction,
            psql_command,
            dry_run_sql,
            explain,
            profile_enabled: profile,
        }),
        Commands::MaterializePostgres {
            app,
            db_url,
            capsule,
            rewrite_subject,
            use_local_subject,
            psql_command,
            dry_run_sql,
            explain,
            chunk_size,
            load_strategy,
            profile,
        } => materialize_postgres(MaterializeOptions {
            app_path: app,
            db_url_arg: db_url,
            capsule_path: capsule,
            rewrite_subject_args: rewrite_subject,
            use_local_subject,
            psql_command,
            dry_run_sql,
            explain,
            chunk_size,
            load_strategy,
            profile_enabled: profile,
        }),
        Commands::Doctor {
            app,
            db_url,
            psql_command,
            strict,
        } => doctor(&app, db_url, &psql_command, strict),
        Commands::Run {
            app,
            capsule,
            db_url,
            rewrite_subject,
            use_local_subject,
            skip_materialize,
            psql_command,
            chunk_size,
            load_strategy,
            trace_out,
            profile,
        } => run_replay(RunOptions {
            app_path: app,
            capsule_path: capsule,
            db_url,
            rewrite_subject,
            use_local_subject,
            skip_materialize,
            psql_command,
            chunk_size,
            load_strategy,
            trace_out,
            profile_enabled: profile,
        }),
        Commands::Inspect { capsule } => {
            let capsule = load_capsule(&capsule)?;
            println!("{}", summary_text(&capsule));
            Ok(())
        }
        Commands::Playbook { app } => print_playbook(&app),
    }
}

fn export_postgres(options: ExportOptions) -> Result<()> {
    let mut profile = Profile::new(options.profile_enabled);
    profile.start("load manifest");
    let app = load_app_config(&options.app_path)?;
    profile.finish("load manifest");
    profile.start("validate subject");
    let subject = parse_kv_pairs(options.subject_args)?;
    validate_subject(&app, &subject)?;
    profile.finish("validate subject");
    let env_name = app
        .postgres
        .prod_database_url_env
        .as_deref()
        .unwrap_or("APP_PROD_DATABASE_URL");

    profile.start("build export sql");
    let sql = build_postgres_export_sql(&app, &subject, options.since_days)?;
    profile.finish("build export sql");

    if options.dry_run_sql {
        print!("{sql}");
        profile.print();
        return Ok(());
    }

    let db_url = options.db_url_arg.trim().to_string().or_else_env(env_name);
    require_db_url(&db_url, env_name, &options.psql_command)?;
    if options.explain {
        profile.start("explain export sql");
        let stdout = run_psql(&db_url, &explain_sql(&sql), &options.psql_command)?;
        profile.finish("explain export sql");
        print!("{stdout}");
        profile.print();
        return Ok(());
    }

    profile.start("run export sql");
    let stdout = run_psql(&db_url, &sql, &options.psql_command)?;
    profile.finish("run export sql");
    profile.start("parse export json");
    let mut document: Value =
        serde_json::from_str(stdout.trim()).context("parse psql export JSON output")?;
    profile.finish("parse export json");

    if matches!(options.redaction, RedactionMode::Safe) {
        profile.start("apply redactions");
        apply_redactions(&mut document, &app.redaction_rules);
        profile.finish("apply redactions");
    }
    object_mut(&mut document, "capsule")?.insert(
        "redaction".to_string(),
        Value::String(
            match options.redaction {
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

    if let Some(parent) = options
        .out
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    profile.start("write capsule");
    fs::write(
        &options.out,
        format!("{}\n", serde_json::to_string_pretty(&document)?),
    )
    .with_context(|| format!("write capsule {}", options.out.display()))?;
    profile.finish("write capsule");
    println!("wrote {}", options.out.display());
    println!("{}", summary_text(&document));
    profile.print();
    Ok(())
}

fn materialize_postgres(options: MaterializeOptions) -> Result<()> {
    let mut profile = Profile::new(options.profile_enabled);
    profile.start("load manifest");
    let app = load_app_config(&options.app_path)?;
    profile.finish("load manifest");
    let env_name = app
        .postgres
        .replay_database_url_env
        .as_deref()
        .unwrap_or("APP_REPLAY_DATABASE_URL");

    profile.start("load capsule");
    let mut capsule = load_capsule(&options.capsule_path)?;
    require_capsule_for_app(&capsule, &app)?;
    profile.finish("load capsule");

    profile.start("rewrite subject");
    let mut rewrite_values = parse_kv_pairs(options.rewrite_subject_args)?;
    if options.use_local_subject {
        rewrite_values.extend(app.local_subject.clone());
    }
    if !rewrite_values.is_empty() {
        capsule = rewrite_subject_scope(capsule, &rewrite_values)?;
    }
    profile.finish("rewrite subject");

    let sql_mode = if options.explain {
        MaterializeSqlMode::Explain
    } else {
        MaterializeSqlMode::Execute
    };
    profile.start("build materialize sql");
    let sql = build_postgres_materialize_sql(
        &app,
        &capsule,
        options.chunk_size,
        options.load_strategy,
        sql_mode,
    )?;
    profile.finish("build materialize sql");

    if options.dry_run_sql {
        print!("{sql}");
        profile.print();
        return Ok(());
    }

    let db_url = options.db_url_arg.trim().to_string().or_else_env(env_name);
    require_db_url(&db_url, env_name, &options.psql_command)?;
    profile.start(if options.explain {
        "explain materialize sql"
    } else {
        "run materialize sql"
    });
    run_psql(&db_url, &sql, &options.psql_command)?;
    profile.finish(if options.explain {
        "explain materialize sql"
    } else {
        "run materialize sql"
    });
    if options.explain {
        println!("explained {}", options.capsule_path.display());
        profile.print();
        return Ok(());
    }
    println!("materialized {}", options.capsule_path.display());
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
    profile.print();
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
1. Check the manifest and local replay database:\n   \
replay-env doctor --app {app_path} --db-url \"{local_db}\"\n\n\
2. Export a scoped production subject graph:\n   \
replay-env export-postgres --app {app_path} {subject_args} --out capsules/{app_id}/<capsule>.json\n\n\
3. Start or migrate the local app database using the app's normal dev flow:\n   \
cd {repo_path}\n   # use local database: {local_db}\n\n\
4. Materialize the capsule into the local replay database:\n   \
replay-env materialize-postgres --app {app_path} --db-url \"{local_db}\" --capsule capsules/{app_id}/<capsule>.json {local_subject}\n\n\
5. Run the manifest-defined replay probes and write a trace:\n   \
replay-env run --app {app_path} --db-url \"{local_db}\" --capsule capsules/{app_id}/<capsule>.json {local_subject} --trace-out capsules/{app_id}/<capsule>-trace.json --profile",
        app_id = app.app_id,
        app_path = app_path.display(),
    );
    Ok(())
}

fn doctor(app_path: &Path, db_url_arg: String, psql_command: &str, strict: bool) -> Result<()> {
    let app = load_app_config(app_path)?;
    let mut checks = Vec::new();
    checks.push(DoctorCheck::pass(
        "manifest",
        format!("{} uses {}", app.app_id, app.adapter),
    ));
    checks.push(DoctorCheck::pass(
        "subject keys",
        app.subject_keys.join(", "),
    ));
    checks.push(DoctorCheck::pass(
        "tables",
        format!("{} declared", app.postgres.tables.len()),
    ));
    if app.redaction_rules.is_empty() {
        checks.push(DoctorCheck::warn(
            "redaction",
            "no redactionRules declared; safe exports will not redact fields",
        ));
    } else {
        checks.push(DoctorCheck::pass(
            "redaction",
            format!("{} rules declared", app.redaction_rules.len()),
        ));
    }
    if app.commands.is_empty() && app.probes.is_empty() {
        checks.push(DoctorCheck::warn(
            "run loop",
            "no commands or probes declared; replay-env run will only materialize capsules",
        ));
    } else {
        checks.push(DoctorCheck::pass(
            "run loop",
            format!(
                "{} commands, {} probes",
                app.commands.len(),
                app.probes.len()
            ),
        ));
    }

    let psql_parts = split_command(psql_command)?;
    if psql_parts.is_empty() {
        checks.push(DoctorCheck::fail("psql", "empty psql command"));
    } else if command_exists(&psql_parts[0]) {
        checks.push(DoctorCheck::pass(
            "psql",
            format!("{} is available", psql_parts[0]),
        ));
    } else {
        checks.push(DoctorCheck::warn(
            "psql",
            format!("{} is not on PATH", psql_parts[0]),
        ));
    }

    let replay_env = app
        .postgres
        .replay_database_url_env
        .as_deref()
        .unwrap_or("APP_REPLAY_DATABASE_URL");
    let db_url = db_url_arg.trim().to_string().or_else_env(replay_env);
    if db_url.is_empty() {
        checks.push(DoctorCheck::warn(
            "database",
            format!("live schema checks skipped; pass --db-url or set {replay_env}"),
        ));
    } else {
        if looks_production_url(&db_url) {
            checks.push(DoctorCheck::fail(
                "database safety",
                "database URL looks production-like; doctor expects a local/staging replay target",
            ));
        } else {
            checks.push(DoctorCheck::pass(
                "database safety",
                "database URL does not look production-like",
            ));
        }
        match run_live_schema_checks(&app, &db_url, psql_command) {
            Ok(mut live_checks) => checks.append(&mut live_checks),
            Err(error) => checks.push(DoctorCheck::fail("database schema", error.to_string())),
        }
    }

    let failures = checks
        .iter()
        .filter(|check| matches!(check.level, DoctorLevel::Fail))
        .count();
    let warnings = checks
        .iter()
        .filter(|check| matches!(check.level, DoctorLevel::Warn))
        .count();
    for check in &checks {
        println!(
            "[{}] {}: {}",
            check.level.as_str(),
            check.name,
            check.detail
        );
    }
    println!("summary: {failures} failures, {warnings} warnings");
    if failures > 0 || (strict && warnings > 0) {
        bail!("doctor checks did not pass");
    }
    Ok(())
}

fn run_replay(options: RunOptions) -> Result<()> {
    let mut profile = Profile::new(options.profile_enabled);
    profile.start("load manifest");
    let app = load_app_config(&options.app_path)?;
    profile.finish("load manifest");
    let mut trace = json!({
        "schemaVersion": 1,
        "kind": "replay-env-run-trace",
        "app": app.app_id,
        "startedAt": Utc::now().to_rfc3339(),
        "events": []
    });
    let mut failed = false;

    if let Some(capsule_path) = options
        .capsule_path
        .as_deref()
        .filter(|_| !options.skip_materialize)
    {
        profile.start("materialize capsule");
        let event = timed_event("materialize", || {
            materialize_postgres(MaterializeOptions {
                app_path: options.app_path.clone(),
                db_url_arg: options.db_url.clone(),
                capsule_path: capsule_path.to_path_buf(),
                rewrite_subject_args: options.rewrite_subject.clone(),
                use_local_subject: options.use_local_subject,
                psql_command: options.psql_command.clone(),
                dry_run_sql: false,
                explain: false,
                chunk_size: options.chunk_size,
                load_strategy: options.load_strategy,
                profile_enabled: false,
            })
        });
        profile.finish("materialize capsule");
        failed |= !event_success(&event);
        push_trace_event(&mut trace, event)?;
    }

    let repo_path = app.repo_path.as_deref().map(PathBuf::from);
    let mut background = Vec::new();
    for command in &app.commands {
        if command.background {
            profile.start(format!("start {}", command.name));
            let BackgroundStart {
                event,
                child,
                failed: command_failed,
            } = start_background_command(command, repo_path.as_deref());
            profile.finish(format!("start {}", command.name));
            if let Ok(child) = child {
                background.push((command.name.clone(), child));
            }
            failed |= command_failed && !command.allow_failure;
            push_trace_event(&mut trace, event)?;
        } else {
            profile.start(format!("run {}", command.name));
            let event = run_command_event(command, repo_path.as_deref());
            profile.finish(format!("run {}", command.name));
            failed |= !event_success(&event) && !command.allow_failure;
            push_trace_event(&mut trace, event)?;
        }
    }

    for probe in &app.probes {
        profile.start(format!("probe {}", probe_name(probe)));
        let event = run_probe_event(probe, repo_path.as_deref());
        profile.finish(format!("probe {}", probe_name(probe)));
        failed |= !event_success(&event) && !probe.allow_failure;
        push_trace_event(&mut trace, event)?;
    }

    for (name, mut child) in background {
        let _ = child.kill();
        let _ = child.wait();
        push_trace_event(
            &mut trace,
            json!({
                "kind": "background-stop",
                "name": name,
                "success": true,
                "at": Utc::now().to_rfc3339()
            }),
        )?;
    }

    let gate = if failed { "block" } else { "continue" };
    trace
        .as_object_mut()
        .ok_or_else(|| anyhow!("trace must be an object"))?
        .insert(
            "finishedAt".to_string(),
            Value::String(Utc::now().to_rfc3339()),
        );
    trace
        .as_object_mut()
        .ok_or_else(|| anyhow!("trace must be an object"))?
        .insert("gate".to_string(), Value::String(gate.to_string()));

    if let Some(trace_out) = options.trace_out.as_deref() {
        if let Some(parent) = trace_out
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("create trace directory {}", parent.display()))?;
        }
        fs::write(
            trace_out,
            format!("{}\n", serde_json::to_string_pretty(&trace)?),
        )
        .with_context(|| format!("write trace {}", trace_out.display()))?;
        println!("wrote trace {}", trace_out.display());
    } else {
        println!("{}", serde_json::to_string_pretty(&trace)?);
    }
    profile.print();
    if failed {
        bail!("replay run gate: block");
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum DoctorLevel {
    Pass,
    Warn,
    Fail,
}

struct DoctorCheck {
    level: DoctorLevel,
    name: String,
    detail: String,
}

impl DoctorCheck {
    fn pass(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            level: DoctorLevel::Pass,
            name: name.into(),
            detail: detail.into(),
        }
    }

    fn warn(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            level: DoctorLevel::Warn,
            name: name.into(),
            detail: detail.into(),
        }
    }

    fn fail(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            level: DoctorLevel::Fail,
            name: name.into(),
            detail: detail.into(),
        }
    }
}

impl DoctorLevel {
    fn as_str(self) -> &'static str {
        match self {
            DoctorLevel::Pass => "pass",
            DoctorLevel::Warn => "warn",
            DoctorLevel::Fail => "fail",
        }
    }
}

struct BackgroundStart {
    event: Value,
    child: Result<Child>,
    failed: bool,
}

fn run_live_schema_checks(
    app: &AppConfig,
    db_url: &str,
    psql_command: &str,
) -> Result<Vec<DoctorCheck>> {
    let sql = build_schema_check_sql(app)?;
    let stdout = run_psql(db_url, &sql, psql_command)?;
    let report: Value = serde_json::from_str(stdout.trim()).context("parse schema check output")?;
    let mut checks = Vec::new();
    let table_report = report
        .get("tables")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("schema check output missing tables"))?;
    for table in &app.postgres.tables {
        match table_report.get(&table.name).and_then(Value::as_bool) {
            Some(true) => checks.push(DoctorCheck::pass(format!("table {}", table.name), "exists")),
            _ => checks.push(DoctorCheck::fail(
                format!("table {}", table.name),
                "missing from replay database",
            )),
        }
    }
    let columns = report
        .get("columns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut missing_columns = Vec::new();
    for column in columns {
        let table = column.get("table").and_then(Value::as_str).unwrap_or("");
        let name = column.get("column").and_then(Value::as_str).unwrap_or("");
        if !column
            .get("exists")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            missing_columns.push(format!("{table}.{name}"));
        }
    }
    if missing_columns.is_empty() {
        checks.push(DoctorCheck::pass(
            "referenced columns",
            "manifest alias/delete columns exist",
        ));
    } else {
        checks.push(DoctorCheck::fail(
            "referenced columns",
            format!("missing: {}", missing_columns.join(", ")),
        ));
    }
    Ok(checks)
}

fn build_schema_check_sql(app: &AppConfig) -> Result<String> {
    let table_values = app
        .postgres
        .tables
        .iter()
        .map(|table| format!("({})", sql_literal(&table.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let mut column_pairs = BTreeSet::new();
    for table in &app.postgres.tables {
        for column in referenced_columns(table) {
            column_pairs.insert((table.name.clone(), column));
        }
    }
    let column_values = if column_pairs.is_empty() {
        "select null::text as table_name, null::text as column_name where false".to_string()
    } else {
        format!(
            "values {}",
            column_pairs
                .iter()
                .map(|(table, column)| format!("({}, {})", sql_literal(table), sql_literal(column)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    Ok(format!(
        "\
with expected_tables(table_name) as (
    values {table_values}
),
expected_columns(table_name, column_name) as (
    {column_values}
),
doc as (
    select jsonb_build_object(
        'tables',
        (
            select jsonb_object_agg(
                table_name,
                to_regclass(format('%I.%I', current_schema(), table_name)) is not null
            )
            from expected_tables
        ),
        'columns',
        coalesce((
            select jsonb_agg(jsonb_build_object(
                'table', e.table_name,
                'column', e.column_name,
                'exists', exists (
                    select 1
                    from information_schema.columns c
                    where c.table_schema = current_schema()
                      and c.table_name = e.table_name
                      and c.column_name = e.column_name
                )
            ) order by e.table_name, e.column_name)
            from expected_columns e
        ), '[]'::jsonb)
    ) as payload
)
select payload::text from doc;
"
    ))
}

fn referenced_columns(table: &TableConfig) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    let alias = table.alias.as_deref().unwrap_or("t");
    for sql in [
        table.from.as_deref(),
        table.row_sql.as_deref(),
        table.where_sql.as_deref(),
        table.order_by.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        columns.extend(extract_alias_columns(sql, alias));
    }
    if let Some(predicate) = table.delete_predicate.as_deref() {
        columns.extend(extract_alias_columns(predicate, "t"));
    }
    columns
}

fn extract_alias_columns(sql: &str, alias: &str) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    let needle = format!("{alias}.");
    let mut rest = sql;
    while let Some(index) = rest.find(&needle) {
        let after = &rest[index + needle.len()..];
        let column = after
            .chars()
            .take_while(|ch| *ch == '_' || ch.is_ascii_alphanumeric())
            .collect::<String>();
        if !column.is_empty() {
            columns.insert(column);
        }
        rest = after;
    }
    columns
}

fn looks_production_url(db_url: &str) -> bool {
    let lowered = db_url.to_ascii_lowercase();
    lowered.contains("prod")
        || lowered.contains("production")
        || lowered.contains("render.com")
        || lowered.contains("neon.tech")
        || lowered.contains("supabase.co")
        || lowered.contains("rds.amazonaws.com")
}

fn timed_event<F>(name: &str, operation: F) -> Value
where
    F: FnOnce() -> Result<()>,
{
    let started = Instant::now();
    match operation() {
        Ok(()) => json!({
            "kind": name,
            "success": true,
            "durationMs": duration_ms(started.elapsed()),
            "at": Utc::now().to_rfc3339()
        }),
        Err(error) => json!({
            "kind": name,
            "success": false,
            "durationMs": duration_ms(started.elapsed()),
            "error": error.to_string(),
            "at": Utc::now().to_rfc3339()
        }),
    }
}

fn run_command_event(command: &RunCommandConfig, repo_path: Option<&Path>) -> Value {
    let started = Instant::now();
    match run_shell_command(
        &command.run,
        command.cwd.as_deref().map(Path::new).or(repo_path),
        command.timeout_seconds,
    ) {
        Ok(outcome) => json!({
            "kind": "command",
            "name": command.name,
            "command": command.run,
            "success": outcome.success,
            "status": outcome.status,
            "durationMs": duration_ms(started.elapsed()),
            "stdout": truncate_trace_text(&outcome.stdout),
            "stderr": truncate_trace_text(&outcome.stderr),
            "at": Utc::now().to_rfc3339()
        }),
        Err(error) => json!({
            "kind": "command",
            "name": command.name,
            "command": command.run,
            "success": false,
            "durationMs": duration_ms(started.elapsed()),
            "error": error.to_string(),
            "at": Utc::now().to_rfc3339()
        }),
    }
}

fn start_background_command(
    command: &RunCommandConfig,
    repo_path: Option<&Path>,
) -> BackgroundStart {
    let started = Instant::now();
    let cwd = command.cwd.as_deref().map(Path::new).or(repo_path);
    let mut shell = shell_command(&command.run);
    if let Some(cwd) = cwd {
        shell.current_dir(cwd);
    }
    match shell
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => BackgroundStart {
            event: json!({
                "kind": "background-start",
                "name": command.name,
                "command": command.run,
                "success": true,
                "pid": child.id(),
                "durationMs": duration_ms(started.elapsed()),
                "at": Utc::now().to_rfc3339()
            }),
            child: Ok(child),
            failed: false,
        },
        Err(error) => BackgroundStart {
            event: json!({
                "kind": "background-start",
                "name": command.name,
                "command": command.run,
                "success": false,
                "durationMs": duration_ms(started.elapsed()),
                "error": error.to_string(),
                "at": Utc::now().to_rfc3339()
            }),
            child: Err(error.into()),
            failed: true,
        },
    }
}

fn run_probe_event(probe: &ProbeConfig, repo_path: Option<&Path>) -> Value {
    let started = Instant::now();
    match probe.kind.as_str() {
        "http" => match probe.url.as_deref() {
            Some(url) => match http_get_status(url, probe.timeout_seconds.unwrap_or(10)) {
                Ok(status) => {
                    let expect = probe.expect_status.unwrap_or(200);
                    json!({
                        "kind": "probe",
                        "probeKind": "http",
                        "name": probe_name(probe),
                        "url": url,
                        "expectStatus": expect,
                        "status": status,
                        "success": status == expect,
                        "durationMs": duration_ms(started.elapsed()),
                        "at": Utc::now().to_rfc3339()
                    })
                }
                Err(error) => json!({
                    "kind": "probe",
                    "probeKind": "http",
                    "name": probe_name(probe),
                    "url": url,
                    "success": false,
                    "durationMs": duration_ms(started.elapsed()),
                    "error": error.to_string(),
                    "at": Utc::now().to_rfc3339()
                }),
            },
            None => json!({
                "kind": "probe",
                "probeKind": "http",
                "name": probe_name(probe),
                "success": false,
                "durationMs": duration_ms(started.elapsed()),
                "error": "http probe missing url",
                "at": Utc::now().to_rfc3339()
            }),
        },
        "command" => {
            let command = RunCommandConfig {
                name: probe_name(probe),
                run: probe.command.clone().unwrap_or_default(),
                cwd: None,
                background: false,
                allow_failure: probe.allow_failure,
                timeout_seconds: probe.timeout_seconds,
            };
            if command.run.is_empty() {
                json!({
                    "kind": "probe",
                    "probeKind": "command",
                    "name": command.name,
                    "success": false,
                    "durationMs": duration_ms(started.elapsed()),
                    "error": "command probe missing command",
                    "at": Utc::now().to_rfc3339()
                })
            } else {
                run_command_event(&command, repo_path)
            }
        }
        other => json!({
            "kind": "probe",
            "probeKind": other,
            "name": probe_name(probe),
            "success": false,
            "durationMs": duration_ms(started.elapsed()),
            "error": format!("unsupported probe kind: {other}"),
            "at": Utc::now().to_rfc3339()
        }),
    }
}

struct CommandOutcome {
    success: bool,
    status: Option<i32>,
    stdout: String,
    stderr: String,
}

fn run_shell_command(
    command: &str,
    cwd: Option<&Path>,
    timeout_seconds: Option<u64>,
) -> Result<CommandOutcome> {
    let mut child_command = shell_command(command);
    if let Some(cwd) = cwd {
        child_command.current_dir(cwd);
    }
    let mut child = child_command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn command {command:?}"))?;
    if let Some(timeout_seconds) = timeout_seconds {
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
        loop {
            if child.try_wait()?.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output()?;
                return Ok(CommandOutcome {
                    success: false,
                    status: output.status.code(),
                    stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                    stderr: format!(
                        "{}\ncommand timed out after {timeout_seconds}s",
                        String::from_utf8_lossy(&output.stderr)
                    ),
                });
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
    let output = child.wait_with_output()?;
    Ok(CommandOutcome {
        success: output.status.success(),
        status: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn shell_command(command: &str) -> Command {
    #[cfg(windows)]
    {
        let mut cmd = Command::new("cmd");
        cmd.args(["/C", command]);
        cmd
    }
    #[cfg(not(windows))]
    {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", command]);
        cmd
    }
}

fn http_get_status(url: &str, timeout_seconds: u64) -> Result<u16> {
    let stripped = url.strip_prefix("http://").ok_or_else(|| {
        anyhow!("only http:// probes are supported without curl/browser integration")
    })?;
    let (host_port, path) = stripped
        .split_once('/')
        .map(|(host, path)| (host, format!("/{path}")))
        .unwrap_or((stripped, "/".to_string()));
    let (host, port) = host_port
        .rsplit_once(':')
        .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host, port)))
        .unwrap_or((host_port, 80));
    let address = (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("could not resolve {host}:{port}"))?;
    let timeout = Duration::from_secs(timeout_seconds);
    let mut stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| anyhow!("could not parse HTTP status from response"))?;
    Ok(status)
}

fn event_success(event: &Value) -> bool {
    event
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn push_trace_event(trace: &mut Value, event: Value) -> Result<()> {
    trace
        .get_mut("events")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow!("trace events must be an array"))?
        .push(event);
    Ok(())
}

fn probe_name(probe: &ProbeConfig) -> String {
    probe.name.clone().unwrap_or_else(|| {
        probe
            .url
            .clone()
            .or_else(|| probe.command.clone())
            .unwrap_or_default()
    })
}

fn duration_ms(duration: Duration) -> u128 {
    duration.as_millis()
}

fn truncate_trace_text(value: &str) -> String {
    const LIMIT: usize = 16_384;
    if value.len() > LIMIT {
        format!(
            "{}...[truncated]",
            value.chars().take(LIMIT).collect::<String>()
        )
    } else {
        value.to_string()
    }
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
    for command in &app.commands {
        if command.name.trim().is_empty() {
            bail!("command name must not be empty");
        }
        if command.run.trim().is_empty() {
            bail!("command {} run must not be empty", command.name);
        }
    }
    for probe in &app.probes {
        match probe.kind.as_str() {
            "http" => {
                if probe.url.as_deref().is_none_or(str::is_empty) {
                    bail!("http probe must declare url");
                }
            }
            "command" => {
                if probe.command.as_deref().is_none_or(str::is_empty) {
                    bail!("command probe must declare command");
                }
            }
            other => bail!("unsupported probe kind: {other}"),
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

fn build_postgres_materialize_sql(
    app: &AppConfig,
    capsule: &Value,
    chunk_size: usize,
    load_strategy: LoadStrategy,
    mode: MaterializeSqlMode,
) -> Result<String> {
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
    let explain = matches!(mode, MaterializeSqlMode::Explain);
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
        let delete_sql =
            format!("delete from {table_name} t using replay_graph g where {predicate};");
        if explain {
            parts.push(format!("explain {delete_sql}"));
        } else {
            parts.push(delete_sql);
        }
    }
    for table_name in table_order {
        let Some(rows) = capsule_tables.get(&table_name).and_then(Value::as_array) else {
            continue;
        };
        if rows.is_empty() {
            continue;
        }
        let chunk_size = chunk_size.max(1);
        for chunk in rows.chunks(chunk_size) {
            match (load_strategy, mode) {
                (LoadStrategy::Copy, MaterializeSqlMode::Execute) => {
                    parts.push(build_copy_script(&table_name, chunk)?);
                }
                _ => {
                    let insert_sql = format!(
                        "insert into {table_name} select * from jsonb_populate_recordset(null::{table_name}, {}::jsonb);",
                        sql_json_literal(&Value::Array(chunk.to_vec()))?
                    );
                    if explain {
                        parts.push(format!("explain {insert_sql}"));
                    } else {
                        parts.push(insert_sql);
                    }
                }
            }
        }
    }
    if explain {
        parts.push("rollback;".to_string());
    } else {
        parts.push("commit;".to_string());
    }
    Ok(parts.join("\n"))
}

fn build_copy_script(table_name: &str, rows: &[Value]) -> Result<String> {
    let columns = copy_columns(rows);
    if columns.is_empty() {
        return Ok(format!("-- skipped {table_name}: no object columns"));
    }
    let column_sql = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut lines = vec![format!(
        "copy {table_name} ({column_sql}) from stdin with (format csv, null '\\N');"
    )];
    for row in rows {
        let object = row
            .as_object()
            .ok_or_else(|| anyhow!("COPY materialization requires object rows for {table_name}"))?;
        let values = columns
            .iter()
            .map(|column| copy_value(object.get(column)))
            .collect::<Result<Vec<_>>>()?
            .join(",");
        lines.push(values);
    }
    lines.push("\\.".to_string());
    Ok(lines.join("\n"))
}

fn copy_columns(rows: &[Value]) -> Vec<String> {
    let mut columns = BTreeSet::new();
    for row in rows {
        if let Some(object) = row.as_object() {
            columns.extend(object.keys().cloned());
        }
    }
    columns.into_iter().collect()
}

fn copy_value(value: Option<&Value>) -> Result<String> {
    let Some(value) = value else {
        return Ok("\\N".to_string());
    };
    if value.is_null() {
        return Ok("\\N".to_string());
    }
    let raw = match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)?,
        Value::Null => "\\N".to_string(),
    };
    Ok(csv_escape(&raw))
}

fn csv_escape(value: &str) -> String {
    if value == "\\N" || value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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
    String::from_utf8(output.stdout).context("decode psql stdout as UTF-8")
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

fn explain_sql(sql: &str) -> String {
    let trimmed = sql.trim().trim_end_matches(';');
    format!("explain {trimmed};\n")
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

impl Profile {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            started_at: None,
            events: Vec::new(),
        }
    }

    fn start(&mut self, _name: impl Into<String>) {
        if self.enabled {
            self.started_at = Some(Instant::now());
        }
    }

    fn finish(&mut self, name: impl Into<String>) {
        if !self.enabled {
            return;
        }
        if let Some(started_at) = self.started_at.take() {
            self.events.push((name.into(), started_at.elapsed()));
        }
    }

    fn print(&self) {
        if !self.enabled {
            return;
        }
        eprintln!("profile:");
        for (name, duration) in &self.events {
            eprintln!("  {name}: {}ms", duration_ms(*duration));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_app() -> AppConfig {
        AppConfig {
            app_id: "demo".to_string(),
            adapter: "postgres-subject-graph.v1".to_string(),
            repo_path: None,
            subject_keys: vec!["tenant_id".to_string(), "user_id".to_string()],
            local_subject: HashMap::new(),
            local_database_url: None,
            postgres: PostgresConfig {
                prod_database_url_env: None,
                replay_database_url_env: None,
                graph_edges: Vec::new(),
                table_order: vec!["profiles".to_string()],
                tables: vec![TableConfig {
                    name: "profiles".to_string(),
                    alias: Some("p".to_string()),
                    from: None,
                    row_sql: None,
                    where_sql: None,
                    order_by: None,
                    delete_predicate: Some(
                        "t.tenant_id = g.tenant_id and t.user_id = g.user_id".to_string(),
                    ),
                }],
            },
            commands: Vec::new(),
            probes: Vec::new(),
            redaction_rules: Vec::new(),
        }
    }

    fn demo_capsule() -> Value {
        json!({
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
                    {"tenant_id": "tenant-prod", "user_id": "user-1", "display_name": "One"},
                    {"tenant_id": "tenant-prod", "user_id": "user-2", "display_name": "Two"},
                    {"tenant_id": "tenant-prod", "user_id": "user-3", "display_name": "Three"}
                ]
            }
        })
    }

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
        let capsule = demo_capsule();
        let new_subject = HashMap::from([
            ("tenant_id".to_string(), "tenant-local".to_string()),
            ("user_id".to_string(), "user-local".to_string()),
        ]);

        let rewritten = rewrite_subject_scope(capsule, &new_subject).unwrap();

        assert_eq!(rewritten["subject"]["tenant_id"], "tenant-local");
        assert_eq!(rewritten["subject"]["user_id"], "user-local");
        assert_eq!(rewritten["graph"][0]["tenant_id"], "tenant-local");
        assert_eq!(rewritten["graph"][0]["user_id"], "user-local");
        assert_eq!(rewritten["tables"]["profiles"][0]["display_name"], "One");
        assert!(rewritten["materialization"]["subjectScopeRewrite"].is_object());
    }

    #[test]
    fn materialize_sql_chunks_jsonb_inserts() {
        let sql = build_postgres_materialize_sql(
            &demo_app(),
            &demo_capsule(),
            2,
            LoadStrategy::Jsonb,
            MaterializeSqlMode::Execute,
        )
        .unwrap();

        assert_eq!(sql.matches("insert into profiles select").count(), 2);
        assert!(sql.contains("commit;"));
    }

    #[test]
    fn materialize_sql_can_emit_copy_script() {
        let sql = build_postgres_materialize_sql(
            &demo_app(),
            &demo_capsule(),
            2,
            LoadStrategy::Copy,
            MaterializeSqlMode::Execute,
        )
        .unwrap();

        assert_eq!(sql.matches("copy profiles").count(), 2);
        assert!(sql.contains("\\."));
        assert!(sql.contains("\"display_name\""));
    }

    #[test]
    fn materialize_explain_rolls_back() {
        let sql = build_postgres_materialize_sql(
            &demo_app(),
            &demo_capsule(),
            2,
            LoadStrategy::Jsonb,
            MaterializeSqlMode::Explain,
        )
        .unwrap();

        assert!(sql.contains("explain delete from profiles"));
        assert!(sql.contains("explain insert into profiles"));
        assert!(sql.contains("rollback;"));
    }
}
