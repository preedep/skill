mod dependencies;
mod mapper;
mod model;
mod parser;
mod renderer;
mod scheduler;
mod substitution;

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use renderer::RenderContext;

#[derive(Parser, Debug)]
#[command(
    name = "controlm2airflow",
    about = "Convert Control-M job XML exports to Apache Airflow 3.x DAG Python files",
    version
)]
struct Args {
    /// Input Control-M XML file
    #[arg(short, long)]
    input: PathBuf,

    /// Output directory for generated DAG files
    #[arg(short, long, default_value = "output")]
    output: PathBuf,

    /// Company name (lowercased in output)
    #[arg(long)]
    company: String,

    /// Application ID (e.g. APP1234)
    #[arg(long)]
    app_id: String,

    /// Application code (e.g. TESTAPP)
    #[arg(long)]
    app_code: String,

    /// Environment (e.g. dev, sit, prod)
    #[arg(long, default_value = "dev")]
    env: String,

    /// Only process folders matching this name pattern (substring match)
    #[arg(long)]
    folder_filter: Option<String>,

    /// Verify generated DAG syntax after each file
    #[arg(long, default_value_t = false)]
    verify: bool,

    /// Python interpreter to use for verification (default: .venv/bin/python)
    #[arg(long, default_value = ".venv/bin/python")]
    python: String,
}

fn verify_dag(path: &Path, python: &str) -> Result<(), String> {
    let output = std::process::Command::new(python)
        .arg(path)
        .output()
        .map_err(|e| format!("Failed to run python: {}", e))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
    }
}

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let xml = fs::read_to_string(&args.input)
        .with_context(|| format!("Failed to read input file: {}", args.input.display()))?;

    let folders = parser::parse(&xml)
        .with_context(|| "Failed to parse Control-M XML")?;

    fs::create_dir_all(&args.output)
        .with_context(|| format!("Failed to create output directory: {}", args.output.display()))?;

    // Build global job→folder map for ExternalTaskSensor DAG ID derivation
    let job_folder_map: HashMap<String, String> = folders
        .iter()
        .flat_map(|f| {
            let fname = f.folder_name.clone();
            f.jobs.iter().map(move |j| (j.jobname.to_uppercase(), fname.clone()))
        })
        .collect();

    let ctx = RenderContext {
        company: args.company.clone(),
        app_id: args.app_id.clone(),
        app_code: args.app_code.clone(),
        env: args.env.clone(),
        job_folder_map,
    };

    let total = folders.len();
    let mut generated = 0;
    let mut skipped = 0;
    let mut verify_failed = 0;

    for folder in &folders {
        if folder.folder_name.is_empty() {
            skipped += 1;
            continue;
        }

        if let Some(filter) = &args.folder_filter {
            if !folder.folder_name.to_uppercase().contains(&filter.to_uppercase()) {
                skipped += 1;
                continue;
            }
        }

        let dag_filename = format!(
            "{}-{}-{}-{}-{}.py",
            args.company.to_lowercase(),
            args.app_id.to_lowercase(),
            args.app_code.to_lowercase(),
            folder.folder_name.to_lowercase(),
            args.env.to_lowercase()
        );

        let out_path = args.output.join(&dag_filename);

        match renderer::render(folder, &ctx) {
            Ok(content) => {
                fs::write(&out_path, &content)
                    .with_context(|| format!("Failed to write {}", out_path.display()))?;

                if args.verify {
                    match verify_dag(&out_path, &args.python) {
                        Ok(()) => println!("[OK]   {} ({} jobs)", dag_filename, folder.jobs.len()),
                        Err(e) => {
                            eprintln!("[FAIL] {} — {}", dag_filename, e.lines().next().unwrap_or("syntax error"));
                            verify_failed += 1;
                        }
                    }
                } else {
                    println!("[OK] {} ({} jobs)", dag_filename, folder.jobs.len());
                }
                generated += 1;
            }
            Err(e) => {
                eprintln!("[ERROR] {} — {}", folder.folder_name, e);
                skipped += 1;
            }
        }
    }

    println!(
        "\nDone: {}/{} folders generated, {} skipped",
        generated, total, skipped
    );

    if args.verify {
        let passed = generated - verify_failed;
        println!("Verify: {}/{} passed, {} failed", passed, generated, verify_failed);
        if verify_failed > 0 {
            std::process::exit(1);
        }
    }

    Ok(())
}
