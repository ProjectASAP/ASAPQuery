use std::{fs, path::PathBuf};

use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Deserialize)]
struct Manifest {
    queries: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    id: usize,
    query: String,
    eligible: bool,
}

#[derive(Serialize)]
struct Status {
    id: usize,
    parsed: bool,
    eligible: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let manifest: Manifest = serde_json::from_str(&fs::read_to_string(args.manifest)?)?;
    let statuses = manifest
        .queries
        .into_iter()
        .map(|entry| Status {
            id: entry.id,
            parsed: promql_parser::parser::parse(&entry.query).is_ok(),
            eligible: entry.eligible,
        })
        .collect::<Vec<_>>();
    fs::write(args.output, serde_json::to_vec_pretty(&statuses)?)?;
    Ok(())
}
