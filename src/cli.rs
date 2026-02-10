use clap::{Parser, Subcommand, ArgGroup};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fluxforge", version, about = "Database smithing tool")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Verbose output
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Extract schema into internal format
    Extract {
        #[arg(short, long)]
        source: String,

        #[arg(short, long)]
        schema: PathBuf,

        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Migrate structure and optionally data
    #[command(group(
        ArgGroup::new("input")
            .required(true)
            .args(["source", "schema"]),
    ))]
    Migrate {
        /// MySQL source URL (required for data migration)
        #[arg(short, long)]
        source: Option<String>,

        /// Path to internal schema JSON file
        #[arg(short, long)]
        schema: Option<PathBuf>,

        /// PostgreSQL target URL
        #[arg(short, long)]
        target: String,

        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Output SQL statements without executing them
        #[arg(long)]
        dry_run: bool,

        /// Only migrate the structure, skip data transfer
        #[arg(long)]
        schema_only: bool,
    },
}
