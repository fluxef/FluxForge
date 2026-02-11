use clap::{Parser, Subcommand, ArgGroup};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fluxforge", version, about = "Database smithing tool")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Verbose output
    #[arg(long, short = 'v', global = true)]
    pub verbose: bool,

}

#[derive(Subcommand)]
pub enum Commands {
    /// Extract schema into internal format
    Extract {
        #[arg(long)]
        source: String,

        #[arg(long)]
        schema: PathBuf,

        #[arg(long)]
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
        #[arg(long)]
        source: Option<String>,

        /// Path to internal schema JSON file
        #[arg(long)]
        schema: Option<PathBuf>,

        /// PostgreSQL target URL
        #[arg(long)]
        target: String,

        #[arg(long)]
        config: Option<PathBuf>,

        /// Output SQL statements without executing them
        #[arg(long)]
        dry_run: bool,

        #[arg(long)]
        allow_destructive: bool,
    },
    Replicate {
        /// MySQL source URL (required for data migration)
        #[arg(long)]
        source: Option<String>,

        /// Path to internal schema JSON file
        #[arg(long)]
        schema: Option<PathBuf>,

        /// PostgreSQL target URL
        #[arg(long)]
        target: String,

        #[arg(long)]
        config: Option<PathBuf>,

        /// Output SQL statements without executing them
        #[arg(long)]
        dry_run: bool,

        #[arg(long)]
        allow_destructive: bool,
    },


}
