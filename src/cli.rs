use clap::{ArgGroup, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fluxforge", version, about = "Database smithing tool")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,


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
        
        /// Verbose output
        #[arg(long, short = 'v')]
        verbose: bool,
        
    },
    /// Migrate structure and optionally data
    #[command(group(
        ArgGroup::new("input")
            .required(true)
            .args(["source", "schema"]),
    ))]
    Migrate {
        /// source DB-URL, typically MYSQL
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

        /// Verbose output
        #[arg(long, short = 'v')]
        verbose: bool,

        #[arg(long)]
        allow_destructive: bool,
    },
    Replicate {
        /// source DB-URL, typically MYSQL
        #[arg(long)]
        source: String,

        /// target DB-URL, typically PostgreSQL
        #[arg(long)]
        target: String,

        /// Config-File with transformations to apply to the schema
        #[arg(long)]
        config: Option<PathBuf>,

        /// Output SQL statements without executing them
        #[arg(long)]
        dry_run: bool,
        
        /// Verbose output
        #[arg(long, short = 'v')]
        verbose: bool,
        
        // stop data transfer if sql error writing sql data
        #[arg(long)]
        halt_on_error: bool,

        /// Verify data after each table write
        #[arg(long)]
        verify: bool,
    },
}
