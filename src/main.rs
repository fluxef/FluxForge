mod business;
mod cli;

use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    if let Err(e) = business::handle_command(args.command).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
