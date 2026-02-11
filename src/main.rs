mod cli;
mod business;

use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    if let Err(e) = business::handle_command(args.command, args.verbose).await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
