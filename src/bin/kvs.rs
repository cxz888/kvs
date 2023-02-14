use clap::{Parser, Subcommand};

/// Hide message into png.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
#[command(disable_help_subcommand(true))]
pub struct Cli {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() {
    let cli = Cli::parse();
    match cli.commands {
        Commands::Set { .. } => todo!("unimplemented"),
        Commands::Get { .. } => todo!("unimplemented"),
        Commands::Rm { .. } => todo!("unimplemented"),
    }
}
