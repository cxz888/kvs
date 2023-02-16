use std::path::PathBuf;

use clap::{Parser, Subcommand};
use kvs::Error;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
#[command(disable_help_subcommand(true))]
pub struct Cli {
    #[arg(short, long)]
    dir: Option<String>,
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let path = cli
        .dir
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    let mut kvs = kvs::KvStore::open(path).expect("failed to open database");
    match cli.commands {
        Commands::Set { key, value } => kvs.set(key, value)?,
        Commands::Get { key } => {
            if let Some(value) = kvs.get(key)? {
                println!("{value}");
            } else {
                println!("Key not found");
            }
        }
        Commands::Rm { key } => match kvs.remove(key) {
            Ok(_) => {}
            Err(Error::RemoveNonexistKey) => {
                println!("Key not found");
                return Err(Box::new(Error::RemoveNonexistKey));
            }
            Err(e) => return Err(Box::new(e)),
        },
    }
    Ok(())
}
