use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use kvs::{KvsClient, Request, Response};

const DEFAULT_SOCKET_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000);

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
    Set {
        key: String,
        value: String,
        #[arg(long, default_value_t = DEFAULT_SOCKET_ADDR)]
        addr: SocketAddr,
    },
    Get {
        key: String,
        #[arg(long, default_value_t = DEFAULT_SOCKET_ADDR)]
        addr: SocketAddr,
    },
    Rm {
        key: String,
        #[arg(long, default_value_t = DEFAULT_SOCKET_ADDR)]
        addr: SocketAddr,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut is_remove = false;
    let (addr, request) = match cli.commands {
        Commands::Set { key, value, addr } => (addr, Request::Set(key, value)),
        Commands::Get { key, addr } => (addr, Request::Get(key)),
        Commands::Rm { key, addr } => {
            is_remove = true;
            (addr, Request::Rm(key))
        }
    };
    let mut client = KvsClient::new(addr);
    let response = client.request(request)?;
    match response {
        Response::Value(value) => {
            println!("{value}")
        }
        Response::NoKey => {
            println!("Key not found");
            if is_remove {
                return Err(anyhow!("Key not found"));
            };
        }
        Response::Ok => {}
        Response::Err => {
            return Err(anyhow!("Server internal error"));
        }
    }
    Ok(())
}
