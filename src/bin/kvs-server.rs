#![feature(let_chains)]
#![feature(once_cell)]

use std::{
    env,
    fmt::Display,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
    thread,
};

use anyhow::{anyhow, Result};
use clap::Parser;
use env_logger::Target;
use kvs::{thread_pool::SharedQueueThreadPool, KvStore, KvsEngine, KvsServer, SledKvsEngine};

const DEFAULT_SOCKET_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000);

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
pub struct Cli {
    #[arg(short, long)]
    dir: Option<String>,
    #[arg(long, default_value_t = DEFAULT_SOCKET_ADDR)]
    addr: SocketAddr,
    #[arg(long)]
    engine: Option<String>,
}

#[derive(Debug)]
enum Engine {
    Kvs,
    Sled,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Kvs => write!(f, "kvs"),
            Engine::Sled => write!(f, "sled"),
        }
    }
}

fn main() -> Result<()> {
    env_logger::builder().target(Target::Stderr).init();
    let cli = Cli::parse();
    log::info!("cli parsing!");
    let path = cli
        .dir
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);

    let real_engine = get_real_engine(cli.engine, &path)?;

    log::info!("server version: {}", env!("CARGO_PKG_VERSION"));
    log::info!("engine name: {real_engine}",);
    log::info!("listen on https://{}", cli.addr);

    fn run_engine(engine: impl KvsEngine, addr: SocketAddr) -> Result<()> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let n_workers = thread::available_parallelism().unwrap().get();
        let server = KvsServer::<_, SharedQueueThreadPool>::new(engine, shutdown, n_workers);
        Ok(server.listen_on(addr)?)
    }

    match real_engine {
        Engine::Kvs => run_engine(KvStore::open(path)?, cli.addr)?,
        Engine::Sled => run_engine(SledKvsEngine::open(path)?, cli.addr)?,
    }

    Ok(())
}

fn get_real_engine(cli_engine: Option<String>, path: &Path) -> Result<Engine> {
    let sepcified_engine = if let Some(engine) = cli_engine {
        match engine.as_str() {
            "kvs" => Some(Engine::Kvs),
            "sled" => Some(Engine::Sled),
            _ => return Err(anyhow!("Not a valid engine: {}", engine)),
        }
    } else {
        None
    };

    // empty
    let real_engine = if path.exists() {
        // sled
        if path.join("conf").exists() {
            if let Some(engine) = sepcified_engine && let Engine::Kvs = engine {
                return Err(anyhow!("Wrong engine type, 'sled' expected"));
            }
            Engine::Sled
        } else if path.join("kvs").exists() {
            // kvs
            if let Some(engine) = sepcified_engine && let Engine::Sled = engine {
                return Err(anyhow!("Wrong engine type, 'kvs' expected"));
            }
            Engine::Kvs
        } else if let Some(engine) = sepcified_engine {
            engine
        } else {
            Engine::Kvs
        }
    } else if let Some(engine) = sepcified_engine {
        engine
    } else {
        Engine::Kvs
    };
    Ok(real_engine)
}
