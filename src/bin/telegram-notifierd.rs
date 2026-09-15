//! `telegram-notifierd` — the notification daemon.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use telegram_notifier::config::Config;
use telegram_notifier::queue::Queue;
use telegram_notifier::telegram::TelegramClient;
use telegram_notifier::{server, worker, DEFAULT_CONFIG_PATH};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Notify;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "telegram-notifierd",
    version,
    about = "Daemon that delivers queued notifications to Telegram"
)]
struct Args {
    /// Path to the configuration file.
    #[arg(short, long, value_name = "FILE", default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,

    /// Validate the configuration and exit without binding the socket.
    #[arg(long)]
    check: bool,

    /// Override the log level from the configuration file.
    #[arg(short = 'l', long, value_name = "LEVEL")]
    log_level: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let config = Config::load(&args.config)?;
    let filter = args
        .log_level
        .clone()
        .unwrap_or_else(|| config.service.log_level.clone());
    init_logging(&filter);

    if args.check {
        println!(
            "configuration OK: {} target(s), default \"{}\"",
            config.targets.len(),
            config.defaults.target
        );
        return Ok(());
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the async runtime")?
        .block_on(run(config))
}

fn init_logging(filter: &str) {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(filter))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        // systemd captures stderr into the journal, which timestamps it itself.
        .without_time()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

async fn run(config: Config) -> Result<()> {
    let config = Arc::new(config);
    info!(
        version = env!("CARGO_PKG_VERSION"),
        "telegram-notifier starting"
    );

    let queue = Queue::open(&config.service.state_dir)?;
    let client = TelegramClient::new(&config.telegram)?;
    let shutdown = Arc::new(Notify::new());

    let (worker_handle, worker_join) = worker::spawn(
        Arc::clone(&config),
        queue.clone(),
        client,
        Arc::clone(&shutdown),
    )?;

    let listener = server::bind(&config)?;
    let socket_path = config.service.socket_path.clone();
    let server_join = tokio::spawn(server::serve(
        listener,
        Arc::clone(&config),
        queue,
        worker_handle,
        Arc::clone(&shutdown),
    ));

    notify_systemd_ready();
    wait_for_signal().await;

    info!("shutting down");
    shutdown.notify_waiters();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let _ = server_join.await;
        let _ = worker_join.await;
    })
    .await;

    // Leave no stale socket behind for the next start.
    if let Err(err) = std::fs::remove_file(&socket_path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            error!(error = %err, "could not remove the control socket");
        }
    }
    Ok(())
}

/// Tell systemd we are ready, when running under `Type=notify`.
/// Implemented inline to avoid a dependency for ~20 lines of code.
fn notify_systemd_ready() {
    use std::os::unix::net::UnixDatagram;

    let Ok(address) = std::env::var("NOTIFY_SOCKET") else {
        return;
    };
    // A leading '@' denotes the abstract namespace, encoded as a NUL byte.
    let address = if let Some(rest) = address.strip_prefix('@') {
        format!("\0{rest}")
    } else {
        address
    };
    if let Ok(socket) = UnixDatagram::unbound() {
        let _ = socket.send_to(b"READY=1", address);
    }
}

/// Resolve on SIGINT or SIGTERM.
async fn wait_for_signal() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(err) => {
            error!(error = %err, "could not install the SIGTERM handler");
            return;
        }
    };
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(stream) => stream,
        Err(err) => {
            error!(error = %err, "could not install the SIGINT handler");
            return;
        }
    };
    tokio::select! {
        _ = sigterm.recv() => info!("received SIGTERM"),
        _ = sigint.recv() => info!("received SIGINT"),
    }
}
