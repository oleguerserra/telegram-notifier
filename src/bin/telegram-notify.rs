//! `telegram-notify` — command line client for the notification daemon.
//!
//! Exit codes: 0 on success, 1 on a rejected request, 2 on a usage or
//! transport error. Scripts can rely on these.

use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use telegram_notifier::client::send_request;
use telegram_notifier::protocol::{
    Format, NotifyRequest, Priority, Request, Response, PROTOCOL_VERSION,
};
use telegram_notifier::{DEFAULT_SOCKET_PATH, SOCKET_ENV};

#[derive(Debug, Parser)]
#[command(
    name = "telegram-notify",
    version,
    about = "Send a notification to Telegram through telegram-notifier",
    after_help = "\
EXAMPLES:
  telegram-notify \"Backup finished\"
  telegram-notify --target ops --priority high \"Disk almost full\"
  journalctl -u nginx -n 20 | telegram-notify --title \"nginx\" --stdin
  telegram-notify --status"
)]
struct Args {
    /// Message text. If omitted, the message is read from standard input.
    #[arg(value_name = "TEXT", trailing_var_arg = true)]
    text: Vec<String>,

    /// Named target from the daemon configuration.
    #[arg(short, long, value_name = "NAME")]
    target: Option<String>,

    /// Optional heading, rendered above the body.
    #[arg(short = 'T', long, value_name = "TEXT")]
    title: Option<String>,

    /// Delivery priority: low, normal or high.
    #[arg(short, long, value_name = "LEVEL", default_value = "normal")]
    priority: Priority,

    /// Body markup: plain, markdown, markdownv2 or html.
    #[arg(short, long, value_name = "FORMAT")]
    format: Option<Format>,

    /// Deliver without a notification sound.
    #[arg(short, long)]
    silent: bool,

    /// Label identifying the sender, recorded in the daemon's logs.
    #[arg(short = 'S', long, value_name = "NAME")]
    source: Option<String>,

    /// Read the body from standard input even when TEXT is given; the text
    /// arguments are then used as the first line.
    #[arg(long)]
    stdin: bool,

    /// Path of the daemon control socket.
    #[arg(long, value_name = "PATH", env = SOCKET_ENV, default_value = DEFAULT_SOCKET_PATH)]
    socket: PathBuf,

    /// Seconds to wait for the daemon to answer.
    #[arg(long, value_name = "SECONDS", default_value_t = 10)]
    timeout: u64,

    /// Check that the daemon is reachable and exit.
    #[arg(long, conflicts_with_all = ["status", "text"])]
    ping: bool,

    /// Print the daemon's queue statistics as JSON and exit.
    #[arg(long, conflicts_with_all = ["ping", "text"])]
    status: bool,

    /// Suppress the message id printed on success.
    #[arg(short, long)]
    quiet: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("telegram-notify: {err:#}");
            ExitCode::from(2)
        }
    }
}

fn run(args: &Args) -> anyhow::Result<ExitCode> {
    let timeout = Duration::from_secs(args.timeout.max(1));

    let request = if args.ping {
        Request::Ping
    } else if args.status {
        Request::Status
    } else {
        Request::Notify(build_notify(args)?)
    };

    let response = send_request(&args.socket, &request, timeout)?;

    Ok(match response {
        Response::Queued { id } => {
            if !args.quiet {
                println!("{id}");
            }
            ExitCode::SUCCESS
        }
        Response::Pong => {
            if !args.quiet {
                println!("pong");
            }
            ExitCode::SUCCESS
        }
        Response::Status(report) => {
            println!("{}", serde_json::to_string_pretty(&report)?);
            ExitCode::SUCCESS
        }
        Response::Error { message } => {
            eprintln!("telegram-notify: {message}");
            ExitCode::from(1)
        }
    })
}

fn build_notify(args: &Args) -> anyhow::Result<NotifyRequest> {
    let text = collect_text(args)?;
    let request = NotifyRequest {
        version: PROTOCOL_VERSION,
        text,
        title: args.title.clone(),
        target: args.target.clone(),
        format: args.format,
        priority: args.priority,
        silent: if args.silent { Some(true) } else { None },
        source: args.source.clone().or_else(default_source),
    };
    request
        .validate()
        .map_err(|message| anyhow::anyhow!(message))?;
    Ok(request)
}

/// Positional arguments win; stdin is used when there are none, or when
/// `--stdin` explicitly asks for both.
fn collect_text(args: &Args) -> anyhow::Result<String> {
    let inline = args.text.join(" ");
    if !args.stdin && !inline.is_empty() {
        return Ok(inline);
    }
    let mut piped = String::new();
    std::io::stdin().read_to_string(&mut piped)?;
    let piped = piped.trim_end_matches('\n').to_string();
    Ok(match (inline.is_empty(), piped.is_empty()) {
        (true, _) => piped,
        (false, true) => inline,
        (false, false) => format!("{inline}\n{piped}"),
    })
}

/// Default the source label to the invoking unit or user, which makes the
/// daemon's logs useful without every caller passing `--source`.
fn default_source() -> Option<String> {
    std::env::var("SYSTEMD_UNIT")
        .ok()
        .or_else(|| {
            std::env::var("JOURNAL_STREAM")
                .ok()
                .map(|_| "systemd".into())
        })
        .or_else(|| std::env::var("USER").ok())
}
