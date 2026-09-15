//! Shared library for the `telegram-notifier` daemon and its CLI client.
//!
//! The daemon (`telegram-notifierd`) listens on a Unix domain socket, persists
//! every accepted notification to an on-disk queue and forwards it to the
//! Telegram Bot API with retries and rate limiting. The client
//! (`telegram-notify`) is a thin writer for that socket.

pub mod client;
pub mod config;
pub mod protocol;
pub mod queue;
pub mod server;
pub mod telegram;
pub mod worker;

/// Default location of the daemon configuration file.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/telegram-notifier/config.toml";

/// Default location of the control socket.
pub const DEFAULT_SOCKET_PATH: &str = "/run/telegram-notifier/notifier.sock";

/// Environment variable that overrides the socket path for the CLI client.
pub const SOCKET_ENV: &str = "TELEGRAM_NOTIFIER_SOCKET";

/// Environment variable that overrides the bot token for the daemon.
pub const TOKEN_ENV: &str = "TELEGRAM_NOTIFIER_BOT_TOKEN";
