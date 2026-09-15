//! Configuration loading and validation.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::protocol::Format;
use crate::{DEFAULT_SOCKET_PATH, TOKEN_ENV};

/// Top level configuration, parsed from `/etc/telegram-notifier/config.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub service: ServiceConfig,
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub defaults: DefaultsConfig,
    #[serde(default)]
    pub targets: BTreeMap<String, TargetConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Path of the Unix domain socket the daemon listens on.
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
    /// Group that owns the socket. Members of this group may send messages.
    #[serde(default = "default_socket_group")]
    pub socket_group: String,
    /// Socket permissions, as an octal string such as `"0660"`.
    #[serde(default = "default_socket_mode")]
    pub socket_mode: String,
    /// Directory holding the persistent queue.
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    /// Log filter, in `tracing-subscriber` syntax (`info`, `debug`, ...).
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Refuse new messages once this many are queued. 0 disables the limit.
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
    /// Minimum delay between two Telegram API calls, in milliseconds.
    #[serde(default = "default_min_send_interval_ms")]
    pub min_send_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    /// Bot token issued by @BotFather. Optional here if `bot_token_file` or
    /// the `TELEGRAM_NOTIFIER_BOT_TOKEN` environment variable is used instead.
    #[serde(default)]
    pub bot_token: Option<String>,
    /// File holding the bot token on a single line. Preferred over inlining
    /// the secret, so that the main config file can stay world-readable.
    #[serde(default)]
    pub bot_token_file: Option<PathBuf>,
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Give up after this many failed attempts. 0 means "retry forever".
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_initial_backoff_seconds")]
    pub initial_backoff_seconds: u64,
    #[serde(default = "default_max_backoff_seconds")]
    pub max_backoff_seconds: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultsConfig {
    /// Target used when the client does not name one.
    #[serde(default = "default_target_name")]
    pub target: String,
    /// Default rendering mode for message bodies.
    #[serde(default)]
    pub format: Format,
    /// Send messages without a notification sound by default.
    #[serde(default)]
    pub silent: bool,
    /// Disable Telegram link previews by default.
    #[serde(default = "default_true")]
    pub disable_link_preview: bool,
}

/// A named recipient. Every field except `chat_id` falls back to `[defaults]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    /// Numeric chat id, or `@channelusername`.
    pub chat_id: String,
    /// Forum topic id, for supergroups with topics enabled.
    #[serde(default)]
    pub message_thread_id: Option<i64>,
    /// Per-target bot token, overriding `[telegram] bot_token`.
    #[serde(default)]
    pub bot_token: Option<String>,
    #[serde(default)]
    pub bot_token_file: Option<PathBuf>,
    #[serde(default)]
    pub format: Option<Format>,
    #[serde(default)]
    pub silent: Option<bool>,
    #[serde(default)]
    pub disable_link_preview: Option<bool>,
    /// Text prepended to every message sent to this target.
    #[serde(default)]
    pub prefix: Option<String>,
}

/// A target with every inherited value already resolved.
#[derive(Debug, Clone, Serialize)]
pub struct ResolvedTarget {
    pub name: String,
    pub chat_id: String,
    pub message_thread_id: Option<i64>,
    #[serde(skip)]
    pub bot_token: String,
    pub format: Format,
    pub silent: bool,
    pub disable_link_preview: bool,
    pub prefix: Option<String>,
}

impl Config {
    /// Read and validate the configuration file at `path`.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading configuration file {}", path.display()))?;
        let config: Config = toml::from_str(&raw)
            .with_context(|| format!("parsing configuration file {}", path.display()))?;
        config.validate(path)?;
        Ok(config)
    }

    fn validate(&self, path: &Path) -> Result<()> {
        if self.targets.is_empty() {
            bail!(
                "{}: no targets defined; add at least a [targets.{}] section",
                path.display(),
                self.defaults.target
            );
        }
        if !self.targets.contains_key(&self.defaults.target) {
            bail!(
                "{}: [defaults] target = \"{}\" does not match any [targets.*] section",
                path.display(),
                self.defaults.target
            );
        }
        parse_mode(&self.service.socket_mode)?;
        if self.retry.backoff_multiplier < 1.0 {
            bail!(
                "{}: [retry] backoff_multiplier must be >= 1.0",
                path.display()
            );
        }
        // Every target must be able to resolve a token, one way or another.
        for name in self.targets.keys() {
            self.resolve_target(name)
                .with_context(|| format!("{}: target \"{}\"", path.display(), name))?;
        }
        Ok(())
    }

    /// Socket permissions as a numeric mode.
    pub fn socket_mode(&self) -> Result<u32> {
        parse_mode(&self.service.socket_mode)
    }

    /// Resolve `name` into a target with all defaults and secrets applied.
    pub fn resolve_target(&self, name: &str) -> Result<ResolvedTarget> {
        let target = self
            .targets
            .get(name)
            .with_context(|| format!("unknown target \"{name}\""))?;

        let bot_token = match (&target.bot_token, &target.bot_token_file) {
            (Some(token), _) => token.trim().to_string(),
            (None, Some(file)) => read_token_file(file)?,
            (None, None) => self.global_token()?,
        };
        if bot_token.is_empty() {
            bail!("resolved bot token is empty");
        }

        Ok(ResolvedTarget {
            name: name.to_string(),
            chat_id: target.chat_id.clone(),
            message_thread_id: target.message_thread_id,
            bot_token,
            format: target.format.unwrap_or(self.defaults.format),
            silent: target.silent.unwrap_or(self.defaults.silent),
            disable_link_preview: target
                .disable_link_preview
                .unwrap_or(self.defaults.disable_link_preview),
            prefix: target.prefix.clone(),
        })
    }

    /// The token shared by every target that does not override it. The
    /// environment variable wins so that `systemd` credentials or a drop-in
    /// can inject the secret without touching the config file.
    fn global_token(&self) -> Result<String> {
        if let Ok(token) = std::env::var(TOKEN_ENV) {
            let token = token.trim().to_string();
            if !token.is_empty() {
                return Ok(token);
            }
        }
        if let Some(token) = &self.telegram.bot_token {
            let token = token.trim();
            if !token.is_empty() {
                return Ok(token.to_string());
            }
        }
        if let Some(file) = &self.telegram.bot_token_file {
            return read_token_file(file);
        }
        bail!(
            "no bot token configured; set [telegram] bot_token or bot_token_file, \
             or export {TOKEN_ENV}"
        )
    }
}

fn read_token_file(path: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading bot token file {}", path.display()))?;
    let token = raw.trim().to_string();
    if token.is_empty() {
        bail!("bot token file {} is empty", path.display());
    }
    Ok(token)
}

fn parse_mode(mode: &str) -> Result<u32> {
    u32::from_str_radix(mode.trim_start_matches("0o"), 8)
        .with_context(|| format!("invalid octal socket_mode \"{mode}\""))
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
            socket_group: default_socket_group(),
            socket_mode: default_socket_mode(),
            state_dir: default_state_dir(),
            log_level: default_log_level(),
            max_queue_size: default_max_queue_size(),
            min_send_interval_ms: default_min_send_interval_ms(),
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_backoff_seconds: default_initial_backoff_seconds(),
            max_backoff_seconds: default_max_backoff_seconds(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            target: default_target_name(),
            format: Format::default(),
            silent: false,
            disable_link_preview: true,
        }
    }
}

fn default_socket_path() -> PathBuf {
    PathBuf::from(DEFAULT_SOCKET_PATH)
}
fn default_socket_group() -> String {
    "telegram-notify".to_string()
}
fn default_socket_mode() -> String {
    "0660".to_string()
}
fn default_state_dir() -> PathBuf {
    PathBuf::from("/var/lib/telegram-notifier")
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_max_queue_size() -> usize {
    10_000
}
fn default_min_send_interval_ms() -> u64 {
    50
}
fn default_api_base_url() -> String {
    "https://api.telegram.org".to_string()
}
fn default_timeout_seconds() -> u64 {
    15
}
fn default_max_attempts() -> u32 {
    10
}
fn default_initial_backoff_seconds() -> u64 {
    5
}
fn default_max_backoff_seconds() -> u64 {
    900
}
fn default_backoff_multiplier() -> f64 {
    2.0
}
fn default_target_name() -> String {
    "default".to_string()
}
fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
[telegram]
bot_token = "123:abc"

[targets.default]
chat_id = "42"
"#;

    fn parse(raw: &str) -> Result<Config> {
        let cfg: Config = toml::from_str(raw)?;
        cfg.validate(Path::new("<test>"))?;
        Ok(cfg)
    }

    #[test]
    fn minimal_config_applies_defaults() {
        let cfg = parse(MINIMAL).expect("minimal config should be valid");
        assert_eq!(cfg.defaults.target, "default");
        assert_eq!(cfg.socket_mode().unwrap(), 0o660);
        let target = cfg.resolve_target("default").unwrap();
        assert_eq!(target.bot_token, "123:abc");
        assert_eq!(target.format, Format::Plain);
        assert!(target.disable_link_preview);
    }

    #[test]
    fn targets_inherit_and_override_defaults() {
        let raw = r#"
[telegram]
bot_token = "123:abc"

[defaults]
target = "ops"
format = "html"
silent = true

[targets.ops]
chat_id = "-100123"
message_thread_id = 7
prefix = "[ops] "

[targets.personal]
chat_id = "42"
format = "plain"
bot_token = "999:zzz"
"#;
        let cfg = parse(raw).unwrap();
        let ops = cfg.resolve_target("ops").unwrap();
        assert_eq!(ops.format, Format::Html);
        assert!(ops.silent);
        assert_eq!(ops.message_thread_id, Some(7));
        assert_eq!(ops.prefix.as_deref(), Some("[ops] "));

        let personal = cfg.resolve_target("personal").unwrap();
        assert_eq!(personal.format, Format::Plain);
        assert_eq!(personal.bot_token, "999:zzz");
        assert!(personal.silent, "silent falls back to [defaults]");
    }

    #[test]
    fn default_target_must_exist() {
        let raw = r#"
[telegram]
bot_token = "123:abc"

[defaults]
target = "missing"

[targets.ops]
chat_id = "1"
"#;
        assert!(parse(raw).is_err());
    }

    #[test]
    fn missing_token_is_rejected() {
        let raw = r#"
[telegram]

[targets.default]
chat_id = "1"
"#;
        // Guard against a token leaking in from the ambient environment.
        std::env::remove_var(TOKEN_ENV);
        assert!(parse(raw).is_err());
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let raw = r#"
[telegram]
bot_token = "1:a"
typo_here = true

[targets.default]
chat_id = "1"
"#;
        assert!(parse(raw).is_err());
    }
}
