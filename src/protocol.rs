//! Wire protocol spoken over the control socket.
//!
//! One JSON object per line in each direction: the client writes a
//! [`Request`], the daemon answers with a [`Response`] and keeps the
//! connection open for further requests until the client closes it.

use serde::{Deserialize, Serialize};

/// Current protocol version. The daemon accepts requests without a version
/// field, and rejects versions it does not understand.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum accepted length of a single request line, in bytes.
pub const MAX_REQUEST_BYTES: usize = 256 * 1024;

/// How a message body should be rendered by Telegram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    /// No markup. Safest option: the text is sent verbatim.
    #[default]
    Plain,
    /// Telegram's legacy Markdown dialect.
    Markdown,
    /// Telegram's MarkdownV2 dialect. The caller must escape reserved chars.
    MarkdownV2,
    /// A restricted subset of HTML, as documented by the Bot API.
    Html,
}

impl Format {
    /// The value Telegram expects in the `parse_mode` field, if any.
    pub fn parse_mode(self) -> Option<&'static str> {
        match self {
            Format::Plain => None,
            Format::Markdown => Some("Markdown"),
            Format::MarkdownV2 => Some("MarkdownV2"),
            Format::Html => Some("HTML"),
        }
    }
}

impl std::str::FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().replace(['-', '_'], "").as_str() {
            "plain" | "text" | "none" => Ok(Format::Plain),
            "markdown" | "md" => Ok(Format::Markdown),
            "markdownv2" | "mdv2" => Ok(Format::MarkdownV2),
            "html" => Ok(Format::Html),
            other => Err(format!(
                "unknown format \"{other}\" (expected plain, markdown, markdownv2 or html)"
            )),
        }
    }
}

/// Delivery priority. Higher priorities leave the queue first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
}

impl std::str::FromStr for Priority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "low" => Ok(Priority::Low),
            "normal" | "default" => Ok(Priority::Normal),
            "high" | "urgent" => Ok(Priority::High),
            other => Err(format!(
                "unknown priority \"{other}\" (expected low, normal or high)"
            )),
        }
    }
}

/// A request from a client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Request {
    /// Queue a message for delivery.
    Notify(NotifyRequest),
    /// Liveness check.
    Ping,
    /// Queue statistics and configured targets.
    Status,
    /// Block until the queue drains, or until `timeout_ms` elapses. Used by
    /// callers that must not proceed while a message is still undelivered —
    /// a shutdown hook, most obviously, where the daemon and the network are
    /// about to go away.
    Flush {
        #[serde(default = "default_flush_timeout_ms")]
        timeout_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyRequest {
    #[serde(default = "default_version")]
    pub version: u32,
    /// Message body. Required and non-empty.
    pub text: String,
    /// Optional bold/first line prepended to the body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Named target from the config. Defaults to `[defaults] target`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<Format>,
    #[serde(default)]
    pub priority: Priority,
    /// Suppress the notification sound. Defaults to the target's setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    /// Free-form label identifying the sender, used in the daemon's logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

fn default_version() -> u32 {
    PROTOCOL_VERSION
}

fn default_flush_timeout_ms() -> u64 {
    15_000
}

/// Refuse to hold a connection open longer than this, whatever the client
/// asks for, so a stuck client cannot pin a daemon task indefinitely.
pub const MAX_FLUSH_TIMEOUT_MS: u64 = 300_000;

/// The daemon's answer to a [`Request`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    /// The message was persisted and will be delivered.
    Queued { id: String },
    /// The daemon is alive.
    Pong,
    /// Queue statistics.
    Status(StatusReport),
    /// Outcome of a [`Request::Flush`]. `pending` is what was still queued
    /// when the daemon stopped waiting, so zero means everything was
    /// delivered.
    Flushed { pending: usize, timed_out: bool },
    /// The request was rejected. Nothing was queued.
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusReport {
    pub version: String,
    pub pending: usize,
    pub failed: usize,
    pub delivered_since_start: u64,
    pub targets: Vec<String>,
    pub default_target: String,
}

impl NotifyRequest {
    /// Reject requests that could never be delivered, before touching the disk.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != PROTOCOL_VERSION {
            return Err(format!(
                "unsupported protocol version {} (this daemon speaks {})",
                self.version, PROTOCOL_VERSION
            ));
        }
        if self.text.trim().is_empty() && self.title.as_deref().unwrap_or("").trim().is_empty() {
            return Err("message text is empty".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_as_tagged_json() {
        let line = r#"{"action":"notify","text":"hello","target":"ops","priority":"high"}"#;
        let request: Request = serde_json::from_str(line).unwrap();
        let Request::Notify(notify) = request else {
            panic!("expected a notify request");
        };
        assert_eq!(notify.text, "hello");
        assert_eq!(notify.target.as_deref(), Some("ops"));
        assert_eq!(notify.priority, Priority::High);
        assert_eq!(notify.version, PROTOCOL_VERSION);
        notify.validate().unwrap();
    }

    #[test]
    fn ping_needs_no_payload() {
        let request: Request = serde_json::from_str(r#"{"action":"ping"}"#).unwrap();
        assert!(matches!(request, Request::Ping));
    }

    #[test]
    fn empty_text_is_rejected() {
        let notify = NotifyRequest {
            version: PROTOCOL_VERSION,
            text: "   ".into(),
            title: None,
            target: None,
            format: None,
            priority: Priority::Normal,
            silent: None,
            source: None,
        };
        assert!(notify.validate().is_err());
    }

    #[test]
    fn future_protocol_versions_are_rejected() {
        let line = r#"{"action":"notify","version":99,"text":"hi"}"#;
        let Request::Notify(notify) = serde_json::from_str(line).unwrap() else {
            panic!("expected notify");
        };
        assert!(notify.validate().is_err());
    }

    #[test]
    fn flush_defaults_its_timeout() {
        let request: Request = serde_json::from_str(r#"{"action":"flush"}"#).unwrap();
        let Request::Flush { timeout_ms } = request else {
            panic!("expected a flush request");
        };
        assert_eq!(timeout_ms, default_flush_timeout_ms());

        let explicit: Request =
            serde_json::from_str(r#"{"action":"flush","timeout_ms":250}"#).unwrap();
        assert!(matches!(explicit, Request::Flush { timeout_ms: 250 }));
    }

    #[test]
    fn priority_orders_low_to_high() {
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[test]
    fn formats_map_to_telegram_parse_modes() {
        assert_eq!(Format::Plain.parse_mode(), None);
        assert_eq!(Format::MarkdownV2.parse_mode(), Some("MarkdownV2"));
        assert_eq!("mdv2".parse::<Format>().unwrap(), Format::MarkdownV2);
        assert!("nope".parse::<Format>().is_err());
    }
}
