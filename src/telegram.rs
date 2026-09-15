//! Thin client for the Telegram Bot API `sendMessage` method.

use std::time::Duration;

use serde::Deserialize;
use serde_json::json;
use thiserror::Error;

use crate::config::{ResolvedTarget, TelegramConfig};
use crate::protocol::Format;

/// Telegram refuses message bodies longer than this many UTF-16 code units.
/// Counting characters instead is conservative and therefore safe.
pub const MAX_MESSAGE_CHARS: usize = 4096;

#[derive(Debug, Error)]
pub enum SendError {
    /// Network or timeout problem. Always worth retrying.
    #[error("transport error: {0}")]
    Transport(String),
    /// Telegram asked us to slow down. Retry after the given delay.
    #[error("rate limited, retry in {retry_after}s: {description}")]
    RateLimited {
        retry_after: u64,
        description: String,
    },
    /// Telegram rejected the request in a way that a retry cannot fix
    /// (bad token, unknown chat, malformed markup).
    #[error("permanent failure ({code}): {description}")]
    Permanent { code: i64, description: String },
    /// Server-side problem on Telegram's end. Worth retrying.
    #[error("temporary failure ({code}): {description}")]
    Temporary { code: i64, description: String },
}

impl SendError {
    /// Whether a later attempt could plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        !matches!(self, SendError::Permanent { .. })
    }

    /// Delay Telegram explicitly asked for, if any.
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            SendError::RateLimited { retry_after, .. } => Some(*retry_after),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    ok: bool,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    error_code: Option<i64>,
    #[serde(default)]
    parameters: Option<ApiParameters>,
}

#[derive(Debug, Deserialize)]
struct ApiParameters {
    #[serde(default)]
    retry_after: Option<u64>,
}

/// Reusable HTTP client for the Bot API.
#[derive(Debug, Clone)]
pub struct TelegramClient {
    http: reqwest::Client,
    api_base_url: String,
}

impl TelegramClient {
    pub fn new(config: &TelegramConfig) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .user_agent(concat!("telegram-notifier/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            http,
            api_base_url: config.api_base_url.trim_end_matches('/').to_string(),
        })
    }

    /// Deliver `body` to `target`, splitting it if Telegram would reject the
    /// length. All chunks must succeed for the call to be considered done.
    pub async fn send_message(
        &self,
        target: &ResolvedTarget,
        body: &str,
        format: Format,
        silent: bool,
    ) -> Result<(), SendError> {
        for chunk in split_message(body, MAX_MESSAGE_CHARS) {
            self.send_chunk(target, &chunk, format, silent).await?;
        }
        Ok(())
    }

    async fn send_chunk(
        &self,
        target: &ResolvedTarget,
        text: &str,
        format: Format,
        silent: bool,
    ) -> Result<(), SendError> {
        let url = format!("{}/bot{}/sendMessage", self.api_base_url, target.bot_token);
        let mut payload = json!({
            "chat_id": target.chat_id,
            "text": text,
            "disable_notification": silent,
        });
        if let Some(parse_mode) = format.parse_mode() {
            payload["parse_mode"] = json!(parse_mode);
        }
        if let Some(thread) = target.message_thread_id {
            payload["message_thread_id"] = json!(thread);
        }
        if target.disable_link_preview {
            payload["link_preview_options"] = json!({ "is_disabled": true });
        }

        let response = self
            .http
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|err| SendError::Transport(redact(&err.to_string(), &target.bot_token)))?;

        let http_status = response.status();
        let parsed: ApiResponse = match response.json().await {
            Ok(parsed) => parsed,
            Err(err) => {
                // A non-JSON body means a proxy or gateway answered, not the API.
                return Err(classify_http(
                    http_status.as_u16(),
                    &redact(&err.to_string(), &target.bot_token),
                ));
            }
        };

        if parsed.ok {
            return Ok(());
        }

        let description = parsed
            .description
            .unwrap_or_else(|| "no description".to_string());
        let code = parsed.error_code.unwrap_or(http_status.as_u16() as i64);

        if let Some(retry_after) = parsed.parameters.and_then(|p| p.retry_after) {
            return Err(SendError::RateLimited {
                retry_after,
                description,
            });
        }
        Err(classify_api(code, description))
    }
}

/// Map a Bot API error code onto a retry decision.
fn classify_api(code: i64, description: String) -> SendError {
    match code {
        429 => SendError::RateLimited {
            retry_after: 30,
            description,
        },
        // 4xx other than 429 means the request itself is wrong.
        400..=499 => SendError::Permanent { code, description },
        _ => SendError::Temporary { code, description },
    }
}

/// Map a raw HTTP status onto a retry decision, for replies that were not
/// valid Bot API JSON.
fn classify_http(status: u16, detail: &str) -> SendError {
    let description = format!("HTTP {status}: {detail}");
    match status {
        429 => SendError::RateLimited {
            retry_after: 30,
            description,
        },
        401 | 403 | 404 => SendError::Permanent {
            code: status as i64,
            description,
        },
        _ => SendError::Temporary {
            code: status as i64,
            description,
        },
    }
}

/// Strip a bot token out of a string before it reaches the logs.
fn redact(text: &str, token: &str) -> String {
    if token.is_empty() {
        return text.to_string();
    }
    text.replace(token, "<redacted>")
}

/// Compose the final message body from an optional title and the text.
pub fn render_body(title: Option<&str>, text: &str, format: Format) -> String {
    let Some(title) = title.map(str::trim).filter(|t| !t.is_empty()) else {
        return text.to_string();
    };
    let decorated = match format {
        Format::Plain => title.to_string(),
        Format::Markdown => format!("*{title}*"),
        Format::MarkdownV2 => format!("*{}*", escape_markdown_v2(title)),
        Format::Html => format!("<b>{}</b>", escape_html(title)),
    };
    if text.trim().is_empty() {
        decorated
    } else {
        format!("{decorated}\n{text}")
    }
}

/// Escape the characters MarkdownV2 reserves.
pub fn escape_markdown_v2(text: &str) -> String {
    const RESERVED: &[char] = &[
        '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!',
    ];
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if RESERVED.contains(&ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Escape the three characters Telegram's HTML subset reserves.
pub fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Split `text` into chunks of at most `limit` characters, preferring to cut
/// at a line break so that multi-line output stays readable.
pub fn split_message(text: &str, limit: usize) -> Vec<String> {
    if text.chars().count() <= limit {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_len = 0usize;

    for line in text.split_inclusive('\n') {
        let line_len = line.chars().count();
        if line_len > limit {
            // A single line longer than the limit: flush and hard-split it.
            if current_len > 0 {
                chunks.push(std::mem::take(&mut current));
                current_len = 0;
            }
            let mut piece = String::new();
            let mut piece_len = 0usize;
            for ch in line.chars() {
                piece.push(ch);
                piece_len += 1;
                if piece_len == limit {
                    chunks.push(std::mem::take(&mut piece));
                    piece_len = 0;
                }
            }
            if piece_len > 0 {
                current = piece;
                current_len = piece_len;
            }
        } else {
            if current_len + line_len > limit {
                chunks.push(std::mem::take(&mut current));
                current_len = 0;
            }
            current.push_str(line);
            current_len += line_len;
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_messages_are_not_split() {
        assert_eq!(split_message("hello", 4096), vec!["hello".to_string()]);
    }

    #[test]
    fn splitting_prefers_line_boundaries() {
        let text = "aaaa\nbbbb\ncccc\n";
        let chunks = split_message(text, 10);
        assert_eq!(
            chunks,
            vec!["aaaa\nbbbb\n".to_string(), "cccc\n".to_string()]
        );
        assert_eq!(chunks.concat(), text, "no content is lost");
    }

    #[test]
    fn overlong_single_lines_are_hard_split() {
        let text = "x".repeat(25);
        let chunks = split_message(&text, 10);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks.concat(), text);
        assert!(chunks.iter().all(|c| c.chars().count() <= 10));
    }

    #[test]
    fn splitting_counts_characters_not_bytes() {
        let text = "€".repeat(12); // 3 bytes each
        let chunks = split_message(&text, 5);
        assert!(chunks.iter().all(|c| c.chars().count() <= 5));
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn titles_are_escaped_per_format() {
        assert_eq!(render_body(Some("Hi"), "body", Format::Plain), "Hi\nbody");
        assert_eq!(
            render_body(Some("a.b"), "body", Format::MarkdownV2),
            "*a\\.b*\nbody"
        );
        assert_eq!(
            render_body(Some("a<b>"), "body", Format::Html),
            "<b>a&lt;b&gt;</b>\nbody"
        );
        assert_eq!(render_body(None, "body", Format::Plain), "body");
        assert_eq!(render_body(Some("only"), "", Format::Plain), "only");
    }

    #[test]
    fn permanent_errors_are_not_retried() {
        let permanent = classify_api(400, "chat not found".into());
        assert!(!permanent.is_retryable());

        let temporary = classify_api(500, "internal".into());
        assert!(temporary.is_retryable());

        let limited = SendError::RateLimited {
            retry_after: 12,
            description: "slow".into(),
        };
        assert!(limited.is_retryable());
        assert_eq!(limited.retry_after(), Some(12));
    }

    #[test]
    fn tokens_never_reach_the_logs() {
        let msg = "failed to POST https://api.telegram.org/bot123:SECRET/sendMessage";
        assert!(!redact(msg, "123:SECRET").contains("SECRET"));
    }
}
