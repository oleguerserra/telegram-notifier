//! Delivery worker: drains the persistent queue into Telegram.
//!
//! A single worker task owns the queue, which keeps ordering predictable and
//! makes the Telegram rate limit trivial to honour. It wakes on a new message,
//! on a retry becoming due, or on shutdown.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, Notify};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::queue::{self, Queue, QueueEntry};
use crate::telegram::{render_body, SendError, TelegramClient};

/// Counters the socket server reports through the `status` action.
#[derive(Debug, Default)]
pub struct Metrics {
    pub pending: AtomicUsize,
    pub delivered: AtomicU64,
}

/// Shared handle used by the socket server to hand work to the worker.
#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<QueueEntry>,
    pub metrics: Arc<Metrics>,
    max_queue_size: usize,
}

impl WorkerHandle {
    /// Hand an already-persisted entry to the worker. Returns an error when
    /// the queue is full, so the caller can reject the client's request.
    pub async fn submit(&self, entry: QueueEntry) -> Result<(), String> {
        if self.max_queue_size > 0
            && self.metrics.pending.load(Ordering::Relaxed) >= self.max_queue_size
        {
            return Err(format!(
                "queue is full ({} messages pending)",
                self.max_queue_size
            ));
        }
        self.sender
            .send(entry)
            .await
            .map_err(|_| "delivery worker is not running".to_string())
    }

    pub fn pending(&self) -> usize {
        self.metrics.pending.load(Ordering::Relaxed)
    }

    pub fn delivered(&self) -> u64 {
        self.metrics.delivered.load(Ordering::Relaxed)
    }
}

/// Spawn the delivery worker and return a handle plus its join handle.
pub fn spawn(
    config: Arc<Config>,
    queue: Queue,
    client: TelegramClient,
    shutdown: Arc<Notify>,
) -> Result<(WorkerHandle, tokio::task::JoinHandle<()>)> {
    let entries = queue.load_all()?;
    if !entries.is_empty() {
        info!(count = entries.len(), "restored pending messages from disk");
    }

    let metrics = Arc::new(Metrics::default());
    metrics.pending.store(entries.len(), Ordering::Relaxed);

    let (sender, receiver) = mpsc::channel(1024);
    let handle = WorkerHandle {
        sender,
        metrics: Arc::clone(&metrics),
        max_queue_size: config.service.max_queue_size,
    };

    let worker = Worker {
        config,
        queue,
        client,
        entries,
        metrics,
        last_send: None,
    };
    let join = tokio::spawn(worker.run(receiver, shutdown));
    Ok((handle, join))
}

struct Worker {
    config: Arc<Config>,
    queue: Queue,
    client: TelegramClient,
    entries: HashMap<String, QueueEntry>,
    metrics: Arc<Metrics>,
    last_send: Option<Instant>,
}

impl Worker {
    async fn run(mut self, mut receiver: mpsc::Receiver<QueueEntry>, shutdown: Arc<Notify>) {
        loop {
            let now = queue::now_secs();
            let has_due = queue::next_due(&self.entries, now).is_some();

            if has_due {
                // Drain anything already waiting so newly submitted urgent
                // messages can overtake a low-priority backlog.
                while let Ok(entry) = receiver.try_recv() {
                    self.accept(entry);
                }
                self.respect_rate_limit().await;
                self.attempt_next().await;
                continue;
            }

            let sleep_for = queue::seconds_until_next(&self.entries, now)
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(3600));

            tokio::select! {
                biased;
                _ = shutdown.notified() => {
                    info!(pending = self.entries.len(), "worker stopping; queue is on disk");
                    return;
                }
                maybe_entry = receiver.recv() => {
                    match maybe_entry {
                        Some(entry) => self.accept(entry),
                        None => {
                            info!("request channel closed, worker stopping");
                            return;
                        }
                    }
                }
                _ = tokio::time::sleep(sleep_for) => {}
            }
        }
    }

    fn accept(&mut self, entry: QueueEntry) {
        debug!(id = %entry.id, target = %entry.message.target, "message accepted by worker");
        self.entries.insert(entry.id.clone(), entry);
        self.metrics
            .pending
            .store(self.entries.len(), Ordering::Relaxed);
    }

    /// Keep at least `min_send_interval_ms` between two API calls.
    async fn respect_rate_limit(&mut self) {
        let interval = Duration::from_millis(self.config.service.min_send_interval_ms);
        if interval.is_zero() {
            return;
        }
        if let Some(last) = self.last_send {
            let elapsed = last.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
    }

    async fn attempt_next(&mut self) {
        let now = queue::now_secs();
        let Some(entry) = queue::next_due(&self.entries, now).cloned() else {
            return;
        };

        // Targets are resolved at send time so that a config reload can fix a
        // bad chat id without the operator having to re-send the message.
        let target = match self.config.resolve_target(&entry.message.target) {
            Ok(target) => target,
            Err(err) => {
                error!(
                    id = %entry.id,
                    target = %entry.message.target,
                    error = %err,
                    "dropping message for an unresolvable target"
                );
                self.finish_failed(&entry, &err.to_string());
                return;
            }
        };

        let format = entry.message.format.unwrap_or(target.format);
        let silent = entry.message.silent.unwrap_or(target.silent);
        let mut body = render_body(entry.message.title.as_deref(), &entry.message.text, format);
        if let Some(prefix) = &target.prefix {
            body = format!("{prefix}{body}");
        }

        self.last_send = Some(Instant::now());
        match self
            .client
            .send_message(&target, &body, format, silent)
            .await
        {
            Ok(()) => {
                info!(
                    id = %entry.id,
                    target = %target.name,
                    source = entry.message.source.as_deref().unwrap_or("-"),
                    attempts = entry.attempts + 1,
                    "message delivered"
                );
                self.entries.remove(&entry.id);
                if let Err(err) = self.queue.remove(&entry.id) {
                    warn!(id = %entry.id, error = %err, "could not remove delivered entry");
                }
                self.metrics.delivered.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .pending
                    .store(self.entries.len(), Ordering::Relaxed);
            }
            Err(err) => self.handle_failure(entry, err),
        }
    }

    fn handle_failure(&mut self, mut entry: QueueEntry, err: SendError) {
        entry.attempts += 1;
        entry.last_error = Some(err.to_string());

        let max = self.config.retry.max_attempts;
        let exhausted = max > 0 && entry.attempts >= max;

        if !err.is_retryable() || exhausted {
            error!(
                id = %entry.id,
                target = %entry.message.target,
                attempts = entry.attempts,
                error = %err,
                "giving up on message"
            );
            self.finish_failed(&entry, &err.to_string());
            return;
        }

        let delay = err
            .retry_after()
            .unwrap_or_else(|| self.backoff_seconds(entry.attempts));
        entry.next_attempt_at = queue::now_secs() + delay;
        warn!(
            id = %entry.id,
            target = %entry.message.target,
            attempts = entry.attempts,
            retry_in_s = delay,
            error = %err,
            "delivery failed, will retry"
        );
        if let Err(store_err) = self.queue.store(&entry) {
            error!(id = %entry.id, error = %store_err, "could not persist retry state");
        }
        self.entries.insert(entry.id.clone(), entry);
    }

    /// Exponential backoff, clamped to `max_backoff_seconds`.
    fn backoff_seconds(&self, attempts: u32) -> u64 {
        let retry = &self.config.retry;
        let exponent = attempts.saturating_sub(1).min(32) as i32;
        let delay = retry.initial_backoff_seconds as f64 * retry.backoff_multiplier.powi(exponent);
        delay.min(retry.max_backoff_seconds as f64).max(1.0) as u64
    }

    fn finish_failed(&mut self, entry: &QueueEntry, reason: &str) {
        let mut dead = entry.clone();
        dead.last_error = Some(reason.to_string());
        if let Err(err) = self.queue.fail(&dead) {
            error!(id = %entry.id, error = %err, "could not move entry to the failed directory");
        }
        self.entries.remove(&entry.id);
        self.metrics
            .pending
            .store(self.entries.len(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RetryConfig;

    fn worker_with(retry: RetryConfig) -> Worker {
        let raw = r#"
[telegram]
bot_token = "1:a"

[targets.default]
chat_id = "1"
"#;
        let mut config: Config = toml::from_str(raw).unwrap();
        config.retry = retry;
        let dir = tempfile::tempdir().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let client = TelegramClient::new(&config.telegram).unwrap();
        // The tempdir is intentionally leaked: the worker only needs the paths
        // to exist for the lifetime of the test.
        std::mem::forget(dir);
        Worker {
            config: Arc::new(config),
            queue,
            client,
            entries: HashMap::new(),
            metrics: Arc::new(Metrics::default()),
            last_send: None,
        }
    }

    #[test]
    fn backoff_grows_then_saturates() {
        let worker = worker_with(RetryConfig {
            max_attempts: 10,
            initial_backoff_seconds: 5,
            max_backoff_seconds: 100,
            backoff_multiplier: 2.0,
        });
        assert_eq!(worker.backoff_seconds(1), 5);
        assert_eq!(worker.backoff_seconds(2), 10);
        assert_eq!(worker.backoff_seconds(3), 20);
        assert_eq!(worker.backoff_seconds(99), 100, "clamped to the maximum");
    }

    #[test]
    fn backoff_is_never_zero() {
        let worker = worker_with(RetryConfig {
            max_attempts: 3,
            initial_backoff_seconds: 0,
            max_backoff_seconds: 60,
            backoff_multiplier: 1.0,
        });
        assert_eq!(worker.backoff_seconds(1), 1);
    }

    #[test]
    fn permanent_errors_go_straight_to_failed() {
        let mut worker = worker_with(RetryConfig::default());
        let entry = QueueEntry::new(
            crate::protocol::NotifyRequest {
                version: crate::protocol::PROTOCOL_VERSION,
                text: "hi".into(),
                title: None,
                target: None,
                format: None,
                priority: crate::protocol::Priority::Normal,
                silent: None,
                source: None,
            },
            "default".into(),
        );
        worker.queue.store(&entry).unwrap();
        worker.entries.insert(entry.id.clone(), entry.clone());

        worker.handle_failure(
            entry,
            SendError::Permanent {
                code: 400,
                description: "chat not found".into(),
            },
        );

        assert!(worker.entries.is_empty(), "not retried");
        assert_eq!(worker.queue.failed_count(), 1, "kept for inspection");
    }

    #[test]
    fn retryable_errors_are_rescheduled() {
        let mut worker = worker_with(RetryConfig::default());
        let entry = QueueEntry::new(
            crate::protocol::NotifyRequest {
                version: crate::protocol::PROTOCOL_VERSION,
                text: "hi".into(),
                title: None,
                target: None,
                format: None,
                priority: crate::protocol::Priority::Normal,
                silent: None,
                source: None,
            },
            "default".into(),
        );
        let id = entry.id.clone();
        worker.queue.store(&entry).unwrap();
        worker.entries.insert(id.clone(), entry.clone());

        worker.handle_failure(entry, SendError::Transport("connection refused".into()));

        let rescheduled = worker.entries.get(&id).expect("still queued");
        assert_eq!(rescheduled.attempts, 1);
        assert!(rescheduled.next_attempt_at > queue::now_secs());
        assert_eq!(worker.queue.failed_count(), 0);
    }

    #[test]
    fn rate_limit_replies_set_the_retry_delay() {
        let mut worker = worker_with(RetryConfig::default());
        let entry = QueueEntry::new(
            crate::protocol::NotifyRequest {
                version: crate::protocol::PROTOCOL_VERSION,
                text: "hi".into(),
                title: None,
                target: None,
                format: None,
                priority: crate::protocol::Priority::Normal,
                silent: None,
                source: None,
            },
            "default".into(),
        );
        let id = entry.id.clone();
        worker.entries.insert(id.clone(), entry.clone());
        let before = queue::now_secs();

        worker.handle_failure(
            entry,
            SendError::RateLimited {
                retry_after: 42,
                description: "too many".into(),
            },
        );

        let rescheduled = worker.entries.get(&id).unwrap();
        assert!(rescheduled.next_attempt_at >= before + 42);
    }
}
