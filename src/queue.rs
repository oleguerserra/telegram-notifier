//! Crash-safe on-disk queue.
//!
//! Every accepted message becomes one JSON file under `<state_dir>/queue`.
//! Files are written to a temporary name and then renamed into place, so a
//! crash mid-write can never leave a half-parsed entry behind. Entries that
//! exhaust their retries are moved to `<state_dir>/failed` for inspection
//! rather than being dropped silently.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use crate::protocol::{Format, NotifyRequest, Priority};

/// A queued message, as stored on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub id: String,
    /// Unix timestamp (seconds) when the daemon accepted the message.
    pub created_at: u64,
    /// Unix timestamp (seconds) of the earliest next delivery attempt.
    pub next_attempt_at: u64,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub priority: Priority,
    pub message: QueuedMessage,
}

/// The payload as resolved at accept time, so that later config edits do not
/// silently change the meaning of messages already in flight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedMessage {
    pub target: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<Format>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

impl QueueEntry {
    /// Build a new entry, due immediately.
    pub fn new(request: NotifyRequest, target: String) -> Self {
        let now = now_secs();
        Self {
            id: Uuid::new_v4().to_string(),
            created_at: now,
            next_attempt_at: now,
            attempts: 0,
            last_error: None,
            priority: request.priority,
            message: QueuedMessage {
                target,
                text: request.text,
                title: request.title,
                format: request.format,
                silent: request.silent,
                source: request.source,
            },
        }
    }

    /// Whether this entry may be attempted at `now`.
    pub fn is_due(&self, now: u64) -> bool {
        self.next_attempt_at <= now
    }
}

/// Handle to the queue directories.
#[derive(Debug, Clone)]
pub struct Queue {
    queue_dir: PathBuf,
    failed_dir: PathBuf,
}

impl Queue {
    /// Open (creating if needed) the queue rooted at `state_dir`.
    pub fn open(state_dir: &Path) -> Result<Self> {
        let queue_dir = state_dir.join("queue");
        let failed_dir = state_dir.join("failed");
        for dir in [&queue_dir, &failed_dir] {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating queue directory {}", dir.display()))?;
        }
        Ok(Self {
            queue_dir,
            failed_dir,
        })
    }

    pub fn queue_dir(&self) -> &Path {
        &self.queue_dir
    }

    pub fn failed_dir(&self) -> &Path {
        &self.failed_dir
    }

    fn entry_path(&self, id: &str) -> PathBuf {
        self.queue_dir.join(format!("{id}.json"))
    }

    /// Persist `entry`, replacing any previous version of it. The rename is
    /// atomic, so readers always see either the old or the new content.
    pub fn store(&self, entry: &QueueEntry) -> Result<()> {
        let final_path = self.entry_path(&entry.id);
        let tmp_path = self.queue_dir.join(format!(".{}.json.tmp", entry.id));
        let body = serde_json::to_vec_pretty(entry).context("serialising queue entry")?;
        std::fs::write(&tmp_path, &body)
            .with_context(|| format!("writing {}", tmp_path.display()))?;
        std::fs::rename(&tmp_path, &final_path)
            .with_context(|| format!("renaming into {}", final_path.display()))?;
        // fsync the directory so the rename survives a power loss.
        if let Ok(dir) = std::fs::File::open(&self.queue_dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Remove a delivered entry.
    pub fn remove(&self, id: &str) -> Result<()> {
        let path = self.entry_path(id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).with_context(|| format!("removing {}", path.display())),
        }
    }

    /// Move an entry that exhausted its retries into the `failed` directory.
    pub fn fail(&self, entry: &QueueEntry) -> Result<()> {
        let target = self
            .failed_dir
            .join(format!("{}-{}.json", entry.created_at, entry.id));
        let body = serde_json::to_vec_pretty(entry).context("serialising failed entry")?;
        std::fs::write(&target, &body).with_context(|| format!("writing {}", target.display()))?;
        self.remove(&entry.id)
    }

    /// Load every valid entry. Unparsable files are quarantined instead of
    /// aborting startup, so one corrupt file cannot wedge the daemon.
    pub fn load_all(&self) -> Result<HashMap<String, QueueEntry>> {
        let mut entries = HashMap::new();
        let dir = std::fs::read_dir(&self.queue_dir)
            .with_context(|| format!("reading {}", self.queue_dir.display()))?;
        for item in dir {
            let path = item?.path();
            let is_json = path.extension().and_then(|e| e.to_str()) == Some("json");
            let is_temp = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with('.'));
            if !is_json || is_temp {
                if is_temp {
                    let _ = std::fs::remove_file(&path); // leftover from a crash
                }
                continue;
            }
            match std::fs::read(&path)
                .map_err(anyhow::Error::from)
                .and_then(|body| {
                    serde_json::from_slice::<QueueEntry>(&body).map_err(anyhow::Error::from)
                }) {
                Ok(entry) => {
                    entries.insert(entry.id.clone(), entry);
                }
                Err(err) => {
                    warn!(path = %path.display(), error = %err, "quarantining unreadable queue entry");
                    let quarantine = self.failed_dir.join(
                        path.file_name()
                            .map(|n| n.to_owned())
                            .unwrap_or_else(|| "corrupt.json".into()),
                    );
                    let _ = std::fs::rename(&path, &quarantine);
                }
            }
        }
        Ok(entries)
    }

    /// Number of entries parked in the `failed` directory.
    pub fn failed_count(&self) -> usize {
        std::fs::read_dir(&self.failed_dir)
            .map(|dir| dir.flatten().filter(|e| e.path().is_file()).count())
            .unwrap_or(0)
    }
}

/// Current wall-clock time in whole seconds since the Unix epoch.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Pick the entry to attempt next: highest priority first, then oldest.
/// Returns `None` when nothing is due at `now`.
pub fn next_due(entries: &HashMap<String, QueueEntry>, now: u64) -> Option<&QueueEntry> {
    entries
        .values()
        .filter(|entry| entry.is_due(now))
        .min_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.created_at.cmp(&b.created_at))
                .then(a.id.cmp(&b.id))
        })
}

/// Seconds to wait before the earliest not-yet-due entry becomes due.
pub fn seconds_until_next(entries: &HashMap<String, QueueEntry>, now: u64) -> Option<u64> {
    entries
        .values()
        .map(|entry| entry.next_attempt_at.saturating_sub(now))
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_VERSION;

    fn request(text: &str, priority: Priority) -> NotifyRequest {
        NotifyRequest {
            version: PROTOCOL_VERSION,
            text: text.into(),
            title: None,
            target: None,
            format: None,
            priority,
            silent: None,
            source: None,
        }
    }

    #[test]
    fn entries_survive_a_store_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let entry = QueueEntry::new(request("hello", Priority::Normal), "default".into());
        queue.store(&entry).unwrap();

        let loaded = queue.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[&entry.id].message.text, "hello");

        queue.remove(&entry.id).unwrap();
        assert!(queue.load_all().unwrap().is_empty());
    }

    #[test]
    fn failed_entries_move_aside_instead_of_vanishing() {
        let dir = tempfile::tempdir().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let entry = QueueEntry::new(request("doomed", Priority::Normal), "default".into());
        queue.store(&entry).unwrap();
        queue.fail(&entry).unwrap();

        assert!(queue.load_all().unwrap().is_empty());
        assert_eq!(queue.failed_count(), 1);
    }

    #[test]
    fn corrupt_files_are_quarantined_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        std::fs::write(queue.queue_dir().join("broken.json"), b"{ not json").unwrap();
        let good = QueueEntry::new(request("fine", Priority::Normal), "default".into());
        queue.store(&good).unwrap();

        let loaded = queue.load_all().unwrap();
        assert_eq!(loaded.len(), 1, "the valid entry still loads");
        assert_eq!(queue.failed_count(), 1, "the corrupt one was moved aside");
    }

    #[test]
    fn scheduling_prefers_priority_then_age() {
        let mut entries = HashMap::new();
        let mut old_normal = QueueEntry::new(request("old", Priority::Normal), "d".into());
        old_normal.created_at = 100;
        let mut new_high = QueueEntry::new(request("urgent", Priority::High), "d".into());
        new_high.created_at = 200;
        let mut future = QueueEntry::new(request("later", Priority::High), "d".into());
        future.created_at = 50;
        future.next_attempt_at = now_secs() + 600;

        for entry in [old_normal, new_high.clone(), future] {
            entries.insert(entry.id.clone(), entry);
        }

        let picked = next_due(&entries, now_secs()).unwrap();
        assert_eq!(picked.id, new_high.id, "high priority wins over age");

        // With nothing due, the scheduler reports how long to sleep.
        let only_future: HashMap<_, _> = entries
            .iter()
            .filter(|(_, e)| !e.is_due(now_secs()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert!(next_due(&only_future, now_secs()).is_none());
        assert!(seconds_until_next(&only_future, now_secs()).unwrap() > 0);
    }
}
