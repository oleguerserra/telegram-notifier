//! Unix domain socket server.
//!
//! Access control is the socket's own file mode: the daemon creates it owned
//! by `root:<socket_group>` with mode 0660, so membership in that group is
//! what grants the right to send notifications.

use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use nix::unistd::{chown, Gid, Group};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::protocol::{Request, Response, StatusReport, MAX_REQUEST_BYTES};
use crate::queue::{Queue, QueueEntry};
use crate::worker::WorkerHandle;

/// Bind the control socket, applying ownership and permissions.
pub fn bind(config: &Config) -> Result<UnixListener> {
    let path = &config.service.socket_path;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating socket directory {}", parent.display()))?;
    }

    // A socket left behind by a crash would make bind() fail with EADDRINUSE.
    match std::fs::metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            std::fs::remove_file(path)
                .with_context(|| format!("removing stale socket {}", path.display()))?;
        }
        Ok(_) => anyhow::bail!(
            "{} exists and is not a socket; refusing to replace it",
            path.display()
        ),
        Err(_) => {}
    }

    let listener = UnixListener::bind(path)
        .with_context(|| format!("binding control socket {}", path.display()))?;

    apply_socket_permissions(path, &config.service.socket_group, config.socket_mode()?)?;
    info!(
        socket = %path.display(),
        group = %config.service.socket_group,
        "listening for notification requests"
    );
    Ok(listener)
}

/// Set the socket's group and mode. A missing group is a warning rather than
/// a fatal error, so a misconfigured group cannot take the service down.
fn apply_socket_permissions(path: &Path, group: &str, mode: u32) -> Result<()> {
    match Group::from_name(group) {
        Ok(Some(entry)) => {
            chown(path, None, Some(Gid::from_raw(entry.gid.as_raw())))
                .with_context(|| format!("setting group {group} on {}", path.display()))?;
        }
        Ok(None) => warn!(
            group,
            "group does not exist; the socket keeps the daemon's primary group"
        ),
        Err(err) => warn!(group, error = %err, "could not look up socket group"),
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting mode {mode:o} on {}", path.display()))?;
    Ok(())
}

/// Accept connections until `shutdown` fires.
pub async fn serve(
    listener: UnixListener,
    config: Arc<Config>,
    queue: Queue,
    worker: WorkerHandle,
    shutdown: Arc<Notify>,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                info!("socket server stopping");
                return;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let ctx = ConnectionContext {
                            config: Arc::clone(&config),
                            queue: queue.clone(),
                            worker: worker.clone(),
                        };
                        tokio::spawn(async move {
                            if let Err(err) = handle_connection(stream, ctx).await {
                                debug!(error = %err, "connection ended with an error");
                            }
                        });
                    }
                    Err(err) => {
                        error!(error = %err, "accept failed");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct ConnectionContext {
    config: Arc<Config>,
    queue: Queue,
    worker: WorkerHandle,
}

async fn handle_connection(stream: UnixStream, ctx: ConnectionContext) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        line.clear();
        let read = (&mut reader)
            .take(MAX_REQUEST_BYTES as u64)
            .read_line(&mut line)
            .await?;
        if read == 0 {
            return Ok(()); // client closed the connection
        }
        if line.trim().is_empty() {
            continue;
        }
        if read >= MAX_REQUEST_BYTES {
            let response = Response::Error {
                message: format!("request exceeds {MAX_REQUEST_BYTES} bytes"),
            };
            write_response(&mut write_half, &response).await?;
            return Ok(());
        }

        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) => dispatch(request, &ctx).await,
            Err(err) => Response::Error {
                message: format!("malformed request: {err}"),
            },
        };
        write_response(&mut write_half, &response).await?;
    }
}

async fn write_response<W>(writer: &mut W, response: &Response) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let mut body = serde_json::to_vec(response)?;
    body.push(b'\n');
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

async fn dispatch(request: Request, ctx: &ConnectionContext) -> Response {
    match request {
        Request::Ping => Response::Pong,
        Request::Status => Response::Status(StatusReport {
            version: env!("CARGO_PKG_VERSION").to_string(),
            pending: ctx.worker.pending(),
            failed: ctx.queue.failed_count(),
            delivered_since_start: ctx.worker.delivered(),
            targets: ctx.config.targets.keys().cloned().collect(),
            default_target: ctx.config.defaults.target.clone(),
        }),
        Request::Notify(notify) => {
            if let Err(message) = notify.validate() {
                return Response::Error { message };
            }
            let target_name = notify
                .target
                .clone()
                .unwrap_or_else(|| ctx.config.defaults.target.clone());
            if !ctx.config.targets.contains_key(&target_name) {
                return Response::Error {
                    message: format!(
                        "unknown target \"{target_name}\"; configured targets: {}",
                        ctx.config
                            .targets
                            .keys()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                };
            }

            let entry = QueueEntry::new(notify, target_name);
            // Persist before acknowledging: a client that got "queued" must be
            // able to rely on the message surviving a restart.
            if let Err(err) = ctx.queue.store(&entry) {
                error!(id = %entry.id, error = %err, "could not persist message");
                return Response::Error {
                    message: format!("could not persist message: {err}"),
                };
            }
            if let Err(message) = ctx.worker.submit(entry.clone()).await {
                let _ = ctx.queue.remove(&entry.id);
                return Response::Error { message };
            }
            Response::Queued { id: entry.id }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telegram::TelegramClient;
    use std::time::Duration;

    fn test_config(dir: &Path) -> Config {
        let raw = format!(
            r#"
[service]
socket_path = "{}"
socket_group = "this-group-does-not-exist"
state_dir = "{}"

[telegram]
bot_token = "1:a"
api_base_url = "http://127.0.0.1:1"

[defaults]
target = "default"

[targets.default]
chat_id = "1"

[targets.ops]
chat_id = "-100"
"#,
            dir.join("notifier.sock").display(),
            dir.display()
        );
        toml::from_str(&raw).unwrap()
    }

    /// Start a daemon against a temporary directory and return its socket path.
    async fn start() -> (tempfile::TempDir, std::path::PathBuf, Arc<Notify>) {
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(test_config(dir.path()));
        let queue = Queue::open(&config.service.state_dir).unwrap();
        let client = TelegramClient::new(&config.telegram).unwrap();
        let shutdown = Arc::new(Notify::new());
        let (worker, _join) = crate::worker::spawn(
            Arc::clone(&config),
            queue.clone(),
            client,
            Arc::clone(&shutdown),
        )
        .unwrap();
        let listener = bind(&config).unwrap();
        let socket_path = config.service.socket_path.clone();
        tokio::spawn(serve(
            listener,
            config,
            queue,
            worker,
            Arc::clone(&shutdown),
        ));
        (dir, socket_path, shutdown)
    }

    async fn request(path: &Path, line: &str) -> String {
        let stream = UnixStream::connect(path).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        write_half.write_all(line.as_bytes()).await.unwrap();
        write_half.write_all(b"\n").await.unwrap();
        let mut reader = BufReader::new(read_half);
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut response))
            .await
            .expect("daemon answered in time")
            .unwrap();
        response
    }

    #[tokio::test]
    async fn ping_gets_a_pong() {
        let (_dir, path, shutdown) = start().await;
        let response = request(&path, r#"{"action":"ping"}"#).await;
        assert!(response.contains("\"pong\""), "got {response}");
        shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn a_notification_is_persisted_before_being_acknowledged() {
        let (dir, path, shutdown) = start().await;
        let response = request(
            &path,
            r#"{"action":"notify","text":"hello","target":"ops"}"#,
        )
        .await;
        assert!(response.contains("\"queued\""), "got {response}");

        // The API base points at a closed port, so nothing can be delivered:
        // the entry must still be on disk.
        let queue = Queue::open(dir.path()).unwrap();
        let stored = queue.load_all().unwrap();
        assert_eq!(stored.len(), 1);
        let entry = stored.values().next().unwrap();
        assert_eq!(entry.message.text, "hello");
        assert_eq!(entry.message.target, "ops");
        shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn unknown_targets_are_rejected_without_queueing() {
        let (dir, path, shutdown) = start().await;
        let response = request(&path, r#"{"action":"notify","text":"x","target":"nope"}"#).await;
        assert!(response.contains("\"error\""), "got {response}");
        assert!(response.contains("unknown target"), "got {response}");

        let queue = Queue::open(dir.path()).unwrap();
        assert!(queue.load_all().unwrap().is_empty());
        shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn malformed_json_does_not_kill_the_connection() {
        let (_dir, path, shutdown) = start().await;
        let stream = UnixStream::connect(&path).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half.write_all(b"{ not json\n").await.unwrap();
        let mut first = String::new();
        reader.read_line(&mut first).await.unwrap();
        assert!(first.contains("malformed request"), "got {first}");

        // The same connection still serves the next request.
        write_half
            .write_all(b"{\"action\":\"ping\"}\n")
            .await
            .unwrap();
        let mut second = String::new();
        reader.read_line(&mut second).await.unwrap();
        assert!(second.contains("\"pong\""), "got {second}");
        shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn status_reports_the_configured_targets() {
        let (_dir, path, shutdown) = start().await;
        let response = request(&path, r#"{"action":"status"}"#).await;
        assert!(response.contains("\"default\""), "got {response}");
        assert!(response.contains("\"ops\""), "got {response}");
        shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn binding_replaces_a_stale_socket_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let first = bind(&config).unwrap();
        drop(first); // simulates a crash that left the socket behind
        assert!(config.service.socket_path.exists());
        let _second = bind(&config).expect("a stale socket must not block startup");
    }
}
