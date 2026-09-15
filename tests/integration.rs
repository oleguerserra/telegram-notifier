//! End-to-end tests: a client request on the socket must reach the Telegram
//! API, and must survive the daemon being unable to reach it.
//!
//! A minimal HTTP server stands in for api.telegram.org so the tests are
//! hermetic and need no token.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use telegram_notifier::config::Config;
use telegram_notifier::queue::Queue;
use telegram_notifier::telegram::TelegramClient;
use telegram_notifier::{server, worker};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::Notify;

/// Captured request bodies plus a scripted reply, so a test can make the fake
/// API fail before it succeeds.
#[derive(Default)]
struct FakeApi {
    bodies: Mutex<Vec<String>>,
    calls: AtomicUsize,
    /// Number of leading requests that should fail with HTTP 500.
    fail_first: usize,
}

impl FakeApi {
    fn bodies(&self) -> Vec<String> {
        self.bodies.lock().unwrap().clone()
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// Start the stand-in API and return its base URL.
async fn start_fake_api(state: Arc<FakeApi>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let mut reader = BufReader::new(&mut stream);
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 && reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                state
                    .bodies
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&body).into_owned());
                let call = state.calls.fetch_add(1, Ordering::SeqCst);

                let (status, payload) = if call < state.fail_first {
                    (
                        "500 Internal Server Error",
                        r#"{"ok":false,"error_code":500,"description":"boom"}"#,
                    )
                } else {
                    ("200 OK", r#"{"ok":true,"result":{"message_id":1}}"#)
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                    payload.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            });
        }
    });

    format!("http://{addr}")
}

struct Harness {
    _dir: tempfile::TempDir,
    socket: std::path::PathBuf,
    queue: Queue,
    api: Arc<FakeApi>,
    shutdown: Arc<Notify>,
}

async fn start_daemon(fail_first: usize) -> Harness {
    let api = Arc::new(FakeApi {
        fail_first,
        ..Default::default()
    });
    let api_url = start_fake_api(Arc::clone(&api)).await;
    let dir = tempfile::tempdir().unwrap();

    let raw = format!(
        r#"
[service]
socket_path = "{socket}"
# Empty: never chown, so the test does not depend on host groups.
socket_group = ""
state_dir = "{state}"
min_send_interval_ms = 0

[telegram]
bot_token = "1:testtoken"
api_base_url = "{api}"
timeout_seconds = 5

[retry]
max_attempts = 5
initial_backoff_seconds = 1
max_backoff_seconds = 1
backoff_multiplier = 1.0

[defaults]
target = "default"

[targets.default]
chat_id = "111"

[targets.ops]
chat_id = "-100222"
prefix = "[ops] "
"#,
        socket = dir.path().join("notifier.sock").display(),
        state = dir.path().display(),
        api = api_url,
    );

    let config: Arc<Config> = Arc::new(toml::from_str(&raw).unwrap());
    let queue = Queue::open(&config.service.state_dir).unwrap();
    let client = TelegramClient::new(&config.telegram).unwrap();
    let shutdown = Arc::new(Notify::new());
    let (handle, _worker) = worker::spawn(
        Arc::clone(&config),
        queue.clone(),
        client,
        Arc::clone(&shutdown),
    )
    .unwrap();
    let listener = server::bind(&config).unwrap();
    let socket = config.service.socket_path.clone();
    tokio::spawn(server::serve(
        listener,
        config,
        queue.clone(),
        handle,
        Arc::clone(&shutdown),
    ));

    Harness {
        _dir: dir,
        socket,
        queue,
        api,
        shutdown,
    }
}

async fn send(socket: &std::path::Path, line: &str) -> String {
    let stream = UnixStream::connect(socket).await.unwrap();
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

/// Poll until `check` holds or the deadline passes.
async fn eventually<F: Fn() -> bool>(check: F, what: &str) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test]
async fn a_message_reaches_the_api_and_leaves_the_queue() {
    let h = start_daemon(0).await;

    let response = send(&h.socket, r#"{"action":"notify","text":"hello world"}"#).await;
    assert!(response.contains("\"queued\""), "got {response}");

    eventually(|| h.api.calls() >= 1, "the API to be called").await;

    let body = h.api.bodies().remove(0);
    let payload: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(payload["chat_id"], "111");
    assert_eq!(payload["text"], "hello world");
    assert_eq!(payload["disable_notification"], false);

    eventually(
        || h.queue.load_all().unwrap().is_empty(),
        "the delivered entry to be removed from disk",
    )
    .await;
    assert_eq!(h.queue.failed_count(), 0);

    h.shutdown.notify_waiters();
}

#[tokio::test]
async fn target_settings_are_applied_to_the_payload() {
    let h = start_daemon(0).await;

    let response = send(
        &h.socket,
        r#"{"action":"notify","text":"body","title":"Alert","target":"ops","format":"html","silent":true}"#,
    )
    .await;
    assert!(response.contains("\"queued\""), "got {response}");

    eventually(|| h.api.calls() >= 1, "the API to be called").await;
    let payload: serde_json::Value = serde_json::from_str(&h.api.bodies()[0]).unwrap();

    assert_eq!(payload["chat_id"], "-100222");
    assert_eq!(payload["parse_mode"], "HTML");
    assert_eq!(payload["disable_notification"], true);
    assert_eq!(
        payload["text"], "[ops] <b>Alert</b>\nbody",
        "prefix and escaped title are applied"
    );

    h.shutdown.notify_waiters();
}

#[tokio::test]
async fn a_failing_api_is_retried_until_it_succeeds() {
    let h = start_daemon(2).await;

    send(&h.socket, r#"{"action":"notify","text":"eventually"}"#).await;

    eventually(|| h.api.calls() >= 3, "two failures and one success").await;
    eventually(
        || h.queue.load_all().unwrap().is_empty(),
        "the entry to be delivered",
    )
    .await;
    assert_eq!(
        h.queue.failed_count(),
        0,
        "a retried message is not a failure"
    );

    h.shutdown.notify_waiters();
}

#[tokio::test]
async fn queued_messages_survive_a_restart() {
    // Point the daemon at a port nobody is listening on, so delivery cannot
    // happen, then reopen the queue as a fresh process would.
    let dir = tempfile::tempdir().unwrap();
    let raw = format!(
        r#"
[service]
socket_path = "{socket}"
socket_group = ""
state_dir = "{state}"

[telegram]
bot_token = "1:t"
api_base_url = "http://127.0.0.1:1"

[targets.default]
chat_id = "1"
"#,
        socket = dir.path().join("s.sock").display(),
        state = dir.path().display(),
    );
    let config: Arc<Config> = Arc::new(toml::from_str(&raw).unwrap());
    let queue = Queue::open(&config.service.state_dir).unwrap();
    let shutdown = Arc::new(Notify::new());
    let (handle, _worker) = worker::spawn(
        Arc::clone(&config),
        queue.clone(),
        TelegramClient::new(&config.telegram).unwrap(),
        Arc::clone(&shutdown),
    )
    .unwrap();
    let listener = server::bind(&config).unwrap();
    let socket = config.service.socket_path.clone();
    tokio::spawn(server::serve(
        listener,
        Arc::clone(&config),
        queue.clone(),
        handle,
        Arc::clone(&shutdown),
    ));

    send(&socket, r#"{"action":"notify","text":"durable"}"#).await;
    shutdown.notify_waiters();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // What a fresh daemon start would see.
    let reopened = Queue::open(dir.path()).unwrap().load_all().unwrap();
    assert_eq!(reopened.len(), 1, "the message is still queued");
    assert_eq!(reopened.values().next().unwrap().message.text, "durable");
}

#[tokio::test]
async fn long_messages_are_split_into_several_api_calls() {
    let h = start_daemon(0).await;

    let long = "x".repeat(5000);
    let request = serde_json::json!({ "action": "notify", "text": long }).to_string();
    send(&h.socket, &request).await;

    eventually(|| h.api.calls() >= 2, "the body to be split in two").await;
    let bodies = h.api.bodies();
    let total: usize = bodies
        .iter()
        .map(|b| {
            serde_json::from_str::<serde_json::Value>(b).unwrap()["text"]
                .as_str()
                .unwrap()
                .chars()
                .count()
        })
        .sum();
    assert_eq!(total, 5000, "no characters are lost in the split");

    h.shutdown.notify_waiters();
}

#[tokio::test]
async fn flush_returns_once_the_queue_has_drained() {
    let h = start_daemon(0).await;

    send(&h.socket, r#"{"action":"notify","text":"before shutdown"}"#).await;
    // A shutdown hook cannot proceed until this comes back.
    let response = send(&h.socket, r#"{"action":"flush","timeout_ms":10000}"#).await;

    assert!(response.contains("\"flushed\""), "got {response}");
    assert!(response.contains("\"pending\":0"), "got {response}");
    assert!(response.contains("\"timed_out\":false"), "got {response}");
    assert!(h.api.calls() >= 1, "the message really went out");
    assert!(h.queue.load_all().unwrap().is_empty());

    h.shutdown.notify_waiters();
}

#[tokio::test]
async fn flush_reports_what_it_could_not_deliver() {
    // fail_first is effectively unlimited here: every attempt 500s, so the
    // message stays queued and flush must give up rather than hang.
    let h = start_daemon(usize::MAX).await;

    send(&h.socket, r#"{"action":"notify","text":"doomed"}"#).await;
    let started = std::time::Instant::now();
    let response = send(&h.socket, r#"{"action":"flush","timeout_ms":300}"#).await;

    assert!(response.contains("\"timed_out\":true"), "got {response}");
    assert!(response.contains("\"pending\":1"), "got {response}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "flush must honour its deadline, took {:?}",
        started.elapsed()
    );

    h.shutdown.notify_waiters();
}

#[tokio::test]
async fn flush_on_an_empty_queue_returns_immediately() {
    let h = start_daemon(0).await;

    let started = std::time::Instant::now();
    let response = send(&h.socket, r#"{"action":"flush","timeout_ms":30000}"#).await;

    assert!(response.contains("\"pending\":0"), "got {response}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "nothing to wait for, took {:?}",
        started.elapsed()
    );

    h.shutdown.notify_waiters();
}
