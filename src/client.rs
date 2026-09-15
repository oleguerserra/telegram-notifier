//! Blocking client used by `telegram-notify`.
//!
//! Deliberately synchronous and dependency-light: the CLI is meant to be
//! usable from cron jobs and shell scripts where startup time matters.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::protocol::{Request, Response};

/// Send one request over the control socket and return the daemon's answer.
pub fn send_request(socket: &Path, request: &Request, timeout: Duration) -> Result<Response> {
    let stream = UnixStream::connect(socket).with_context(|| {
        format!(
            "connecting to {}: is telegram-notifier running and are you in its socket group?",
            socket.display()
        )
    })?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let mut writer = stream.try_clone().context("cloning socket handle")?;
    let mut line = serde_json::to_vec(request)?;
    line.push(b'\n');
    writer.write_all(&line).context("sending request")?;
    writer.flush()?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    let read = reader
        .read_line(&mut response)
        .context("reading response")?;
    if read == 0 {
        bail!("daemon closed the connection without answering");
    }
    serde_json::from_str(&response)
        .with_context(|| format!("parsing daemon response: {}", response.trim()))
}
