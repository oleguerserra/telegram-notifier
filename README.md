# telegram-notifier

A small Debian service that relays notifications from local programs to
Telegram. Other services and shell scripts hand it a message over a Unix
socket; it queues the message to disk, then delivers it to the Telegram Bot
API with retries and rate limiting.

```
┌──────────────┐  JSON over a   ┌────────────────────┐  HTTPS  ┌──────────┐
│ any service  │───────────────▶│ telegram-notifierd │────────▶│ Telegram │
│ or CLI user  │  Unix socket   │  persistent queue  │         │  Bot API │
└──────────────┘                └────────────────────┘         └──────────┘
```

## Why a daemon instead of a curl one-liner

* **Nothing is lost.** A message is on disk before the sender is told it was
  accepted, so a reboot or a network outage does not swallow an alert.
* **The token lives in one place.** Senders never see it, and never need
  network access of their own.
* **Rate limits are handled once.** Telegram's `429` replies are honoured
  centrally instead of by every caller.
* **Access is a group membership.** The socket is `root:telegram-notify`
  mode `0660`; `adduser someone telegram-notify` is the whole permission
  model.

## Components

| Path | What it is |
| --- | --- |
| `/usr/bin/telegram-notifierd` | The daemon. Runs as the `telegram-notifier` system user under systemd. |
| `/usr/bin/telegram-notify` | Command line client. |
| `/etc/telegram-notifier/config.toml` | Configuration (dpkg conffile). |
| `/etc/telegram-notifier/bot-token` | The bot token, `0640 root:telegram-notifier`. |
| `/run/telegram-notifier/notifier.sock` | Control socket. |
| `/var/lib/telegram-notifier/queue` | Pending messages, one JSON file each. |
| `/var/lib/telegram-notifier/failed` | Abandoned messages, kept for inspection. |
| `/usr/share/telegram-notifier/examples/` | systemd `OnFailure=` handler, shell helpers, a Python client. |

## Installing

Build the package and install it with `apt`, which pulls in the two runtime
dependencies (`adduser`, `ca-certificates`) for you:

```sh
dpkg-buildpackage -us -uc -b
sudo apt install ../telegram-notifier_0.1.0-1_amd64.deb
```

See [Building from source](#building-from-source) for the build dependencies.

Installing creates the `telegram-notify` group, the `telegram-notifier` system
user and `/var/lib/telegram-notifier`. **The service is not started
automatically** — it has no token and no chat id yet, so there would be nothing
for it to do. The next two sections fix that.

## Creating the bot and finding your chat id

Talk to [@BotFather](https://t.me/BotFather) in Telegram and send `/newbot`. It
asks for a display name (`Notifications db01`) and then a username, which must
end in `bot` (`db01_notify_bot`). It replies with a token shaped like
`123456789:AAH...`.

Check the token before going any further. `telegram-notifierd --check`
validates the configuration file but never contacts Telegram, so a
syntactically fine but wrong token passes it:

```sh
curl -s "https://api.telegram.org/bot<TOKEN>/getMe"
```

`"ok": true` and your bot's name means the token is good; `401 Unauthorized`
means it was copied wrong.

Now the chat id. Send the bot a message — press *Start* in a private chat, or
post in the group or channel you want alerts in — and then ask for the pending
updates:

```sh
curl -s "https://api.telegram.org/bot<TOKEN>/getUpdates" | python3 -c '
import json, sys
for u in json.load(sys.stdin)["result"]:
    c = (u.get("message") or u.get("channel_post") or {}).get("chat")
    if c:
        print(c["id"], c["type"], c.get("title") or c.get("username") or c.get("first_name"))'
```

Three things trip people up here:

* **Groups.** Bots have privacy mode on by default and do not see ordinary
  group messages, so the list comes back empty. Either send `/start@yourbot` in
  the group — bots always see commands aimed at them — or turn privacy off with
  `/setprivacy` in @BotFather. Group ids are negative, and supergroup ids start
  with `-100`.
* **Channels.** Make the bot an administrator and post something; the update
  arrives as `channel_post` rather than `message`. A public channel can also
  use `@channelname` directly as its `chat_id`.
* **Webhooks.** If the bot has a webhook configured, `getUpdates` fails with a
  409 — clear it with `deleteWebhook` first. Updates also expire after 24 hours
  and are consumed once read, so send a fresh message and retry if the list is
  empty.

## Configuring and starting

Put the token in its own file rather than inline in the configuration, so the
secret has its own permissions:

```sh
printf '%s\n' '123456789:AAH...' | sudo tee /etc/telegram-notifier/bot-token >/dev/null
sudo chown root:telegram-notifier /etc/telegram-notifier/bot-token
sudo chmod 0640 /etc/telegram-notifier/bot-token
```

Then edit `/etc/telegram-notifier/config.toml` and replace the placeholder chat
id with the one you just found:

```toml
[targets.default]
chat_id = "123456789"
```

Validate, start, and grant yourself the right to send:

```sh
sudo telegram-notifierd --check     # expect: configuration OK: 1 target(s), ...
sudo systemctl enable --now telegram-notifier
sudo adduser "$USER" telegram-notify
```

A new group does not apply to a session that is already open. Log out and back
in, or start a shell that has it right away:

```sh
newgrp telegram-notify
telegram-notify --ping              # expect: pong
telegram-notify "It works"
```

If `--ping` reports a permission error, that is the group: check that `id -nG`
lists `telegram-notify`.

## Using it

```sh
telegram-notify "Backup finished"
telegram-notify --target ops --priority high "Disk almost full on db01"
journalctl -u nginx -n 20 | telegram-notify --title "nginx" --stdin
telegram-notify --status          # queue depth and configured targets
telegram-notify --ping            # is the daemon alive?
```

From a cron job or a script, where the notifier must never break the job:

```sh
/usr/local/bin/backup || telegram-notify -p high "backup failed on $(hostname)" || true
```

As a systemd failure handler, on any unit:

```ini
[Unit]
OnFailure=telegram-notify-failure@%n.service
```

Copy `examples/telegram-notify-failure@.service` to `/etc/systemd/system/`
first. Full details in `telegram-notify(1)`.

### Named targets

Different services can reach different chats through the same daemon:

```toml
[defaults]
target = "default"

[targets.default]
chat_id = "123456789"

[targets.ops]
chat_id = "-1001234567890"
message_thread_id = 42     # a forum topic inside the supergroup
prefix = "[db01] "

[targets.quiet]
chat_id = "123456789"
silent = true
```

Each target may also carry its own bot token, format and link-preview
setting. See `telegram-notifier.toml(5)`.

## Talking to the daemon directly

The protocol is one JSON object per line in each direction; the connection
stays open for further requests. Anything that can write to a Unix socket can
use it — see `examples/send.py`.

```sh
$ printf '{"action":"notify","text":"hello","target":"ops"}\n' \
    | socat - UNIX-CONNECT:/run/telegram-notifier/notifier.sock
{"status":"queued","id":"0f1c…"}
```

Requests: `{"action":"notify", ...}`, `{"action":"ping"}`,
`{"action":"status"}`. A `notify` request takes `text` plus optional `title`,
`target`, `format`, `priority`, `silent` and `source`. Responses carry a
`status` field of `queued`, `pong`, `status` or `error`.

`queued` means the message is on disk and will be retried until it is
delivered or abandoned — it does not mean Telegram has already accepted it.

## Delivery behaviour

* One worker sends at a time, highest priority first, oldest first within a
  priority, with at least `min_send_interval_ms` between API calls.
* Retryable failures (network errors, Telegram 5xx, `429`) back off
  exponentially from `initial_backoff_seconds` up to `max_backoff_seconds`.
  A `429` uses the exact delay Telegram asks for.
* Permanent failures (bad token, unknown chat, malformed markup) are not
  retried.
* Abandoned messages move to `/var/lib/telegram-notifier/failed` with their
  last error. Nothing deletes them for you.
* Bodies longer than Telegram's 4096-character limit are split, preferring
  line boundaries.

## Troubleshooting

```sh
journalctl -u telegram-notifier -f        # every delivery and every error
telegram-notify --status                  # pending, failed, delivered so far
sudo ls -l /var/lib/telegram-notifier/failed/
```

Abandoned messages keep their last error inside the JSON file, so nothing is
ever lost silently. The errors you are most likely to meet:

| Symptom | Cause |
| --- | --- |
| `chat not found` | Wrong chat id, or the bot was never added to the group. |
| `401 Unauthorized` | Bad or revoked bot token. |
| `403 bot was blocked by the user` | The recipient blocked the bot. |
| `can't parse entities` | Markup does not match `format`; `plain` never fails this way. |
| Client: permission denied on the socket | The sender is not in the `telegram-notify` group. |
| Client: connection refused | The daemon is not running — check `systemctl status`. |
| Build: `No such file or directory` all over `target/` | Two builds at once. `dpkg-buildpackage` starts with `debian/rules clean`, which removes the target directory; `debian/rules` now refuses the second one instead. |

## Building from source

Needs a Rust toolchain and a C toolchain (for the TLS backend):

```sh
sudo apt install --no-install-recommends build-essential cargo rustc rustfmt rust-clippy
cargo build --release
cargo test
```

The crate targets rustc 1.85, which is what Debian 13 ships, and sets
`resolver = "3"` so cargo's MSRV-aware resolver keeps transitive dependencies
within reach of it. Regenerating `Cargo.lock` with a much newer toolchain can
pin crates that the distribution's rustc cannot build.

### The Debian package

```sh
sudo apt install --no-install-recommends \
    build-essential cargo rustc debhelper fakeroot lintian devscripts
dpkg-buildpackage -us -uc -b
lintian --display-info --pedantic ../telegram-notifier_*.changes
```

`debian/rules` builds with `--locked`, and adds `--offline` automatically if a
`vendor/` directory is present. To produce a source package that builds with
no network access:

```sh
cargo vendor vendor
mkdir -p .cargo
cargo vendor vendor > .cargo/config.toml
```

## Security notes

* The socket's group is the only access control. Anyone in
  `telegram-notify` can send to any configured target.
* Queue files can contain message bodies; `/var/lib/telegram-notifier` is
  `0750` and entries are created with umask `0077`.
* Bot tokens are stripped from error messages before they are logged.
* The systemd unit runs with no capabilities, `ProtectSystem=strict`,
  a `@system-service` syscall filter and `MemoryDenyWriteExecute=yes`.
* `purge` removes the queue, the token file and the service user.

## License

GPL-3.0-or-later. See `LICENSE` and `debian/copyright`.
