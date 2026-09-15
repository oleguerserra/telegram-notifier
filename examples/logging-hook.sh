# Minimal library for shell scripts: notify() sends a line to Telegram and
# never fails the caller, so a broken notifier cannot break a backup job.
#
#   . /usr/share/telegram-notifier/examples/logging-hook.sh
#   notify "Backup started"
#   run_backup || notify_high "Backup FAILED"

notify() {
    telegram-notify --quiet --source "${0##*/}" "$@" || true
}

notify_high() {
    telegram-notify --quiet --priority high --source "${0##*/}" "$@" || true
}

notify_silent() {
    telegram-notify --quiet --silent --priority low --source "${0##*/}" "$@" || true
}
