#!/usr/bin/env python3
"""Send a notification from Python, speaking the socket protocol directly.

Useful when spawning a process per message is too expensive, or when the
sender already has an event loop and wants to keep the socket open.
"""
import json
import socket
import sys

SOCKET_PATH = "/run/telegram-notifier/notifier.sock"


def notify(text, target=None, priority="normal", title=None, path=SOCKET_PATH):
    request = {"action": "notify", "text": text, "priority": priority}
    if target:
        request["target"] = target
    if title:
        request["title"] = title

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(10)
        sock.connect(path)
        sock.sendall(json.dumps(request).encode() + b"\n")
        with sock.makefile("rb") as stream:
            response = json.loads(stream.readline())

    if response.get("status") != "queued":
        raise RuntimeError(response.get("message", "request rejected"))
    return response["id"]


if __name__ == "__main__":
    print(notify(" ".join(sys.argv[1:]) or "hello from python"))
