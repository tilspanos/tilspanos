"""Structured logging + the Breads-style activity log ring buffer.

Every decision the fleet makes is visible: BOOK, SEND, RESP, ORDER, FILL,
CLOSE, RISK, WATCHDOG lines go to stdout AND into an in-memory ring buffer
that the dashboard streams live.
"""

from __future__ import annotations

import logging
import sys
import time
from collections import deque
from typing import Deque

LOG_FORMAT = "%(asctime)s | %(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
log = logging.getLogger("fleet")


class ActivityLog:
    """Ring buffer of user-facing events, consumed by the dashboard."""

    def __init__(self, maxlen: int = 500) -> None:
        self.events: Deque[dict] = deque(maxlen=maxlen)
        self._seq = 0

    def add(self, level: str, tag: str, symbol: str, message: str) -> dict:
        """level: 'ok' | 'warn' | 'err'   tag: BOOK/SEND/RESP/ORDER/FILL/..."""
        self._seq += 1
        event = {
            "seq": self._seq,
            "ts": time.time(),
            "level": level,
            "tag": tag,
            "symbol": symbol,
            "message": message,
        }
        self.events.append(event)
        icon = {"ok": "\u2713", "warn": "\u26a0", "err": "\u2717"}.get(level, "\u00b7")
        log.info("%-5s | %-8s | %s %s", tag, symbol or "-", icon, message)
        return event

    def ok(self, tag: str, symbol: str, message: str) -> dict:
        return self.add("ok", tag, symbol, message)

    def warn(self, tag: str, symbol: str, message: str) -> dict:
        return self.add("warn", tag, symbol, message)

    def err(self, tag: str, symbol: str, message: str) -> dict:
        return self.add("err", tag, symbol, message)

    def since(self, seq: int, limit: int = 200) -> list[dict]:
        return [e for e in self.events if e["seq"] > seq][-limit:]

    def tail(self, limit: int = 100) -> list[dict]:
        return list(self.events)[-limit:]


activity = ActivityLog()
