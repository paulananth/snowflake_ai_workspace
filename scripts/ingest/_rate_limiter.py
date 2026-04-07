"""
Thread-safe token-bucket rate limiter.

SEC EDGAR enforces a hard limit of 10 requests/second per IP address.
We cap at 8 req/s (20% safety margin) to avoid triggering 429s or IP bans.
"""
import time
import threading


class RateLimiter:
    """Token-bucket rate limiter. Default: 8 tokens/sec."""

    def __init__(self, rps: float = 8.0):
        self._rps = rps
        self._tokens = rps          # start full
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._rps, self._tokens + elapsed * self._rps)
            self._last = now
            if self._tokens < 1.0:
                sleep_for = (1.0 - self._tokens) / self._rps
                time.sleep(sleep_for)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0
