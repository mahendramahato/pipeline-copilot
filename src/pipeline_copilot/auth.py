"""Owner-only access to live mode: one password, a signed session cookie.

The password comes from COPILOT_PASSWORD. A successful login sets an HttpOnly
cookie holding an expiry time and an HMAC signature (keyed by COPILOT_SECRET),
so no session state is stored on the server. Public visitors only ever see the
showcase; asking new questions (which spends API credits and reads pipeline
data) requires this cookie.
"""
import hashlib
import hmac
import os
import secrets
import time
from collections import defaultdict, deque
from functools import cache

COOKIE = "copilot_session"
SESSION_SECONDS = 7 * 24 * 3600

# Brute-force protection: per-client and global limits on failed logins
WINDOW_SECONDS = 15 * 60
MAX_FAILURES_PER_CLIENT = 5
MAX_FAILURES_TOTAL = 50

_failures: defaultdict[str, deque] = defaultdict(deque)
_all_failures: deque = deque()

# Read on first use, not at import: .env is loaded after this module is imported.
# Without COPILOT_SECRET a random key is used, so sessions end on restart.
@cache
def _secret() -> bytes:
    return (os.environ.get("COPILOT_SECRET") or secrets.token_hex(32)).encode()


def auth_enabled() -> bool:
    return bool(os.environ.get("COPILOT_PASSWORD"))


def _sign(payload: str) -> str:
    return hmac.new(_secret(), payload.encode(), hashlib.sha256).hexdigest()


def new_session_token() -> str:
    expires = str(int(time.time()) + SESSION_SECONDS)
    return f"{expires}.{_sign(expires)}"


def is_valid_session(token: str | None) -> bool:
    if not token or "." not in token:
        return False
    expires, signature = token.split(".", 1)
    # compare_digest: constant-time, so timing can't leak the right signature
    return (
        expires.isdigit()
        and int(expires) > time.time()
        and hmac.compare_digest(signature, _sign(expires))
    )


def _prune(q: deque, now: float) -> None:
    while q and q[0] < now - WINDOW_SECONDS:
        q.popleft()


def login_blocked(client: str) -> bool:
    now = time.time()
    _prune(_failures[client], now)
    _prune(_all_failures, now)
    return len(_failures[client]) >= MAX_FAILURES_PER_CLIENT or len(_all_failures) >= MAX_FAILURES_TOTAL


def check_password(client: str, password: str) -> bool:
    ok = auth_enabled() and hmac.compare_digest(password.encode(), os.environ["COPILOT_PASSWORD"].encode())
    if not ok:
        now = time.time()
        _failures[client].append(now)
        _all_failures.append(now)
    return ok
