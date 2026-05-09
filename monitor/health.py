"""Liveness HTTP endpoint for the sidecar watchdog.

Mirrors crypto-trader's /health contract on a different port (8766) so a
single watchdog can poll both. mark_alive() is wired into APScheduler's
EVENT_JOB_EXECUTED so any healthy scheduler activity refreshes the timestamp.
A 10-min staleness on /health → HTTP 503 → watchdog kills + restarts the bot.
"""
import json
import logging
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

logger = logging.getLogger(__name__)

HEALTH_PORT = 8766
STALE_SECONDS = 600  # 10 min — return 503 after this so watchdog restarts us

_state = {
    "last_tick_ts": time.time(),
    "last_job_id": None,
    "started_at": datetime.now(timezone.utc).isoformat(),
}
_lock = threading.Lock()


def mark_alive(source: str = ""):
    with _lock:
        _state["last_tick_ts"] = time.time()
        if source:
            _state["last_job_id"] = source


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, data: dict, code: int = 200):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path != "/health":
            self._send({"error": "not found"}, 404)
            return
        with _lock:
            state = dict(_state)
        elapsed = time.time() - state["last_tick_ts"]
        is_alive = elapsed < STALE_SECONDS
        self._send({
            "status": "ok" if is_alive else "stale",
            "last_job_id": state["last_job_id"],
            "seconds_since_last_tick": int(elapsed),
            "started_at": state["started_at"],
        }, code=200 if is_alive else 503)


def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"poly-trader health server on http://0.0.0.0:{HEALTH_PORT}/health")
