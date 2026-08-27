from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock

EVENTS: list[dict[str, object]] = []
EVENTS_LOCK = Lock()


class Handler(BaseHTTPRequestHandler):
    def _json_response(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._json_response(200, {"status": "ok"})
            return
        if self.path == "/events":
            with EVENTS_LOCK:
                self._json_response(200, EVENTS)
            return
        self._json_response(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/alerts":
            self._json_response(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length))
        except (ValueError, json.JSONDecodeError):
            self._json_response(400, {"error": "invalid JSON"})
            return
        if not isinstance(payload, dict):
            self._json_response(400, {"error": "payload must be an object"})
            return
        with EVENTS_LOCK:
            EVENTS.append(payload)
        self._json_response(202, {"accepted": True})

    def log_message(self, _format: str, *_args: object) -> None:
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
