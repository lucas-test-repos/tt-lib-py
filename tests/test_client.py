"""Tests de ServiceClient contra un servidor HTTP real (sin mocks).

Se usa `http.server` en un hilo con puerto efímero, igual que las
librerías hermanas (`httptest.NewServer` en Go, Fastify en Node con
`port: 0`): ServiceClient recibe una `base_url` real y hace peticiones
de socket de verdad, ejerciendo raise_for_status() y el pool de
conexiones tal como los usarán los 20 servicios generados.
"""

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest

from tt_lib.client import ServiceClient


class _Handler(BaseHTTPRequestHandler):
    routes: dict[str, Callable[["_Handler"], None]] = {}

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # silencia el log de acceso del servidor de test

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()

    def _dispatch(self) -> None:
        handler = self.routes.get(self.path)
        if handler is None:
            self.send_response(404)
            self.end_headers()
            return
        handler(self)

    def _send_json(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")


@pytest.fixture
def run_server():
    """Levanta un http.server real en 127.0.0.1 con puerto efímero por test."""
    server: HTTPServer | None = None

    def _run(routes: dict[str, Callable[[_Handler], None]]) -> str:
        nonlocal server
        handler_cls = type("_TestHandler", (_Handler,), {"routes": routes})
        server = HTTPServer(("127.0.0.1", 0), handler_cls)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        port = server.server_address[1]
        return f"http://127.0.0.1:{port}"

    yield _run

    if server is not None:
        server.shutdown()
        server.server_close()


def test_get_json_decodes_response(run_server):
    def handle_get(handler: _Handler) -> None:
        handler._send_json(200, {"count": 42})

    base_url = run_server({"/api/v1/seat/available": handle_get})

    out = ServiceClient(base_url).get_json("/api/v1/seat/available")

    assert out == {"count": 42}


def test_post_json_sends_body_and_returns_response(run_server):
    def handle_post(handler: _Handler) -> None:
        body = handler._read_json()
        handler._send_json(200, {"reserved": body["seat"]})

    base_url = run_server({"/api/v1/seat/reserve": handle_post})

    out = ServiceClient(base_url).post_json("/api/v1/seat/reserve", {"seat": 7})

    assert out == {"reserved": 7}


def test_get_json_raises_with_url_on_server_error(run_server):
    def handle_get(handler: _Handler) -> None:
        handler.send_response(500)
        handler.end_headers()

    base_url = run_server({"/boom": handle_get})

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        ServiceClient(base_url).get_json("/boom")

    assert f"{base_url}/boom" in str(exc_info.value)
