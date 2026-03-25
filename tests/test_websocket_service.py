import asyncio
import json
import socket
import subprocess
import sys
import time

import httpx
import pytest
import websockets

from services.websocket_service import main as ws_main


def _app_has_ws_route() -> bool:
    routes = getattr(ws_main.app, "routes", []) or []
    for r in routes:
        path = getattr(r, "path", "") or ""
        if path.startswith("/ws/") or "/ws/{job_id}" in path:
            return True
    return False


def test_websocket_broadcast_status_roundtrip():
    """
    Ensure the WebSocket microservice can:
    - accept a native WS connection at /ws/{job_id}
    - broadcast a JSON payload via POST /broadcast/{job_id}
    - deliver the payload to the WS client
    """
    if not _app_has_ws_route():
        pytest.skip("WebSocket route /ws/{job_id} not available in this runtime")

    job_id = "pytest_job_ws_1"
    port = _find_free_port()

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "services.websocket_service.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]
    proc: subprocess.Popen[str] | None = None

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # Wait for /health to come up
        health_url = f"http://127.0.0.1:{port}/health"
        for _ in range(40):
            try:
                r = httpx.get(health_url, timeout=0.5)
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.25)
        else:
            output = ""
            if proc and proc.stdout:
                try:
                    output = proc.stdout.read(2000)
                except Exception:
                    output = ""
            pytest.fail(f"WebSocket service did not start on port {port}. Output: {output}")

        async def _run() -> None:
            ws_url = f"ws://127.0.0.1:{port}/ws/{job_id}"
            payload = {"type": "status", "status": "running", "progress": 10}

            async with websockets.connect(ws_url, open_timeout=5) as websocket:
                async with httpx.AsyncClient(timeout=5) as client:
                    resp = await client.post(f"http://127.0.0.1:{port}/broadcast/{job_id}", json=payload)
                    assert resp.status_code == 200

                message_text = await asyncio.wait_for(websocket.recv(), timeout=5)
                message = json.loads(message_text)

                assert message["type"] == "status"
                assert message["status"] == "running"
                assert message["progress"] == 10

        asyncio.run(_run())
    finally:
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()


def _find_free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port

