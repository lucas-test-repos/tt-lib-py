from fastapi import FastAPI
from fastapi.testclient import TestClient

from tt_lib.health import health_router


def test_health_returns_ok():
    app = FastAPI()
    app.include_router(health_router("admin-user-service"))

    res = TestClient(app).get("/health")

    assert res.status_code == 200
    assert res.json() == {"status": "ok", "service": "admin-user-service"}
