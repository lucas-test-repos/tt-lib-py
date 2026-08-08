from tt_lib.config import load_config


def test_load_config_uses_environment(monkeypatch):
    monkeypatch.setenv("SERVICE_NAME", "audit-service")
    monkeypatch.setenv("PORT", "8080")
    monkeypatch.setenv("DATABASE_URL", "postgresql://db/audit")

    cfg = load_config()

    assert cfg.service_name == "audit-service"
    assert cfg.port == 8080
    assert cfg.database_url == "postgresql://db/audit"


def test_load_config_defaults(monkeypatch):
    for key in ("SERVICE_NAME", "PORT", "DATABASE_URL", "REDIS_URL"):
        monkeypatch.delenv(key, raising=False)

    cfg = load_config()

    assert cfg.service_name == "unnamed-service"
    assert cfg.port == 8080
    assert cfg.database_url is None
    assert cfg.redis_url is None


def test_load_config_treats_empty_as_unset(monkeypatch):
    """Política común a las tres librerías: variable vacía == variable ausente.

    Python ya la cumplía (`os.getenv(...) or <default>`) y Go también (el
    helper `env` de tt-lib-go compara con ""), pero Node no: con PORT="",
    `Number(process.env.PORT ?? 8080)` daba 0 — que para listen() significa
    "puerto efímero al azar", así que el servicio arrancaba en un puerto que
    nadie conoce. Este test fija la política aquí para que no se pierda.
    """
    for key in ("SERVICE_NAME", "PORT", "DATABASE_URL", "REDIS_URL"):
        monkeypatch.setenv(key, "")

    cfg = load_config()

    assert cfg.service_name == "unnamed-service"
    assert cfg.port == 8080
    assert cfg.database_url is None
    assert cfg.redis_url is None
