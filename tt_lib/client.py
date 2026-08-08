"""Cliente HTTP para llamar a otros servicios de la plataforma."""

from typing import Any

import httpx


class ServiceClient:
    """Llama a otro servicio a partir de su URL base.

    Se instancia una vez por dependencia (p. ej. una vez al arrancar el
    servicio o en el constructor de un cliente inyectado) y se reutiliza
    para todas las peticiones — no se crea una instancia nueva por request.
    Mantiene un `httpx.Client` abierto de por vida con su pool de conexiones,
    que es lo correcto para un servicio de vida larga siempre que no se
    reconstruya en cada llamada.
    """

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def get_json(self, path: str) -> Any:
        res = self._client.get(path)
        res.raise_for_status()
        return res.json()

    def post_json(self, path: str, body: Any) -> Any:
        res = self._client.post(path, json=body)
        res.raise_for_status()
        return res.json()
