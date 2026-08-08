"""Endpoint de salud común a todos los servicios Python."""

from fastapi import APIRouter


def health_router(service_name: str) -> APIRouter:
    """Devuelve un router con GET /health para el servicio indicado."""
    router = APIRouter()

    @router.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": service_name}

    return router
