"""Publicación y consumo de eventos, sea cual sea el transporte.

Un canal se identifica por su prefijo — ``kafka:order-events`` o
``rabbitmq:sms.send`` — y es esta librería la que decide por dónde va; ni el
emisor ni el código del servicio saben si algo viaja por Kafka o por RabbitMQ.
Por eso hay una sola API para los dos transportes.

Misma forma que las librerías hermanas ``tt-lib-go/events`` y
``tt-lib-node/src/events.ts``, adaptada a asyncio: un publicador con
``publish``/``close``, un consumidor con ``start``/``close`` y una función que
fabrica el manejador que registra lo recibido. Quien lea una debe poder
entender las otras.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

import aio_pika
import asyncpg
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger(__name__)

# Espera entre dos intentos de conexión de un bucle de consumo cuando el
# broker aún no está listo o la conexión se cae. No hay backoff creciente
# (queda anotado como mejora pendiente, igual que en tt-lib-go): un valor fijo
# basta para no bloquear el arranque, que es lo que importa aquí.
_RETRY_DELAY_SECONDS = 2.0

# Procesa un mensaje recibido. El canal llega con su prefijo, para que el
# manejador sepa de dónde vino sin que el consumidor tenga que contarlo aparte.
Handler = Callable[[str, bytes], Awaitable[None]]

T = TypeVar("T")

# Centinela para distinguir "todavía no se ha resuelto" de un resultado que
# legítimamente sea None. Comparar con None no serviría.
_UNSET: Any = object()


def split_channel(channel: str) -> tuple[str, str]:
    """Separa el transporte del nombre del canal.

    Devuelve transporte vacío si el canal no lleva prefijo, que es un error de
    declaración en system.yaml y no algo que deba adivinarse aquí.
    """
    transport, separator, name = channel.partition(":")
    if not separator:
        return "", channel
    return transport, name


def _kafka_brokers() -> str:
    return os.getenv("KAFKA_BROKERS") or "kafka:9092"


def _rabbit_url() -> str:
    return os.getenv("RABBITMQ_URL") or "amqp://tt:tt@rabbitmq:5672"


async def _close_quietly(closing: Awaitable[Any]) -> None:
    """Espera un cierre ignorando su error.

    Al cerrar ya no hay nada útil que hacer con el fallo, y dejarlo escapar
    taparía el error real que provocó el cierre.
    """
    try:
        await closing
    except Exception:
        logger.debug("error ignorado al cerrar una conexión", exc_info=True)


def _memoize_async(op: Callable[[], Awaitable[T]]) -> Callable[[], Awaitable[T]]:
    """Memoiza una operación asíncrona guardando SOLO el éxito.

    El fallo NO se cachea, y es deliberado. Cachear conexión y error juntos
    (lo que hacía ``tt-lib-go`` con un ``sync.Once``) rompe el caso normal de
    docker compose, que arranca los servicios sin esperar a que Kafka,
    RabbitMQ o Postgres acepten conexiones: si el primer mensaje llega antes
    que la base de datos, el error queda cacheado para siempre y ningún
    mensaje posterior vuelve a intentarlo, aunque la base de datos se recupere
    segundos después. Es el mismo patrón que ya causó un incidente con Redis
    en una fase anterior.

    Aquí ``result`` solo se asigna si ``op()`` terminó bien; si lanza, el
    estado se queda intacto en ``_UNSET`` y la siguiente llamada reintenta
    desde cero. El lock evita que dos corrutinas en frío disparen dos
    conexiones a la vez, sin convertir el fallo de una en el fallo de todas.
    """
    lock = asyncio.Lock()
    result: Any = _UNSET

    async def ensure() -> T:
        nonlocal result
        if result is not _UNSET:
            return result
        async with lock:
            # Otra corrutina pudo resolverlo mientras esperábamos el lock.
            if result is not _UNSET:
                return result
            value = await op()
            # Solo se llega aquí si `op()` no lanzó: el fallo nunca se guarda.
            result = value
            return value

    return ensure


class Publisher:
    """Publica mensajes en los canales que el servicio declare."""

    def __init__(self, service_name: str) -> None:
        """Crea el publicador del servicio.

        No conecta todavía: la conexión a cada transporte se abre en la
        primera publicación que lo use, para no bloquear el arranque del
        servicio si el broker aún no está listo.
        """
        self._service_name = service_name
        self._producer: AIOKafkaProducer | None = None
        self._rabbit_connection: AbstractRobustConnection | None = None
        self._ensure_producer = _memoize_async(self._open_producer)
        self._ensure_rabbit_channel = _memoize_async(self._open_rabbit_channel)

    async def _open_producer(self) -> AIOKafkaProducer:
        producer = AIOKafkaProducer(
            bootstrap_servers=_kafka_brokers(),
            client_id=self._service_name,
        )
        try:
            await producer.start()
        except Exception:
            # Se cierra el productor a medio arrancar para no dejar sockets
            # colgando, y se relanza sin guardar nada: `_memoize_async` deja
            # el estado intacto, así que la siguiente publicación reintenta.
            await _close_quietly(producer.stop())
            raise
        self._producer = producer
        return producer

    async def _open_rabbit_channel(self) -> AbstractChannel:
        connection = await aio_pika.connect_robust(_rabbit_url())
        try:
            channel = await connection.channel()
        except Exception:
            await _close_quietly(connection.close())
            raise
        self._rabbit_connection = connection
        return channel

    async def publish(self, channel: str, payload: Any) -> None:
        """Envía el payload al canal indicado, serializado como JSON."""
        transport, name = split_channel(channel)
        try:
            body = json.dumps(payload).encode()
        except (TypeError, ValueError) as err:
            raise ValueError(f"serializando el mensaje de {channel}: {err}") from err

        if transport == "kafka":
            producer = await self._ensure_producer()
            await producer.send_and_wait(name, body)
        elif transport == "rabbitmq":
            rabbit_channel = await self._ensure_rabbit_channel()
            await rabbit_channel.declare_queue(name, durable=True)
            await rabbit_channel.default_exchange.publish(
                aio_pika.Message(body=body, content_type="application/json"),
                routing_key=name,
            )
        else:
            raise ValueError(f'canal "{channel}" sin transporte reconocido')

    async def close(self) -> None:
        """Cierra las conexiones abiertas."""
        if self._producer is not None:
            await _close_quietly(self._producer.stop())
            self._producer = None
        if self._rabbit_connection is not None:
            await _close_quietly(self._rabbit_connection.close())
            self._rabbit_connection = None


class Consumer:
    """Escucha los canales que el servicio declare."""

    def __init__(self, service_name: str, channels: Sequence[str], handler: Handler) -> None:
        """Crea el consumidor del servicio. No escucha nada hasta `start()`."""
        self._service_name = service_name
        self._channels = list(channels)
        self._handler = handler
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        """Lanza un bucle por canal como tarea y devuelve INMEDIATAMENTE.

        Estos consumidores viven dentro de aplicaciones FastAPI de vida larga.
        Si `start()` esperase a que cada broker aceptase la conexión, un broker
        caído retrasaría el arranque del servicio entero — y con varios
        consumidores, el del stack completo. Cada canal reintenta por su cuenta
        mientras el servicio ya atiende peticiones.

        Los canales se validan ANTES de lanzar ninguna tarea: si uno no trae un
        transporte reconocido, `start()` lanza sin haber arrancado nada, así el
        llamador no tiene que llamar a `close()` para limpiar un arranque a
        medias.
        """
        for channel in self._channels:
            transport, _ = split_channel(channel)
            if transport not in ("kafka", "rabbitmq"):
                raise ValueError(f'canal "{channel}" sin transporte reconocido')

        for channel in self._channels:
            transport, name = split_channel(channel)
            loop = self._run_kafka if transport == "kafka" else self._run_rabbit
            self._tasks.append(
                asyncio.create_task(loop(channel, name), name=f"{self._service_name}:{channel}")
            )

    async def _dispatch(self, channel: str, payload: bytes) -> bool:
        """Entrega un mensaje al manejador. Un fallo suyo no corta el bucle."""
        try:
            await self._handler(channel, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s: procesando %s", self._service_name, channel)
            return False
        return True

    async def _run_kafka(self, channel: str, topic: str) -> None:
        while True:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=_kafka_brokers(),
                client_id=self._service_name,
                group_id=self._service_name,
            )
            try:
                await consumer.start()
                async for message in consumer:
                    await self._dispatch(channel, message.value or b"")
            except asyncio.CancelledError:
                raise  # parada ordenada desde close()
            except Exception:
                logger.exception("%s: leyendo de %s", self._service_name, channel)
            finally:
                await _close_quietly(consumer.stop())
            await asyncio.sleep(_RETRY_DELAY_SECONDS)

    async def _run_rabbit(self, channel: str, queue_name: str) -> None:
        while True:
            connection: AbstractRobustConnection | None = None
            try:
                connection = await aio_pika.connect_robust(_rabbit_url())
                rabbit_channel = await connection.channel()
                queue = await rabbit_channel.declare_queue(queue_name, durable=True)
                async with queue.iterator() as messages:
                    async for message in messages:
                        # Ack explícito tras procesar: si el manejador falla, el
                        # mensaje vuelve a la cola en vez de perderse.
                        if await self._dispatch(channel, message.body):
                            await message.ack()
                        else:
                            await message.nack(requeue=True)
            except asyncio.CancelledError:
                raise  # parada ordenada desde close()
            except Exception:
                logger.exception("%s: escuchando %s", self._service_name, channel)
            finally:
                if connection is not None:
                    await _close_quietly(connection.close())
            await asyncio.sleep(_RETRY_DELAY_SECONDS)

    async def close(self) -> None:
        """Cancela los bucles y espera a que terminen de forma ordenada.

        Se espera (`await`) a todas las tareas: cada bucle cierra su conexión
        en su propio `finally`, así que al volver de `close()` no queda ninguna
        conexión abierta ni ninguna tarea viva.
        """
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()


_CREATE_TABLE_POSTGRES = """CREATE TABLE IF NOT EXISTS received_events (
    id SERIAL PRIMARY KEY,
    channel VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)"""

_INSERT_POSTGRES = "INSERT INTO received_events (channel, payload) VALUES ($1, $2)"


def _engine_of(database_url: str | None) -> str:
    """Deduce el motor SQL soportado a partir de la cadena de conexión.

    Devuelve cadena vacía si no hay base de datos o si el motor no es un SQL
    soportado por esta librería.

    Solo se contempla PostgreSQL, y es deliberado, no un olvido: los únicos
    consumidores Python del sistema son audit-service y fraud-detection-service
    (Postgres), schedule-optimizer-service y analytics-query-service (sin base
    de datos). Ningún consumidor Python usa MySQL — el único servicio Python
    con MySQL es consign-service, que solo publica y nunca registra lo
    recibido. Añadir aquí un camino con `aiomysql` sería código muerto: si algún
    día un consumidor Python estrena MySQL, se añade entonces (las sentencias
    no son portables — MySQL usa `?` y `AUTO_INCREMENT` donde Postgres usa `$1`
    y `SERIAL`, ver tt-lib-go y tt-lib-node, que sí lo necesitan).
    """
    if not database_url:
        return ""
    if database_url.startswith(("postgresql://", "postgres://")):
        return "postgres"
    return ""


def _log_recorder(service_name: str) -> Handler:
    async def record(channel: str, payload: bytes) -> None:
        logger.info(
            "%s: recibido de %s: %s",
            service_name,
            channel,
            payload.decode("utf-8", errors="replace"),
        )

    return record


def make_recorder(service_name: str, database_url: str | None) -> Handler:
    """Devuelve el manejador que registra lo recibido.

    Si el servicio tiene una base de datos PostgreSQL, escribe una fila en
    `received_events`; si no tiene base de datos, o el motor no es un SQL
    soportado (hoy MongoDB), registra en el log en vez de fallar. Ver
    `_engine_of` para por qué MySQL no está aquí.

    La conexión y el `CREATE TABLE` se hacen en el primer mensaje, no al
    fabricar el manejador, y su fallo NO se cachea: si Postgres todavía no
    acepta conexiones, el siguiente mensaje reintenta desde cero (ver
    `_memoize_async`).
    """
    if _engine_of(database_url) != "postgres":
        return _log_recorder(service_name)

    async def open_pool() -> asyncpg.Pool:
        pool = await asyncpg.create_pool(database_url)
        try:
            await pool.execute(_CREATE_TABLE_POSTGRES)
        except Exception:
            # Ni la conexión ni la tabla se dan por buenas si el CREATE falla:
            # se cierra el pool y se relanza, dejando el memo intacto para que
            # el siguiente mensaje lo intente otra vez.
            await _close_quietly(pool.close())
            raise
        return pool

    ensure_pool = _memoize_async(open_pool)

    async def record(channel: str, payload: bytes) -> None:
        pool = await ensure_pool()
        await pool.execute(
            _INSERT_POSTGRES,
            channel,
            payload.decode("utf-8", errors="replace"),
        )

    return record
