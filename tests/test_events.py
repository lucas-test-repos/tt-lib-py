import asyncio

import pytest

from tt_lib.events import Consumer, Publisher, make_recorder, split_channel


def test_split_channel_separates_transport():
    assert split_channel("kafka:order-events") == ("kafka", "order-events")
    assert split_channel("rabbitmq:sms.send") == ("rabbitmq", "sms.send")


def test_split_channel_without_prefix_has_no_transport():
    transport, _ = split_channel("order-events")
    assert transport == ""


@pytest.mark.asyncio
async def test_recorder_without_database_logs_instead_of_failing():
    record = make_recorder("audit-service", None)
    await record("kafka:order-events", b'{"a":1}')


@pytest.mark.asyncio
async def test_recorder_with_unsupported_engine_falls_back_to_log():
    record = make_recorder("catalog-sync-service", "mongodb://mongo:27017/catalog")
    await record("kafka:travel-events", b"{}")


# --------------------------------------------------------------------------
# El fallo de conexión NO se cachea.
#
# Estos tests cubren el bug Critical que se encontró en tt-lib-go: su recorder
# memoizaba conexión y error juntos con un sync.Once, así que si el primer
# mensaje llegaba antes de que Postgres aceptase conexiones —el caso normal
# cuando compose arranca la base de datos y el servicio a la vez— el error
# quedaba cacheado para siempre y ningún mensaje posterior volvía a intentarlo.
# Aquí se fija la conducta contraria para que no se pierda.
# --------------------------------------------------------------------------


class _FakePool:
    """Pool de asyncpg de mentira: apunta lo que se ejecuta contra él."""

    def __init__(self, fail_on_create_table: bool = False) -> None:
        self.executed: list[tuple] = []
        self.closed = False
        self._fail_on_create_table = fail_on_create_table

    async def execute(self, sql: str, *args: object) -> None:
        if self._fail_on_create_table and sql.startswith("CREATE TABLE"):
            raise ConnectionError("la base de datos todavía no acepta conexiones")
        self.executed.append((sql, *args))

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_recorder_retries_when_the_first_connection_fails(monkeypatch):
    """Un fallo de conexión no debe dejar al consumidor mudo para siempre."""
    pool = _FakePool()
    attempts = []

    async def fake_create_pool(url: str):
        attempts.append(url)
        if len(attempts) == 1:
            raise ConnectionError("connection refused")
        return pool

    monkeypatch.setattr("tt_lib.events.asyncpg.create_pool", fake_create_pool)
    record = make_recorder("audit-service", "postgresql://tt:tt@postgres:5432/audit")

    # Primer mensaje: Postgres aún no está arriba.
    with pytest.raises(ConnectionError):
        await record("kafka:order-events", b'{"a":1}')

    # Segundo mensaje: la base de datos ya responde y debe reintentarse.
    await record("kafka:order-events", b'{"a":2}')

    assert len(attempts) == 2, "el segundo mensaje debe reintentar la conexión"
    assert pool.executed[0][0].startswith("CREATE TABLE IF NOT EXISTS received_events")
    assert pool.executed[1] == (
        "INSERT INTO received_events (channel, payload) VALUES ($1, $2)",
        "kafka:order-events",
        '{"a":2}',
    )


@pytest.mark.asyncio
async def test_recorder_retries_when_create_table_fails(monkeypatch):
    """Si falla el CREATE TABLE tampoco se da la conexión por buena."""
    pools = [_FakePool(fail_on_create_table=True), _FakePool()]

    failed_pool, good_pool = pools

    async def fake_create_pool(url: str):
        return pools.pop(0)

    monkeypatch.setattr("tt_lib.events.asyncpg.create_pool", fake_create_pool)
    record = make_recorder("fraud-detection-service", "postgres://tt:tt@postgres:5432/fraud")

    with pytest.raises(ConnectionError):
        await record("kafka:order-events", b"{}")

    await record("kafka:order-events", b'{"ok":true}')

    assert pools == [], "el segundo intento debe abrir un pool nuevo, no reusar el fallido"
    assert failed_pool.closed, "el pool cuyo CREATE TABLE falló se cierra"
    assert good_pool.executed[-1][1:] == ("kafka:order-events", '{"ok":true}')


@pytest.mark.asyncio
async def test_recorder_memoizes_the_successful_connection(monkeypatch):
    """El éxito sí se memoiza: una sola conexión y un solo CREATE TABLE."""
    pool = _FakePool()
    attempts = []

    async def fake_create_pool(url: str):
        attempts.append(url)
        return pool

    monkeypatch.setattr("tt_lib.events.asyncpg.create_pool", fake_create_pool)
    record = make_recorder("audit-service", "postgresql://tt:tt@postgres:5432/audit")

    await record("kafka:order-events", b"{}")
    await record("kafka:order-events", b"{}")

    assert len(attempts) == 1
    creates = [sql for sql, *_ in pool.executed if sql.startswith("CREATE TABLE")]
    assert len(creates) == 1


class _FakeProducer:
    """Productor de aiokafka de mentira, con arranque configurable."""

    instances: list["_FakeProducer"] = []

    def __init__(self, *, bootstrap_servers: str, client_id: str) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self.stopped = False
        self.fail_start = False
        _FakeProducer.instances.append(self)

    async def start(self) -> None:
        if self.fail_start:
            raise ConnectionError("el broker todavía no acepta conexiones")

    async def send_and_wait(self, topic: str, body: bytes) -> None:
        self.sent.append((topic, body))

    async def stop(self) -> None:
        self.stopped = True


@pytest.mark.asyncio
async def test_publisher_retries_when_the_producer_fails_to_start(monkeypatch):
    """Igual que el recorder: un arranque fallido no se queda cacheado."""
    _FakeProducer.instances = []
    fail_first = {"pending": True}

    def factory(**kwargs):
        producer = _FakeProducer(**kwargs)
        if fail_first["pending"]:
            producer.fail_start = True
            fail_first["pending"] = False
        return producer

    monkeypatch.setattr("tt_lib.events.AIOKafkaProducer", factory)
    publisher = Publisher("admin-user-service")

    with pytest.raises(ConnectionError):
        await publisher.publish("kafka:audit-events", {"actor": "ana"})

    await publisher.publish("kafka:audit-events", {"actor": "ana"})

    assert len(_FakeProducer.instances) == 2, "la segunda publicación debe reintentar"
    assert _FakeProducer.instances[0].stopped, "el productor a medio arrancar se cierra"
    assert _FakeProducer.instances[1].sent == [("audit-events", b'{"actor": "ana"}')]


@pytest.mark.asyncio
async def test_publisher_rejects_a_channel_without_transport():
    publisher = Publisher("admin-user-service")

    with pytest.raises(ValueError, match="sin transporte reconocido"):
        await publisher.publish("audit-events", {})


# --------------------------------------------------------------------------
# Ciclo de vida del consumidor: start() no bloquea, close() espera.
# --------------------------------------------------------------------------


class _FakeKafkaConsumer:
    """Consumidor que se queda esperando al broker y nunca conecta."""

    started = None
    stopped: list[bool] = []

    def __init__(self, *topics: str, **kwargs: object) -> None:
        pass

    async def start(self) -> None:
        _FakeKafkaConsumer.started.set()
        await asyncio.Event().wait()  # el broker no responde: espera indefinida

    async def stop(self) -> None:
        _FakeKafkaConsumer.stopped.append(True)

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


@pytest.mark.asyncio
async def test_consumer_start_returns_without_waiting_for_the_broker(monkeypatch):
    """start() debe devolver enseguida aunque el broker no responda.

    Estos consumidores viven dentro de aplicaciones FastAPI: si start()
    esperase a la conexión, un broker caído retrasaría el arranque de varios
    servicios a la vez.
    """
    _FakeKafkaConsumer.started = asyncio.Event()
    _FakeKafkaConsumer.stopped = []
    monkeypatch.setattr("tt_lib.events.AIOKafkaConsumer", _FakeKafkaConsumer)

    async def handler(channel: str, payload: bytes) -> None:
        pass

    consumer = Consumer("audit-service", ["kafka:order-events"], handler)

    await asyncio.wait_for(consumer.start(), timeout=1)

    # La tarea de consumo corre por su cuenta después de que start() volviera.
    await asyncio.wait_for(_FakeKafkaConsumer.started.wait(), timeout=1)

    # close() cancela y espera: al volver, el bucle ya cerró su conexión.
    await asyncio.wait_for(consumer.close(), timeout=1)
    assert _FakeKafkaConsumer.stopped == [True]


@pytest.mark.asyncio
async def test_consumer_rejects_a_channel_without_transport_before_starting(monkeypatch):
    """Un canal mal declarado falla sin dejar tareas a medio arrancar."""
    _FakeKafkaConsumer.started = asyncio.Event()
    _FakeKafkaConsumer.stopped = []
    monkeypatch.setattr("tt_lib.events.AIOKafkaConsumer", _FakeKafkaConsumer)

    async def handler(channel: str, payload: bytes) -> None:
        pass

    consumer = Consumer("audit-service", ["kafka:order-events", "order-events"], handler)

    with pytest.raises(ValueError, match="sin transporte reconocido"):
        await consumer.start()

    await consumer.close()
    assert not _FakeKafkaConsumer.started.is_set(), "no debe arrancar ningún canal"


# --------------------------------------------------------------------------
# Despacho de mensajes: qué recibe el manejador y qué se ack/nack-ea.
#
# Los dobles entregan una lista de mensajes y luego se quedan esperando, sin
# terminar el iterador: así el bucle de consumo no reconecta ni duerme, y el
# test no depende de temporizadores.
# --------------------------------------------------------------------------


class _FakeKafkaMessage:
    def __init__(self, value: bytes) -> None:
        self.value = value


class _DeliveringKafkaConsumer:
    """Consumidor de Kafka que entrega `payloads` y después se queda quieto."""

    payloads: list[bytes] = []
    drained: asyncio.Event | None = None
    instances = 0

    def __init__(self, *topics: str, **kwargs: object) -> None:
        _DeliveringKafkaConsumer.instances += 1
        self._pending = list(_DeliveringKafkaConsumer.payloads)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def __aiter__(self):
        return self

    async def __anext__(self) -> _FakeKafkaMessage:
        if self._pending:
            return _FakeKafkaMessage(self._pending.pop(0))
        _DeliveringKafkaConsumer.drained.set()
        # No se agota el iterador: el bucle sigue esperando mensajes, que es lo
        # que hace un consumidor real entre entrega y entrega.
        await asyncio.Event().wait()


def _install_kafka(monkeypatch, payloads: list[bytes]) -> asyncio.Event:
    _DeliveringKafkaConsumer.payloads = payloads
    _DeliveringKafkaConsumer.drained = asyncio.Event()
    _DeliveringKafkaConsumer.instances = 0
    monkeypatch.setattr("tt_lib.events.AIOKafkaConsumer", _DeliveringKafkaConsumer)
    return _DeliveringKafkaConsumer.drained


@pytest.mark.asyncio
async def test_kafka_message_reaches_the_handler_with_its_channel_prefix(monkeypatch):
    """El manejador recibe el canal COMPLETO, con prefijo, y el cuerpo tal cual.

    El prefijo importa: audit-service escucha tres tópicos con el mismo
    manejador y distingue el origen solo por él.
    """
    drained = _install_kafka(monkeypatch, [b'{"id":1}'])
    received: list[tuple[str, bytes]] = []

    async def handler(channel: str, payload: bytes) -> None:
        received.append((channel, payload))

    consumer = Consumer("audit-service", ["kafka:order-events"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert received == [("kafka:order-events", b'{"id":1}')]


@pytest.mark.asyncio
async def test_kafka_handler_failure_does_not_kill_the_consume_loop(monkeypatch):
    """Un fallo del manejador no puede tumbar el bucle ni reconectar.

    En Kafka no hay ack explícito, así que lo único que hay que garantizar es
    que el mensaje siguiente se sigue entregando.
    """
    drained = _install_kafka(monkeypatch, [b"veneno", b"bueno"])
    received: list[bytes] = []

    async def handler(channel: str, payload: bytes) -> None:
        received.append(payload)
        if payload == b"veneno":
            raise RuntimeError("el manejador explota con este mensaje")

    consumer = Consumer("audit-service", ["kafka:order-events"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert received == [b"veneno", b"bueno"], "el bucle sigue tras la excepción"
    assert _DeliveringKafkaConsumer.instances == 1, "no debe reconectar por un fallo del manejador"


class _FakeRabbitMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.acked = False
        self.nacked_requeue: bool | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = False) -> None:
        self.nacked_requeue = requeue


class _FakeQueueIterator:
    def __init__(self, messages: list[_FakeRabbitMessage], drained: asyncio.Event) -> None:
        self._pending = list(messages)
        self._drained = drained

    async def __aenter__(self) -> "_FakeQueueIterator":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def __aiter__(self):
        return self

    async def __anext__(self) -> _FakeRabbitMessage:
        if self._pending:
            return self._pending.pop(0)
        self._drained.set()
        await asyncio.Event().wait()


class _FakeQueue:
    def __init__(self, messages: list[_FakeRabbitMessage], drained: asyncio.Event) -> None:
        self._messages = messages
        self._drained = drained

    def iterator(self) -> _FakeQueueIterator:
        return _FakeQueueIterator(self._messages, self._drained)


class _FakeRabbitChannel:
    def __init__(self, queue: _FakeQueue) -> None:
        self._queue = queue
        self.declared: tuple[str, bool] | None = None

    async def declare_queue(self, name: str, durable: bool = False) -> _FakeQueue:
        self.declared = (name, durable)
        return self._queue


class _FakeRabbitConnection:
    def __init__(self, channel: _FakeRabbitChannel) -> None:
        self._channel = channel
        self.closed = False

    async def channel(self) -> _FakeRabbitChannel:
        return self._channel

    async def close(self) -> None:
        self.closed = True


def _install_rabbit(monkeypatch, messages: list[_FakeRabbitMessage]):
    drained = asyncio.Event()
    channel = _FakeRabbitChannel(_FakeQueue(messages, drained))
    connection = _FakeRabbitConnection(channel)

    async def fake_connect_robust(url: str) -> _FakeRabbitConnection:
        return connection

    monkeypatch.setattr("tt_lib.events.aio_pika.connect_robust", fake_connect_robust)
    return drained, channel


@pytest.mark.asyncio
async def test_rabbit_message_reaches_the_handler_and_is_acked(monkeypatch):
    """Entrega correcta: el manejador recibe canal con prefijo y cuerpo, y se ack-ea."""
    message = _FakeRabbitMessage(b'{"to":"+34600"}')
    drained, channel = _install_rabbit(monkeypatch, [message])
    received: list[tuple[str, bytes]] = []

    async def handler(channel_name: str, payload: bytes) -> None:
        received.append((channel_name, payload))

    consumer = Consumer("sms-gateway-service", ["rabbitmq:sms.send"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert received == [("rabbitmq:sms.send", b'{"to":"+34600"}')]
    assert channel.declared == ("sms.send", True), "la cola se declara duradera"
    assert message.acked is True
    assert message.nacked_requeue is None, "un mensaje procesado no se reencola"


@pytest.mark.asyncio
async def test_rabbit_nacks_with_requeue_when_the_handler_fails(monkeypatch):
    """Si el manejador falla, el mensaje vuelve a la cola en vez de perderse.

    Sin el nack con requeue el evento desaparecería en silencio; con un ack
    equivocado, también. Por eso se comprueban las dos cosas a la vez.
    """
    message = _FakeRabbitMessage(b"{}")
    drained, _ = _install_rabbit(monkeypatch, [message])

    async def handler(channel_name: str, payload: bytes) -> None:
        raise RuntimeError("el manejador no pudo procesarlo")

    consumer = Consumer("sms-gateway-service", ["rabbitmq:sms.send"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert message.nacked_requeue is True, "debe reencolarse"
    assert message.acked is False, "no debe ack-earse un mensaje que falló"
