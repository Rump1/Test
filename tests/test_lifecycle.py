from unittest.mock import AsyncMock

import pytest
from faststream.rabbit import RabbitBroker

from app.workers import consumer


@pytest.mark.parametrize("failure", ["connect", "topology", "during_run"])
async def test_consumer_releases_resources_when_startup_or_run_fails(monkeypatch, failure):
    closed = []
    broker = RabbitBroker()
    broker.connect = AsyncMock(
        side_effect=RuntimeError("connect") if failure == "connect" else None
    )
    broker.stop = AsyncMock()
    broker.declare_queue = AsyncMock(
        side_effect=RuntimeError("topology") if failure == "topology" else None
    )

    class Database:
        uow = None

        def __init__(self, url):
            pass

        async def close(self):
            closed.append("database")

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            closed.append("http")

    monkeypatch.setattr(consumer, "Database", Database)
    monkeypatch.setattr(consumer, "AsyncClient", Client)
    monkeypatch.setattr(consumer, "create_broker", lambda settings: broker)
    app = consumer.create_consumer()
    with pytest.raises(RuntimeError, match=failure):
        async with app.lifespan_context():
            raise RuntimeError("during_run")
    assert closed == ["http", "database"]
    broker.stop.assert_awaited_once()
