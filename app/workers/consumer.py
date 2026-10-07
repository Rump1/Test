from contextlib import AsyncExitStack, asynccontextmanager

from faststream import FastStream
from faststream.middlewares.acknowledgement.config import AckPolicy
from httpx import AsyncClient

from app.application.retry import RetryPolicy
from app.bootstrap import Database, Workers, create_broker
from app.infrastructure.config import get_settings
from app.infrastructure.rabbit import PAYMENTS_QUEUE
from app.presentation.messaging.payments import PaymentMessageHandler


def create_consumer() -> FastStream:
    settings = get_settings()
    broker = create_broker(settings)
    handler = None

    @asynccontextmanager
    async def lifespan():
        nonlocal handler
        async with AsyncExitStack() as resources:
            database = Database(settings.database_url)
            resources.push_async_callback(database.close)
            client = await resources.enter_async_context(AsyncClient())
            # Also closes partial startup when connect/topology declaration fails.
            resources.push_async_callback(broker.stop)
            workers = Workers(database, broker, client)
            await broker.connect()
            await workers.publisher.declare_topology()
            handler = PaymentMessageHandler(
                workers.processor, workers.publisher, RetryPolicy().max_attempts
            )
            try:
                yield
            finally:
                handler = None

    app = FastStream(broker, lifespan=lifespan)

    @broker.subscriber(PAYMENTS_QUEUE, ack_policy=AckPolicy.NACK_ON_ERROR)
    async def handle_payment_created(message: dict) -> None:
        if handler is None:
            raise RuntimeError("consumer has not started")
        await handler.handle(message)

    return app


app = create_consumer()
