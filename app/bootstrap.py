"""Composition root: concrete resources are created here and closed by entrypoints."""

import asyncio
import random
from datetime import UTC, datetime
from functools import partial

from faststream.rabbit import Channel, RabbitBroker
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.application.notifications import DeliverPaymentNotification, WebhookDelivery
from app.application.payments import Payments
from app.application.processing import ProcessPayment
from app.application.retry import RetryPolicy
from app.infrastructure.config import Settings
from app.infrastructure.gateway import SimulatedPaymentGateway
from app.infrastructure.persistence.uow import SqlUnitOfWork
from app.infrastructure.rabbit import RabbitEventPublisher
from app.infrastructure.webhooks import HttpWebhookTransport


def utc_now() -> datetime:
    return datetime.now(UTC)


class Database:
    def __init__(self, url: str) -> None:
        self.engine = create_async_engine(url, pool_pre_ping=True)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.uow = partial(SqlUnitOfWork, self.sessions)
        self.payments = Payments(self.uow, utc_now)

    async def close(self) -> None:
        await self.engine.dispose()


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            publisher_confirms=True,
            on_return_raises=True,
            prefetch_count=1,
        ),
    )


class Workers:
    def __init__(self, database: Database, broker: RabbitBroker, client: AsyncClient) -> None:
        self.publisher = RabbitEventPublisher(broker)
        self.processor = ProcessPayment(
            database.uow,
            SimulatedPaymentGateway(asyncio.sleep, random.Random()),
            DeliverPaymentNotification(
                database.uow,
                WebhookDelivery(
                    HttpWebhookTransport(client), RetryPolicy(), utc_now, asyncio.sleep
                ),
                self.publisher,
            ),
            self.publisher,
            RetryPolicy(),
            utc_now,
        )
