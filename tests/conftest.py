import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from app.application.payments import Payments
from app.domain.enums import Currency
from app.domain.payments import PaymentRequest
from app.infrastructure.persistence.base import Base
from app.infrastructure.persistence.uow import SqlUnitOfWork
from tests.fakes import MemoryStore


def now():
    return datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


async def no_sleep(delay):
    pass


def request(**overrides):
    values = dict(
        amount=Decimal("1500.50"),
        currency=Currency.RUB,
        description="Order #123",
        metadata={"order_id": "123"},
        webhook_url="https://client.example.com/webhook",
    )
    values.update(overrides)
    return PaymentRequest(**values)


POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")


@asynccontextmanager
async def postgres_store():
    schema = "test_payments_" + uuid4().hex
    engine = create_async_engine(
        POSTGRES_URL, execution_options={"schema_translate_map": {None: schema}}
    )
    created = False
    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema))
            await connection.run_sync(Base.metadata.create_all)
        created = True
        yield partial(SqlUnitOfWork, async_sessionmaker(engine, expire_on_commit=False))
    finally:
        try:
            if created:
                async with engine.begin() as connection:
                    await connection.execute(DropSchema(schema, cascade=True))
        finally:
            await engine.dispose()


@pytest.fixture
async def postgres_uow():
    if not POSTGRES_URL:
        pytest.skip("Set TEST_POSTGRES_URL to run PostgreSQL concurrency checks")
    async with postgres_store() as factory:
        yield factory


@pytest.fixture(params=["memory", "sqlite"] + (["postgres"] if POSTGRES_URL else []))
async def uow(request):
    if request.param == "memory":
        yield MemoryStore().uow
    elif request.param == "postgres":
        async with postgres_store() as factory:
            yield factory
    else:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        try:
            yield partial(SqlUnitOfWork, async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()


@pytest.fixture
async def payment(uow):
    return await Payments(uow, now).create(request(), "key-1")
