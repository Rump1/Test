"""Run: docker compose exec -T api python - < scripts/check_postgres.py."""

import asyncio
from decimal import Decimal
from functools import partial
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from app.application.payments import IdempotencyConflictError, Payments
from app.bootstrap import utc_now
from app.domain.enums import Currency
from app.domain.payments import PaymentRequest
from app.infrastructure.config import get_settings
from app.infrastructure.persistence.base import Base
from app.infrastructure.persistence.uow import SqlUnitOfWork


async def main():
    schema = "architecture_test_" + uuid4().hex
    engine = create_async_engine(
        get_settings().database_url, execution_options={"schema_translate_map": {None: schema}}
    )
    created = False
    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema))
            await connection.run_sync(Base.metadata.create_all)
        created = True
        uow = partial(SqlUnitOfWork, async_sessionmaker(engine, expire_on_commit=False))
        payments = Payments(uow, utc_now)
        request = PaymentRequest(
            amount=Decimal("10.00"),
            currency=Currency.RUB,
            description="PostgreSQL contract",
            metadata={},
            webhook_url="http://unused",
        )
        results = await asyncio.gather(*(payments.create(request, "same-key") for _ in range(12)))
        assert len({p.id for p in results}) == 1
        async with uow() as tx:
            assert len(await tx.outbox.pending(100)) == 1
        print("Concurrent idempotency: one payment and one outbox event: OK")

        other = PaymentRequest(
            amount=Decimal("20.00"),
            currency=Currency.RUB,
            description=request.description,
            metadata={},
            webhook_url=request.webhook_url,
        )
        try:
            await payments.create(other, "same-key")
        except IdempotencyConflictError:
            pass
        else:
            raise AssertionError("Changed payload must conflict")

        await payments.create(request, "second-key")
        async with uow() as first:
            first_batch = await first.outbox.pending(1)
            assert len(first_batch) == 1
            async with uow() as second:
                second_batch = await asyncio.wait_for(second.outbox.pending(100), timeout=2)
                assert len(second_batch) == 1
                assert first_batch[0].id != second_batch[0].id
        async with uow() as tx:
            assert len(await tx.outbox.pending(100)) == 2
        print("SKIP LOCKED: concurrent batches are disjoint; rollback releases locks: OK")
    finally:
        if created:
            async with engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
        await engine.dispose()


asyncio.run(main())
