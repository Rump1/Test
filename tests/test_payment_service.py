from decimal import Decimal

import pytest

from app.application.payments import IdempotencyConflictError, Payments
from app.domain.enums import PaymentStatus
from tests.conftest import now, request


async def test_create_commits_payment_and_outbox_atomically(uow):
    payment = await Payments(uow, now).create(request(), "key-1")
    async with uow() as transaction:
        persisted = await transaction.payments.get(payment.id)
        events = await transaction.outbox.pending(10)
    assert persisted.status == PaymentStatus.PENDING
    assert persisted.request.amount == Decimal("1500.50")
    assert len(events) == 1
    assert events[0].aggregate_id == payment.id
    assert events[0].payload["payment_id"] == str(payment.id)


async def test_idempotent_replay_does_not_create_second_event(uow):
    payments = Payments(uow, now)
    first = await payments.create(request(metadata={"a": 1, "b": 2}), "key")
    replay = await payments.create(request(metadata={"b": 2, "a": 1}), "key")
    assert first.id == replay.id
    async with uow() as transaction:
        assert len(await transaction.outbox.pending(10)) == 1


async def test_idempotency_conflict(uow):
    payments = Payments(uow, now)
    await payments.create(request(), "key")
    with pytest.raises(IdempotencyConflictError):
        await payments.create(request(amount=Decimal("20.00")), "key")


async def test_outbox_failure_rolls_back_payment(uow):
    class BrokenOutboxUow:
        async def __aenter__(self):
            self.inner = uow()
            transaction = await self.inner.__aenter__()

            async def fail(event):
                raise RuntimeError("outbox storage failed")

            transaction.outbox.add = fail
            return transaction

        async def __aexit__(self, *args):
            await self.inner.__aexit__(*args)

    with pytest.raises(RuntimeError, match="outbox storage failed"):
        await Payments(BrokenOutboxUow, now).create(request(), "key")
    async with uow() as transaction:
        assert await transaction.payments.by_key("key") is None
        assert await transaction.outbox.pending(10) == []
