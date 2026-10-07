"""Real row-lock behavior; each test owns an isolated PostgreSQL schema."""

import asyncio

from app.application.notifications import DeliverPaymentNotification, WebhookDelivery
from app.application.outbox import PublishOutbox
from app.application.payments import Payments
from app.application.retry import RetryPolicy
from app.domain.payments import GatewayResult
from tests.conftest import no_sleep, now, request
from tests.fakes import RecordingPublisher


async def test_publishers_skip_busy_event_and_do_not_republish_snapshot(postgres_uow):
    uow = postgres_uow
    await Payments(uow, now).create(request(), "first")
    await Payments(uow, now).create(request(), "second")
    entered, release = asyncio.Event(), asyncio.Event()

    class SlowPublisher(RecordingPublisher):
        async def publish_payment(self, payload):
            await super().publish_payment(payload)
            entered.set()
            await release.wait()

    slow, fast = SlowPublisher(), RecordingPublisher()
    task = asyncio.create_task(PublishOutbox(uow, slow, RetryPolicy(), now, no_sleep).execute())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        # claim() skips a lock, not just pending()'s snapshot.
        async with uow() as tx:
            candidates = await tx.outbox.pending(10)
            assert len(candidates) == 1
        assert (
            await asyncio.wait_for(
                PublishOutbox(uow, fast, RetryPolicy(), now, no_sleep).execute(), timeout=5
            )
            == 1
        )
        release.set()
        assert await asyncio.wait_for(task, timeout=5) == 1
        assert len(slow.published) == len(fast.published) == 1
        assert slow.published[0]["payment_id"] != fast.published[0]["payment_id"]
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_claim_skips_locked_and_published_events(postgres_uow):
    uow = postgres_uow
    await Payments(uow, now).create(request(), "one")
    async with uow() as tx:
        event = (await tx.outbox.pending(1))[0]
    async with uow() as owner:
        assert await owner.outbox.claim(event.id) is not None
        async with uow() as other:
            assert await asyncio.wait_for(other.outbox.claim(event.id), timeout=2) is None
        await owner.outbox.save(event.published(now()))
        await owner.commit()
    async with uow() as tx:
        assert await tx.outbox.claim(event.id) is None


async def test_concurrent_notifications_see_final_payment_and_send_once(postgres_uow):
    uow = postgres_uow
    payment = await Payments(uow, now).create(request(), "notification")
    async with uow() as tx:
        await tx.payments.save(payment.finalize(GatewayResult(True), now()))
        await tx.commit()
    entered, release, second_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    class Transport:
        async def send(self, notification):
            calls.append(notification)
            saved = await Payments(uow, now).get(payment.id)
            assert saved.status == "succeeded"
            entered.set()
            await release.wait()

    use_case = DeliverPaymentNotification(
        uow, WebhookDelivery(Transport(), RetryPolicy(), now, no_sleep), RecordingPublisher()
    )

    async def second_delivery():
        second_started.set()
        await use_case.execute(payment.id)

    first = asyncio.create_task(use_case.execute(payment.id))
    second = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        second = asyncio.create_task(second_delivery())
        await second_started.wait()
        release.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=5)
        assert len(calls) == 1
    finally:
        release.set()
        tasks = [task for task in (first, second) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
