from dataclasses import replace

import pytest

from app.application.outbox import PublishOutbox
from app.application.retry import RetryPolicy
from app.domain.enums import OutboxStatus
from tests.conftest import no_sleep, now
from tests.fakes import RecordingPublisher


async def test_published_event_is_not_selected_again(uow, payment):
    publisher = RecordingPublisher()
    use_case = PublishOutbox(uow, publisher, RetryPolicy(), now, no_sleep)
    assert await use_case.execute() == 1
    assert await use_case.execute() == 0
    assert publisher.published[0]["payment_id"] == str(payment.id)


@pytest.mark.parametrize("legacy_failed", [True, False])
async def test_broker_outage_recovers_on_next_poll(uow, payment, legacy_failed):
    delays = []

    async def sleep(delay):
        delays.append(delay)

    publisher = RecordingPublisher()
    publisher.unavailable = True
    use_case = PublishOutbox(uow, publisher, RetryPolicy(), now, sleep)
    assert await use_case.execute() == 0
    assert delays == [1, 2]
    async with uow() as tx:
        events = await tx.outbox.pending(10)
        assert events[0].attempts == 3
        assert events[0].status == OutboxStatus.PENDING
        if legacy_failed:
            await tx.outbox.save(replace(events[0], status=OutboxStatus.FAILED))
            await tx.commit()
    publisher.unavailable = False
    assert await use_case.execute() == 1
    assert publisher.published[0]["payment_id"] == str(payment.id)


async def test_backoff_releases_transaction_and_rechecks_other_publishers(uow, payment):
    import asyncio

    first, other = RecordingPublisher(), RecordingPublisher()
    first.unavailable = True
    delays = []

    async def sleep(delay):
        delays.append(delay)
        # This would deadlock with a transaction held by the memory adapter.
        assert (
            await asyncio.wait_for(
                PublishOutbox(uow, other, RetryPolicy(), now, no_sleep).execute(), timeout=2
            )
            == 1
        )

    assert await PublishOutbox(uow, first, RetryPolicy(), now, sleep).execute() == 0
    assert delays == [1]
    assert len(other.published) == 1
    async with uow() as tx:
        assert await tx.outbox.pending(10) == []


@pytest.mark.parametrize("failure", ["cancel", "commit"])
async def test_later_failure_preserves_committed_publications(uow, payment, failure):
    import asyncio

    from app.application.payments import Payments
    from tests.conftest import request

    second = await Payments(uow, now).create(request(), "key-2")

    class Publisher(RecordingPublisher):
        async def publish_payment(self, payload):
            super_payload = dict(payload)
            await super().publish_payment(super_payload)
            if failure == "cancel" and len(self.published) == 2:
                raise asyncio.CancelledError()

    publisher = Publisher()

    def failing_uow():
        tx = uow()
        commit = tx.commit

        async def fail_late():
            if failure == "commit" and len(publisher.published) == 2:
                raise RuntimeError("commit unavailable")
            await commit()

        tx.commit = fail_late
        return tx

    error = asyncio.CancelledError if failure == "cancel" else RuntimeError
    with pytest.raises(error):
        await PublishOutbox(failing_uow, publisher, RetryPolicy(), now, no_sleep).execute()
    async with uow() as tx:
        remaining = await tx.outbox.pending(10)
        assert len(remaining) == 1
        # Ordering of equal timestamps is intentionally unspecified.
        assert str(remaining[0].aggregate_id) == publisher.published[1]["payment_id"]
    assert await PublishOutbox(uow, publisher, RetryPolicy(), now, no_sleep).execute() == 1
    ids = [p["payment_id"] for p in publisher.published]
    assert ids.count(ids[0]) == 1
    assert set(ids) == {str(payment.id), str(second.id)}
    assert ids[1] == ids[2]  # Only the uncommitted event may be delivered again.
