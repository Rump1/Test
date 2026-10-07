import asyncio

import pytest

from app.application.notifications import DeliverPaymentNotification, WebhookDelivery
from app.application.payments import Payments
from app.application.ports import DeliveryError
from app.application.retry import RetryPolicy
from app.domain.enums import PaymentStatus, WebhookStatus
from app.domain.payments import GatewayResult
from tests.conftest import no_sleep, now
from tests.fakes import RecordingPublisher


async def finalize(uow, payment):
    async with uow() as tx:
        await tx.payments.save(payment.finalize(GatewayResult(True), now()))
        await tx.commit()


class Transport:
    def __init__(self):
        self.calls = []

    async def send(self, notification):
        self.calls.append(notification)


def notifications(uow, transport, publisher):
    return DeliverPaymentNotification(
        uow, WebhookDelivery(transport, RetryPolicy(), now, no_sleep), publisher
    )


async def test_http_success_before_commit_can_replay_with_same_event_id(uow, payment):
    await finalize(uow, payment)
    transport, publisher = Transport(), RecordingPublisher()

    def failing_uow():
        tx = uow()

        async def fail_commit():
            raise asyncio.CancelledError()

        tx.commit = fail_commit
        return tx

    with pytest.raises(asyncio.CancelledError):
        await notifications(failing_uow, transport, publisher).execute(payment.id)
    saved = await Payments(uow, now).get(payment.id)
    assert saved.status == PaymentStatus.SUCCEEDED
    assert saved.webhook_status == WebhookStatus.PENDING
    use_case = notifications(uow, transport, publisher)
    await use_case.execute(payment.id)
    await use_case.execute(payment.id)
    assert len(transport.calls) == 2
    assert transport.calls[0].event_id == transport.calls[1].event_id
    assert (await Payments(uow, now).get(payment.id)).webhook_status == WebhookStatus.SENT


async def test_mixed_errors_then_dlq_recovery_preserves_delivery_result(uow, payment):
    await finalize(uow, payment)

    class FailingTransport(Transport):
        async def send(self, notification):
            await super().send(notification)
            if len(self.calls) == 1:
                raise DeliveryError("HTTP 503")
            raise RuntimeError("transport broke")

    transport, publisher = FailingTransport(), RecordingPublisher()
    publisher.unavailable = True
    use_case = notifications(uow, transport, publisher)
    with pytest.raises(RuntimeError, match="broker unavailable"):
        await use_case.execute(payment.id)
    saved = await Payments(uow, now).get(payment.id)
    assert saved.status == PaymentStatus.SUCCEEDED
    assert saved.webhook_status == WebhookStatus.FAILED
    assert saved.webhook_attempts == 2
    publisher.unavailable = False
    await use_case.execute(payment.id)
    assert len(transport.calls) == 2
    assert publisher.dead[0]["attempt"] == 2
    assert publisher.dead[0]["error"] == "transport broke"


async def test_orchestration_error_does_not_invent_a_delivery_result(uow, payment):
    await finalize(uow, payment)

    class BrokenDelivery:
        async def deliver(self, notification):
            raise RuntimeError("unknown delivery outcome")

    publisher = RecordingPublisher()
    use_case = DeliverPaymentNotification(uow, BrokenDelivery(), publisher)
    with pytest.raises(RuntimeError, match="unknown delivery outcome"):
        await use_case.execute(payment.id)
    saved = await Payments(uow, now).get(payment.id)
    assert saved.webhook_status == WebhookStatus.PENDING
    assert saved.webhook_attempts == 0
    assert publisher.dead == []
