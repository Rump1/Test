import asyncio
from uuid import uuid4

import pytest

from app.application.notifications import DeliverPaymentNotification
from app.application.payments import Payments
from app.application.processing import PaymentMessage, ProcessPayment
from app.application.retry import RetryPolicy
from app.domain.enums import PaymentStatus, WebhookStatus
from app.domain.payments import DeliveryResult, GatewayResult
from app.presentation.messaging.payments import PaymentMessageHandler
from tests.conftest import now
from tests.fakes import RecordingPublisher


class Gateway:
    def __init__(self, error=None, succeeded=True):
        self.calls = 0
        self.error = error
        self.succeeded = succeeded

    async def process(self, payment):
        self.calls += 1
        if self.error:
            raise self.error
        return GatewayResult(self.succeeded, None if self.succeeded else "declined")


class Delivery:
    def __init__(self, error=None, sent=True):
        self.calls = []
        self.error = error
        self.sent = sent

    async def deliver(self, notification):
        self.calls.append(notification)
        if self.error:
            raise self.error
        return DeliveryResult(
            self.sent,
            1 if self.sent else 3,
            None if self.sent else "HTTP 503",
            now() if self.sent else None,
        )


def processor(uow, gateway, delivery, publisher):
    return ProcessPayment(
        uow,
        gateway,
        DeliverPaymentNotification(uow, delivery, publisher),
        publisher,
        RetryPolicy(),
        now,
    )


@pytest.mark.parametrize("succeeded", [True, False])
async def test_finalizes_and_notifies_once_on_redelivery(uow, payment, succeeded):
    gateway, delivery, publisher = Gateway(succeeded=succeeded), Delivery(), RecordingPublisher()
    use_case = processor(uow, gateway, delivery, publisher)
    await use_case.execute(PaymentMessage(payment.id))
    await use_case.execute(PaymentMessage(payment.id))
    saved = await Payments(uow, now).get(payment.id)
    assert saved.status == (PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED)
    assert saved.webhook_status == WebhookStatus.SENT
    assert saved.processed_at is not None
    assert gateway.calls == len(delivery.calls) == 1
    assert publisher.dead == []


async def test_crash_after_payment_commit_resumes_only_webhook(uow, payment):
    gateway, publisher = Gateway(), RecordingPublisher()
    with pytest.raises(asyncio.CancelledError):
        await processor(uow, gateway, Delivery(error=asyncio.CancelledError()), publisher).execute(
            PaymentMessage(payment.id)
        )
    saved = await Payments(uow, now).get(payment.id)
    assert saved.status == PaymentStatus.SUCCEEDED
    assert saved.webhook_status == WebhookStatus.PENDING
    await processor(uow, gateway, Delivery(), publisher).execute(PaymentMessage(payment.id))
    assert gateway.calls == 1
    assert (await Payments(uow, now).get(payment.id)).webhook_status == WebhookStatus.SENT


async def test_final_payment_with_pending_webhook_resumes(uow, payment):
    async with uow() as tx:
        await tx.payments.save(payment.finalize(GatewayResult(True), now()))
        await tx.commit()
    gateway, delivery = Gateway(), Delivery()
    await processor(uow, gateway, delivery, RecordingPublisher()).execute(
        PaymentMessage(payment.id)
    )
    assert gateway.calls == 0
    assert len(delivery.calls) == 1


async def test_webhook_failure_goes_to_dlq_without_changing_payment(uow, payment):
    publisher = RecordingPublisher()
    await processor(uow, Gateway(), Delivery(sent=False), publisher).execute(
        PaymentMessage(payment.id)
    )
    saved = await Payments(uow, now).get(payment.id)
    assert saved.status == PaymentStatus.SUCCEEDED
    assert saved.webhook_status == WebhookStatus.FAILED
    assert saved.webhook_attempts == 3
    assert publisher.dead[0]["stage"] == "webhook"
    assert publisher.dead[0]["attempt"] == 3


async def test_dlq_publish_failure_resumes_without_resending(uow, payment):
    gateway, delivery, publisher = Gateway(), Delivery(sent=False), RecordingPublisher()
    publisher.unavailable = True
    use_case = processor(uow, gateway, delivery, publisher)
    with pytest.raises(RuntimeError, match="broker unavailable"):
        await use_case.execute(PaymentMessage(payment.id))
    publisher.unavailable = False
    await use_case.execute(PaymentMessage(payment.id))
    assert gateway.calls == len(delivery.calls) == 1
    assert publisher.dead[0]["stage"] == "webhook"


@pytest.mark.parametrize("attempt,delay", [(1, 1), (2, 2), (3, None)])
async def test_processing_errors_retry_then_dlq(uow, payment, attempt, delay):
    publisher = RecordingPublisher()
    await processor(
        uow, Gateway(error=RuntimeError("gateway timeout")), Delivery(), publisher
    ).execute(PaymentMessage(payment.id, attempt))
    assert (await Payments(uow, now).get(payment.id)).status == PaymentStatus.PENDING
    if delay is None:
        assert publisher.dead[0]["attempt"] == 3
    else:
        assert publisher.retries[0][1] == delay
        assert publisher.retries[0][0]["attempt"] == attempt + 1


async def test_retry_publish_failure_propagates_for_input_requeue(uow, payment):
    publisher = RecordingPublisher()
    publisher.unavailable = True
    with pytest.raises(RuntimeError, match="broker unavailable"):
        await processor(uow, Gateway(error=RuntimeError("gateway")), Delivery(), publisher).execute(
            PaymentMessage(payment.id)
        )


@pytest.mark.parametrize("payload", [{}, {"payment_id": "bad"}, {"attempt": 0}])
async def test_malformed_payload_goes_to_dlq(uow, payload):
    publisher = RecordingPublisher()
    handler = PaymentMessageHandler(processor(uow, Gateway(), Delivery(), publisher), publisher, 3)
    await handler.handle(payload)
    assert publisher.dead[0]["stage"] == "validation"


async def test_missing_payment_retries(uow):
    publisher = RecordingPublisher()
    await processor(uow, Gateway(), Delivery(), publisher).execute(PaymentMessage(uuid4()))
    assert publisher.retries[0][0]["attempt"] == 2


async def test_notification_seam_observes_committed_payment(uow, payment):
    seen = []

    class Notifications:
        async def execute(self, payment_id):
            saved = await Payments(uow, now).get(payment_id)
            seen.append(saved)

    await ProcessPayment(
        uow, Gateway(), Notifications(), RecordingPublisher(), RetryPolicy(), now
    ).execute(PaymentMessage(payment.id))
    assert len(seen) == 1
    assert seen[0].status == PaymentStatus.SUCCEEDED
    assert seen[0].webhook_status == WebhookStatus.PENDING
