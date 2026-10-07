from uuid import UUID

from app.application.ports import (
    Clock,
    DeliveryError,
    EventPublisher,
    NotificationDelivery,
    NotificationTransport,
    Sleep,
    UnitOfWorkFactory,
)
from app.application.retry import RetryPolicy
from app.domain.enums import WebhookStatus
from app.domain.payments import DeliveryResult, PaymentNotification


class WebhookDelivery:
    def __init__(
        self, transport: NotificationTransport, retry: RetryPolicy, clock: Clock, sleep: Sleep
    ) -> None:
        self._transport = transport
        self._retry = retry
        self._clock = clock
        self._sleep = sleep

    async def deliver(self, notification: PaymentNotification) -> DeliveryResult:
        error = None
        for attempt in range(1, self._retry.max_attempts + 1):
            try:
                await self._transport.send(notification)
            except DeliveryError as exc:
                error = str(exc)
                if attempt < self._retry.max_attempts:
                    await self._sleep(self._retry.delay_after(attempt))
            except Exception as exc:
                # Unexpected transport failures are terminal, but still preserve
                # the number of attempts actually made. Cancellation propagates.
                return DeliveryResult(sent=False, attempts=attempt, error=str(exc))
            else:
                return DeliveryResult(sent=True, attempts=attempt, sent_at=self._clock())
        return DeliveryResult(sent=False, attempts=self._retry.max_attempts, error=error)


class DeliverPaymentNotification:
    """Own delivery state, redelivery recovery and terminal failure publication."""

    def __init__(
        self,
        uow: UnitOfWorkFactory,
        delivery: NotificationDelivery,
        publisher: EventPublisher,
    ) -> None:
        self._uow = uow
        self._delivery = delivery
        self._publisher = publisher

    async def execute(self, payment_id: UUID) -> None:
        # The caller has already committed the payment result. Serialize concurrent
        # deliveries while keeping payment and delivery outcomes independent.
        async with self._uow() as transaction:
            payment = await transaction.payments.get(payment_id, lock=True)
            if payment is None:
                raise LookupError(f"Payment {payment_id} not found")
            if payment.webhook_status == WebhookStatus.SENT:
                return
            if payment.webhook_status == WebhookStatus.PENDING:
                # Unexpected orchestration errors propagate for input requeue;
                # only the delivery implementation knows its attempt count.
                result = await self._delivery.deliver(payment.notification())
                payment = payment.record_delivery(result)
                await transaction.payments.save(payment)
                await transaction.commit()

        if payment.webhook_status == WebhookStatus.FAILED:
            # Failure propagates to input NACK. A redelivery resumes here without
            # calling the transport again. DLQ publication itself is at-least-once.
            await self._publisher.dead_letter(
                {
                    "payment_id": str(payment.id),
                    "stage": "webhook",
                    "attempt": payment.webhook_attempts,
                    "error": payment.webhook_last_error,
                }
            )
