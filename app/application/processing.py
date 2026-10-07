import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from app.application.ports import (
    Clock,
    EventPublisher,
    PaymentGateway,
    PaymentNotifications,
    UnitOfWorkFactory,
)
from app.application.retry import RetryPolicy
from app.domain.enums import PaymentStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PaymentMessage:
    payment_id: UUID
    attempt: int = 1


class ProcessPayment:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        gateway: PaymentGateway,
        notifications: PaymentNotifications,
        publisher: EventPublisher,
        retry: RetryPolicy,
        clock: Clock,
    ) -> None:
        self._uow = uow
        self._gateway = gateway
        self._notifications = notifications
        self._publisher = publisher
        self._retry = retry
        self._clock = clock

    async def execute(self, message: PaymentMessage) -> None:
        if not 1 <= message.attempt <= self._retry.max_attempts:
            raise ValueError("attempt outside retry policy")
        try:
            async with self._uow() as transaction:
                payment = await transaction.payments.get(message.payment_id, lock=True)
                if payment is None:
                    raise LookupError(f"Payment {message.payment_id} not found")
                if payment.status == PaymentStatus.PENDING:
                    result = await self._gateway.process(payment)
                    payment = payment.finalize(result, self._clock())
                    await transaction.payments.save(payment)
                await transaction.commit()
        except Exception as exc:
            logger.warning("payment processing failed: %s", exc)
            payload: dict[str, Any] = {
                "payment_id": str(message.payment_id),
                "attempt": message.attempt,
                "error": str(exc),
            }
            if message.attempt < self._retry.max_attempts:
                payload["attempt"] = message.attempt + 1
                await self._publisher.retry_payment(
                    payload, self._retry.delay_after(message.attempt)
                )
            else:
                payload["stage"] = "payment"
                await self._publisher.dead_letter(payload)
            return

        await self._notifications.execute(message.payment_id)
