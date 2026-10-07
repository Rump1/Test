from uuid import UUID

from app.application.ports import Clock, DuplicatePaymentKey, UnitOfWorkFactory
from app.domain.events import OutboxEvent
from app.domain.payments import Payment, PaymentRequest


class IdempotencyConflictError(Exception):
    pass


class Payments:
    def __init__(self, uow: UnitOfWorkFactory, clock: Clock) -> None:
        self._uow = uow
        self._clock = clock

    async def create(self, request: PaymentRequest, key: str) -> Payment:
        payment = Payment.create(request, key, self._clock())
        try:
            async with self._uow() as transaction:
                existing = await transaction.payments.by_key(key)
                if existing:
                    return self._replay(existing, payment)
                await transaction.payments.add(payment)
                await transaction.outbox.add(OutboxEvent.payment_created(payment.id, self._clock()))
                await transaction.commit()
            return payment
        except DuplicatePaymentKey:
            # The failed transaction is already rolled back before reading the winner.
            async with self._uow() as transaction:
                existing = await transaction.payments.by_key(key)
                if existing is None:
                    raise
                return self._replay(existing, payment)

    @staticmethod
    def _replay(existing: Payment, requested: Payment) -> Payment:
        if existing.request_fingerprint != requested.request_fingerprint:
            raise IdempotencyConflictError("Idempotency key was already used with another payload")
        return existing

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self._uow() as transaction:
            return await transaction.payments.get(payment_id)
