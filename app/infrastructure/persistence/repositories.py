from copy import deepcopy
from dataclasses import asdict
from datetime import UTC
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.ports import DuplicatePaymentKey
from app.domain.enums import Currency, OutboxStatus, PaymentStatus, WebhookStatus
from app.domain.events import OutboxEvent
from app.domain.payments import Payment, PaymentRequest
from app.infrastructure.persistence.models_outbox import OutboxRow
from app.infrastructure.persistence.models_payment import PaymentRow


def payment_from_row(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        request=PaymentRequest(
            amount=row.amount,
            currency=Currency(row.currency),
            description=row.description,
            metadata=deepcopy(row.metadata_),
            webhook_url=row.webhook_url,
        ),
        idempotency_key=row.idempotency_key,
        request_fingerprint=row.request_fingerprint,
        created_at=row.created_at.replace(tzinfo=UTC)
        if row.created_at.tzinfo is None
        else row.created_at,
        status=PaymentStatus(row.status),
        processed_at=row.processed_at,
        failure_reason=row.failure_reason,
        webhook_status=WebhookStatus(row.webhook_status),
        webhook_attempts=row.webhook_attempts,
        webhook_last_error=row.webhook_last_error,
        webhook_sent_at=row.webhook_sent_at,
    )


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: UUID, *, lock: bool = False) -> Payment | None:
        row = await self._session.get(
            PaymentRow, payment_id, with_for_update=lock, populate_existing=True
        )
        return payment_from_row(row) if row is not None else None

    async def by_key(self, key: str) -> Payment | None:
        row = await self._session.scalar(
            select(PaymentRow).where(PaymentRow.idempotency_key == key)
        )
        return payment_from_row(row) if row is not None else None

    async def add(self, payment: Payment) -> None:
        self._session.add(
            PaymentRow(
                id=payment.id,
                amount=payment.request.amount,
                currency=payment.request.currency,
                description=payment.request.description,
                metadata_=deepcopy(payment.request.metadata),
                webhook_url=payment.request.webhook_url,
                status=payment.status,
                idempotency_key=payment.idempotency_key,
                request_fingerprint=payment.request_fingerprint,
                created_at=payment.created_at,
                webhook_status=payment.webhook_status,
            )
        )
        try:
            await self._session.flush()
        except IntegrityError as exc:
            original = exc.orig
            sqlstate = getattr(original, "sqlstate", None)
            sqlite_unique = "UNIQUE constraint failed: payments.idempotency_key" in str(original)
            if (sqlstate == "23505" and "idempotency_key" in str(original)) or sqlite_unique:
                raise DuplicatePaymentKey(payment.idempotency_key) from exc
            raise

    async def save(self, payment: Payment) -> None:
        row = await self._session.get(PaymentRow, payment.id)
        if row is None:
            raise LookupError(f"Payment {payment.id} not found")
        # Immutable request fields are never changed by processing/delivery.
        for name in (
            "status",
            "processed_at",
            "failure_reason",
            "webhook_status",
            "webhook_attempts",
            "webhook_last_error",
            "webhook_sent_at",
        ):
            setattr(row, name, getattr(payment, name))
        await self._session.flush()


class SqlOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        self._session.add(OutboxRow(**asdict(event)))
        await self._session.flush()

    async def pending(self, limit: int) -> list[OutboxEvent]:
        rows = await self._session.scalars(
            select(OutboxRow)
            .where(
                OutboxRow.status.in_([OutboxStatus.PENDING, OutboxStatus.FAILED]),
                OutboxRow.event_type == "payment.created",
            )
            .order_by(OutboxRow.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [self._event(row) for row in rows]

    async def claim(self, event_id: UUID) -> OutboxEvent | None:
        row = await self._session.scalar(
            select(OutboxRow)
            .where(
                OutboxRow.id == event_id,
                OutboxRow.status.in_([OutboxStatus.PENDING, OutboxStatus.FAILED]),
                OutboxRow.event_type == "payment.created",
            )
            .with_for_update(skip_locked=True)
        )
        return self._event(row) if row is not None else None

    @staticmethod
    def _event(row: OutboxRow) -> OutboxEvent:
        return OutboxEvent(
            id=row.id,
            aggregate_id=row.aggregate_id,
            event_type=row.event_type,
            payload=deepcopy(row.payload),
            created_at=row.created_at,
            status=OutboxStatus(row.status),
            attempts=row.attempts,
            published_at=row.published_at,
            last_error=row.last_error,
        )

    async def save(self, event: OutboxEvent) -> None:
        row = await self._session.get(OutboxRow, event.id)
        if row is None:
            raise LookupError(f"Outbox event {event.id} not found")
        row.status = event.status
        row.attempts = event.attempts
        row.published_at = event.published_at
        row.last_error = event.last_error
        await self._session.flush()
