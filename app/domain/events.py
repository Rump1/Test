from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from app.domain.enums import OutboxStatus


@dataclass(frozen=True, kw_only=True)
class OutboxEvent:
    id: UUID
    aggregate_id: UUID
    event_type: str
    payload: dict[str, Any]
    created_at: datetime
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    published_at: datetime | None = None
    last_error: str | None = None

    @classmethod
    def payment_created(cls, payment_id: UUID, now: datetime) -> "OutboxEvent":
        return cls(
            id=uuid4(),
            aggregate_id=payment_id,
            event_type="payment.created",
            payload={"event_type": "payment.created", "payment_id": str(payment_id), "attempt": 1},
            created_at=now,
        )

    def attempted(self) -> "OutboxEvent":
        return replace(self, attempts=self.attempts + 1)

    def published(self, now: datetime) -> "OutboxEvent":
        return replace(self, status=OutboxStatus.PUBLISHED, published_at=now, last_error=None)

    def deferred(self, error: str) -> "OutboxEvent":
        return replace(self, status=OutboxStatus.PENDING, last_error=error)
