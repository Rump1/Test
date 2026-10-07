"""Dependency contracts owned by the application, not by a framework."""

from collections.abc import Awaitable, Callable
from datetime import datetime
from types import TracebackType
from typing import Any, Protocol
from uuid import UUID

from app.domain.events import OutboxEvent
from app.domain.payments import DeliveryResult, GatewayResult, Payment, PaymentNotification

Clock = Callable[[], datetime]
Sleep = Callable[[float], Awaitable[None]]


class DuplicatePaymentKey(Exception):
    """A concurrent transaction claimed the idempotency key."""


class PaymentRepository(Protocol):
    async def get(self, payment_id: UUID, *, lock: bool = False) -> Payment | None: ...
    async def by_key(self, key: str) -> Payment | None: ...
    async def add(self, payment: Payment) -> None: ...
    async def save(self, payment: Payment) -> None: ...


class OutboxRepository(Protocol):
    async def add(self, event: OutboxEvent) -> None: ...
    async def pending(self, limit: int) -> list[OutboxEvent]:
        """Snapshot eligible events, locking them until transaction exit."""
        ...

    async def claim(self, event_id: UUID) -> OutboxEvent | None:
        """Lock one eligible event until transaction exit; skip busy/final events."""
        ...

    async def save(self, event: OutboxEvent) -> None: ...


class UnitOfWork(Protocol):
    payments: PaymentRepository
    outbox: OutboxRepository

    async def __aenter__(self) -> "UnitOfWork": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    async def commit(self) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]


class PaymentGateway(Protocol):
    async def process(self, payment: Payment) -> GatewayResult: ...


class NotificationTransport(Protocol):
    async def send(self, notification: PaymentNotification) -> None: ...


class DeliveryError(Exception):
    """A notification was not accepted by its recipient."""


class NotificationDelivery(Protocol):
    async def deliver(self, notification: PaymentNotification) -> DeliveryResult: ...


class PaymentNotifications(Protocol):
    async def execute(self, payment_id: UUID) -> None: ...


class EventPublisher(Protocol):
    async def publish_payment(self, payload: dict[str, Any]) -> None: ...
    async def retry_payment(self, payload: dict[str, Any], delay_seconds: int) -> None: ...
    async def dead_letter(self, payload: dict[str, Any]) -> None: ...
