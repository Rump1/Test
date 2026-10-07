from typing import Any
from uuid import UUID

from app.application.ports import EventPublisher
from app.application.processing import PaymentMessage, ProcessPayment


class PaymentMessageHandler:
    def __init__(
        self, processor: ProcessPayment, publisher: EventPublisher, max_attempts: int
    ) -> None:
        self._processor = processor
        self._publisher = publisher
        self._max_attempts = max_attempts

    async def handle(self, payload: dict[str, Any]) -> None:
        try:
            attempt = int(payload.get("attempt", 1))
            if not 1 <= attempt <= self._max_attempts:
                raise ValueError("attempt outside retry policy")
            message = PaymentMessage(UUID(str(payload["payment_id"])), attempt)
        except (KeyError, ValueError, TypeError) as exc:
            await self._publisher.dead_letter(
                {"payload": payload, "stage": "validation", "error": str(exc)}
            )
            return
        await self._processor.execute(message)
