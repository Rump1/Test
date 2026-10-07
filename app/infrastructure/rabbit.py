from typing import Any

from faststream.rabbit import RabbitBroker, RabbitQueue

PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True)
RETRY_QUEUE = RabbitQueue(
    "payments.new.retry",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": PAYMENTS_QUEUE.name,
    },
)
DLQ = RabbitQueue("payments.new.dlq", durable=True)


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def declare_topology(self) -> None:
        for queue in (PAYMENTS_QUEUE, RETRY_QUEUE, DLQ):
            await self._broker.declare_queue(queue)

    async def publish_payment(self, payload: dict[str, Any]) -> None:
        await self._publish(payload, PAYMENTS_QUEUE)

    async def retry_payment(self, payload: dict[str, Any], delay_seconds: int) -> None:
        await self._publish(payload, RETRY_QUEUE, delay_seconds)

    async def dead_letter(self, payload: dict[str, Any]) -> None:
        await self._publish(payload, DLQ)

    async def _publish(
        self, payload: dict[str, Any], queue: RabbitQueue, delay_seconds: int | None = None
    ) -> None:
        await self._broker.publish(
            payload,
            queue=queue,
            routing_key=queue.name,
            persist=True,
            mandatory=True,
            expiration=delay_seconds,
        )
