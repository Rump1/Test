from app.application.ports import Clock, EventPublisher, Sleep, UnitOfWorkFactory
from app.application.retry import RetryPolicy


class PublishOutbox:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        publisher: EventPublisher,
        retry: RetryPolicy,
        clock: Clock,
        sleep: Sleep,
    ) -> None:
        self._uow = uow
        self._publisher = publisher
        self._retry = retry
        self._clock = clock
        self._sleep = sleep

    async def execute(self, limit: int = 100) -> int:
        if limit < 1:
            raise ValueError("limit must be positive")
        # A bounded snapshot, not a reservation: release batch locks immediately.
        async with self._uow() as transaction:
            candidates = await transaction.outbox.pending(limit)

        count = 0
        for candidate in candidates:
            for attempt in range(1, self._retry.max_attempts + 1):
                async with self._uow() as transaction:
                    # Recheck eligibility under a fresh lock: another publisher may
                    # have published or claimed this event since the snapshot.
                    event = await transaction.outbox.claim(candidate.id)
                    if event is None:
                        break
                    event = event.attempted()
                    try:
                        await self._publisher.publish_payment(event.payload)
                    except Exception as exc:
                        event = event.deferred(str(exc))
                        published = False
                    else:
                        event = event.published(self._clock())
                        published = True
                    await transaction.outbox.save(event)
                    await transaction.commit()
                if published:
                    count += 1
                    break
                # Backoff never holds a database transaction or row lock.
                if attempt < self._retry.max_attempts:
                    await self._sleep(self._retry.delay_after(attempt))
        return count
