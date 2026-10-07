import asyncio
from copy import deepcopy

from app.application.ports import DuplicatePaymentKey
from app.domain.enums import OutboxStatus


class MemoryStore:
    def __init__(self):
        self.payments = {}
        self.outbox = {}
        self.lock = asyncio.Lock()

    def uow(self):
        return MemoryUnitOfWork(self)


class MemoryPayments:
    def __init__(self, values):
        self.values = values

    async def get(self, payment_id, *, lock=False):
        return deepcopy(self.values.get(payment_id))

    async def by_key(self, key):
        return next((deepcopy(p) for p in self.values.values() if p.idempotency_key == key), None)

    async def add(self, payment):
        if await self.by_key(payment.idempotency_key):
            raise DuplicatePaymentKey(payment.idempotency_key)
        self.values[payment.id] = deepcopy(payment)

    async def save(self, payment):
        self.values[payment.id] = deepcopy(payment)


class MemoryOutbox:
    def __init__(self, values):
        self.values = values

    async def add(self, event):
        self.values[event.id] = deepcopy(event)

    async def pending(self, limit):
        return [
            deepcopy(e)
            for e in sorted(self.values.values(), key=lambda e: e.created_at)
            if e.status in (OutboxStatus.PENDING, OutboxStatus.FAILED)
            and e.event_type == "payment.created"
        ][:limit]

    async def claim(self, event_id):
        event = self.values.get(event_id)
        if (
            event is not None
            and event.status in (OutboxStatus.PENDING, OutboxStatus.FAILED)
            and event.event_type == "payment.created"
        ):
            return deepcopy(event)
        return None

    async def save(self, event):
        self.values[event.id] = deepcopy(event)


class MemoryUnitOfWork:
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        await self.store.lock.acquire()
        self.payments = MemoryPayments(deepcopy(self.store.payments))
        self.outbox = MemoryOutbox(deepcopy(self.store.outbox))
        return self

    async def __aexit__(self, *args):
        self.store.lock.release()

    async def commit(self):
        self.store.payments = deepcopy(self.payments.values)
        self.store.outbox = deepcopy(self.outbox.values)


class RecordingPublisher:
    def __init__(self):
        self.published = []
        self.retries = []
        self.dead = []
        self.unavailable = False

    def check(self):
        if self.unavailable:
            raise RuntimeError("broker unavailable")

    async def publish_payment(self, payload):
        self.check()
        self.published.append(deepcopy(payload))

    async def retry_payment(self, payload, delay_seconds):
        self.check()
        self.retries.append((deepcopy(payload), delay_seconds))

    async def dead_letter(self, payload):
        self.check()
        self.dead.append(deepcopy(payload))
