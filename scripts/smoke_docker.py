"""Run: docker compose exec -T api python - < scripts/smoke_docker.py."""

import asyncio
import json
import threading
import time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from uuid import uuid4

import aio_pika
import httpx
from sqlalchemy import delete

from app.application.notifications import DeliverPaymentNotification
from app.application.processing import PaymentMessage, ProcessPayment
from app.application.retry import RetryPolicy
from app.bootstrap import Database, utc_now
from app.domain.enums import Currency
from app.domain.payments import DeliveryResult, GatewayResult, Payment, PaymentRequest
from app.infrastructure.config import get_settings
from app.infrastructure.persistence.models_payment import PaymentRow

requests = []


class Webhook(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        requests.append((self.path, body, time.monotonic()))
        self.send_response(503 if self.path == "/fail" else 200)
        self.end_headers()

    def log_message(self, *args):
        pass


async def check_concurrent_deliveries():
    calls = []
    database = Database(get_settings().database_url)

    class Gateway:
        async def process(self, payment):
            calls.append("gateway")
            await asyncio.sleep(0.2)
            return GatewayResult(True)

    class Publisher:
        async def retry_payment(self, *args):
            raise AssertionError("No retry expected")

        async def dead_letter(self, *args):
            raise AssertionError("No DLQ expected")

    class Delivery:
        async def deliver(self, notification):
            calls.append("webhook")
            saved = await database.payments.get(notification.payment_id)
            assert saved.status == "succeeded"
            await asyncio.sleep(0.2)
            return DeliveryResult(True, 1, sent_at=utc_now())

    payment = Payment.create(
        PaymentRequest(
            amount=Decimal("10.00"),
            currency=Currency.RUB,
            description="concurrent smoke",
            metadata={},
            webhook_url="http://unused",
        ),
        str(uuid4()),
        utc_now(),
    )
    try:
        async with database.uow() as tx:
            await tx.payments.add(payment)
            await tx.commit()
        processor = ProcessPayment(
            database.uow,
            Gateway(),
            DeliverPaymentNotification(database.uow, Delivery(), Publisher()),
            Publisher(),
            RetryPolicy(),
            utc_now,
        )
        await asyncio.gather(
            processor.execute(PaymentMessage(payment.id)),
            processor.execute(PaymentMessage(payment.id)),
        )
        assert calls == ["gateway", "webhook"], calls
        print("Concurrent duplicate deliveries and commit before webhook on PostgreSQL: OK")
    finally:
        async with database.sessions() as session:
            await session.execute(delete(PaymentRow).where(PaymentRow.id == payment.id))
            await session.commit()
        await database.close()


async def main():
    await check_concurrent_deliveries()
    settings = get_settings()
    server = HTTPServer(("0.0.0.0", 8099), Webhook)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        async with httpx.AsyncClient(
            base_url="http://localhost:8000", headers={"X-API-Key": settings.api_key}
        ) as client:
            for path in ("/ok", "/fail"):
                payload = {
                    "amount": "10.00",
                    "currency": "RUB",
                    "description": "smoke test",
                    "metadata": {},
                    "webhook_url": f"http://api:8099{path}",
                }
                headers = {"Idempotency-Key": str(uuid4())}
                responses = await asyncio.gather(
                    *(
                        client.post("/api/v1/payments", json=payload, headers=headers)
                        for _ in range(8)
                    )
                )
                assert all(r.status_code == 202 for r in responses), [r.text for r in responses]
                assert len({r.json()["payment_id"] for r in responses}) == 1
                created = responses[0]
                assert created.status_code == 202, created.text
                payment_id = created.json()["payment_id"]
                replay = await client.post("/api/v1/payments", json=payload, headers=headers)
                assert replay.json()["payment_id"] == payment_id
                for _ in range(150):
                    response = await client.get(f"/api/v1/payments/{payment_id}")
                    response.raise_for_status()
                    payment = response.json()
                    if payment["webhook_status"] != "pending":
                        break
                    await asyncio.sleep(0.1)
                assert payment["status"] in {"succeeded", "failed"}, payment
                expected = "sent" if path == "/ok" else "failed"
                assert payment["webhook_status"] == expected, payment
                calls = [r for r in requests if r[1]["payment_id"] == payment_id]
                assert len(calls) == (1 if path == "/ok" else 3)
                assert len({r[1]["event_id"] for r in calls}) == 1
                print(f"API → outbox → RabbitMQ → payment → webhook {path}: OK")
                if path == "/fail":
                    failed_id = payment_id
                    assert 0.9 <= calls[1][2] - calls[0][2] < 3
                    assert 1.9 <= calls[2][2] - calls[1][2] < 4

        channel = await connection.channel()
        queue = await channel.declare_queue("payments.new.dlq", durable=True)
        missing_id = str(uuid4())
        started = time.monotonic()
        await channel.default_exchange.publish(
            aio_pika.Message(
                json.dumps({"payment_id": missing_id, "attempt": 1}).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="payments.new",
        )
        found = set()
        # Unrelated messages stay unacknowledged and return when the connection closes.
        for _ in range(150):
            message = await queue.get(fail=False)
            if message:
                body = json.loads(message.body)
                payment_id = body.get("payment_id")
                if payment_id in {failed_id, missing_id}:
                    assert body["attempt"] == 3, body
                    assert body["stage"] == ("webhook" if payment_id == failed_id else "payment")
                    found.add(payment_id)
                    await message.ack()
            if found == {failed_id, missing_id}:
                break
            await asyncio.sleep(0.1)
        assert found == {failed_id, missing_id}, found
        assert 2.8 <= time.monotonic() - started < 12
        print("Webhook DLQ and payment retry 1s/2s → DLQ: OK")
    finally:
        await connection.close()
        server.shutdown()
        server.server_close()


asyncio.run(main())
