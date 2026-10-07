import httpx
import pytest

from app.application.notifications import WebhookDelivery
from app.application.ports import DeliveryError
from app.application.retry import RetryPolicy
from app.domain.payments import GatewayResult, Payment
from app.infrastructure.webhooks import HttpWebhookTransport
from tests.conftest import now, request


def notification():
    return (
        Payment.create(request(), "key", now()).finalize(GatewayResult(True), now()).notification()
    )


@pytest.mark.parametrize("codes,sent", [([500, 502, 200], True), ([400, 500, 503], False)])
async def test_http_delivery_outcome_and_retry_contract(codes, sent):
    calls, delays = [], []
    target = notification()

    def respond(request):
        calls.append(request)
        return httpx.Response(codes[len(calls) - 1])

    async def sleep(delay):
        delays.append(delay)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        outcome = await WebhookDelivery(
            HttpWebhookTransport(client), RetryPolicy(), now, sleep
        ).deliver(target)
    assert outcome.sent == sent
    assert outcome.attempts == 3
    assert delays == [1, 2]
    assert calls[0].headers["X-Payment-Id"] == str(target.payment_id)
    assert calls[0].headers["X-Payment-Event"] == "payment.succeeded"
    assert calls[0].content == calls[2].content
    assert outcome.sent_at == (now() if sent else None)
    assert outcome.error == (None if sent else "HTTP 503")


async def test_http_timeout_becomes_delivery_error():
    def timeout(request):
        raise httpx.ReadTimeout("timeout", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
        with pytest.raises(DeliveryError, match="timeout"):
            await HttpWebhookTransport(client).send(notification())


def test_event_id_stable_for_same_payment():
    payment = Payment.create(request(), "key", now()).finalize(GatewayResult(True), now())
    assert payment.notification().event_id == payment.notification().event_id


async def test_unexpected_transport_error_preserves_actual_attempt_count():
    from tests.conftest import no_sleep

    class Transport:
        calls = 0

        async def send(self, notification):
            self.calls += 1
            if self.calls == 1:
                raise DeliveryError("HTTP 503")
            raise RuntimeError("unexpected transport failure")

    transport = Transport()
    outcome = await WebhookDelivery(transport, RetryPolicy(), now, no_sleep).deliver(notification())
    assert not outcome.sent
    assert outcome.attempts == transport.calls == 2
    assert outcome.error == "unexpected transport failure"
