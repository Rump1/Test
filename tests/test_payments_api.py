from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.application.payments import Payments
from app.main import create_app
from tests.conftest import now


@pytest.fixture
async def api_client(uow):
    app = create_app(payments=Payments(uow, now))
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        yield client


def valid_payload(**overrides):
    payload = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order #123",
        "metadata": {"order_id": "123"},
        "webhook_url": "https://client.example.com/payment-webhook",
    }
    payload.update(overrides)
    return payload


def headers(**overrides):
    value = {"X-API-Key": "dev-api-key", "Idempotency-Key": "key-1"}
    value.update(overrides)
    return value


async def test_create_payment_returns_accepted_response(api_client):
    response = await api_client.post(
        "/api/v1/payments",
        json=valid_payload(),
        headers=headers(),
    )

    body = response.json()
    assert response.status_code == 202
    assert body["payment_id"]
    assert body["status"] == "pending"
    assert body["created_at"]


async def test_create_payment_replays_same_idempotency_key(api_client):
    first = await api_client.post("/api/v1/payments", json=valid_payload(), headers=headers())
    second = await api_client.post("/api/v1/payments", json=valid_payload(), headers=headers())

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_create_payment_conflicts_on_same_key_with_different_payload(api_client):
    first = await api_client.post("/api/v1/payments", json=valid_payload(), headers=headers())
    second = await api_client.post(
        "/api/v1/payments",
        json=valid_payload(amount="2000.00"),
        headers=headers(),
    )

    assert first.status_code == 202
    assert second.status_code == 409


async def test_create_payment_requires_idempotency_key(api_client):
    response = await api_client.post(
        "/api/v1/payments",
        json=valid_payload(),
        headers={"X-API-Key": "dev-api-key"},
    )

    assert response.status_code == 400


async def test_create_payment_requires_api_key(api_client):
    response = await api_client.post(
        "/api/v1/payments",
        json=valid_payload(),
        headers={"Idempotency-Key": "key-1"},
    )

    assert response.status_code == 401


async def test_get_payment_returns_full_details(api_client):
    created = await api_client.post("/api/v1/payments", json=valid_payload(), headers=headers())
    payment_id = created.json()["payment_id"]

    response = await api_client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": "dev-api-key"},
    )

    body = response.json()
    assert response.status_code == 200
    assert body["id"] == payment_id
    assert body["amount"] == "1500.50"
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": "123"}
    assert body["webhook_status"] == "pending"


async def test_get_payment_returns_404_for_unknown_payment(api_client):
    response = await api_client.get(
        f"/api/v1/payments/{uuid4()}",
        headers={"X-API-Key": "dev-api-key"},
    )

    assert response.status_code == 404
