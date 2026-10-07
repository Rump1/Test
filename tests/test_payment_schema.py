from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.presentation.http.schemas import PaymentCreate


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


def test_payment_create_accepts_valid_payload():
    schema = PaymentCreate.model_validate(valid_payload())

    assert schema.amount == Decimal("1500.50")
    assert schema.currency == "RUB"
    assert schema.metadata == {"order_id": "123"}


@pytest.mark.parametrize("amount", ["0", "-1.00", "10.001"])
def test_payment_create_rejects_invalid_amount(amount):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(valid_payload(amount=amount))


def test_payment_create_rejects_unsupported_currency():
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(valid_payload(currency="GBP"))


@pytest.mark.parametrize("description", ["", "x" * 501])
def test_payment_create_rejects_invalid_description_length(description):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(valid_payload(description=description))


def test_payment_create_rejects_non_object_metadata():
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(valid_payload(metadata=["not", "object"]))


def test_payment_create_rejects_large_metadata():
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(valid_payload(metadata={"blob": "x" * (16 * 1024)}))


@pytest.mark.parametrize("url", ["ftp://example.com/hook", "not-a-url"])
def test_payment_create_rejects_non_http_webhook_url(url):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(valid_payload(webhook_url=url))
