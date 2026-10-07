from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from app.domain.enums import PaymentStatus
from app.domain.payments import (
    DeliveryResult,
    GatewayResult,
    InvalidPayment,
    InvalidTransition,
    Payment,
)
from tests.conftest import now, request


def test_payment_state_is_immutable_and_finalization_is_one_way():
    payment = Payment.create(request(), "key", now())
    final = payment.finalize(GatewayResult(True), now())
    assert payment.status == PaymentStatus.PENDING
    with pytest.raises(FrozenInstanceError):
        final.status = PaymentStatus.PENDING
    with pytest.raises(InvalidTransition):
        final.finalize(GatewayResult(False, "declined"), now())


def test_notification_requires_final_payment():
    payment = Payment.create(request(), "key", now())
    with pytest.raises(InvalidTransition):
        payment.notification()
    with pytest.raises(InvalidTransition):
        payment.record_delivery(DeliveryResult(True, 1, sent_at=now()))


def test_delivery_result_cannot_overwrite_a_final_delivery():
    payment = Payment.create(request(), "key", now()).finalize(GatewayResult(True), now())
    sent = payment.record_delivery(DeliveryResult(True, 1, sent_at=now()))
    with pytest.raises(InvalidTransition):
        sent.record_delivery(DeliveryResult(False, 3, "HTTP 503"))


@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-1", "0", "10000000000", "0.001"])
def test_domain_validates_amount_without_http(amount):
    with pytest.raises(InvalidPayment):
        request(amount=Decimal(amount))


def test_metadata_cannot_change_after_idempotent_request_is_created():
    original = {"nested": {"items": [1]}}
    value = request(metadata=original)
    fingerprint = value.fingerprint()
    original["nested"]["items"].append(2)
    value.metadata["nested"]["items"].append(3)
    assert value.metadata == {"nested": {"items": [1]}}
    assert value.fingerprint() == fingerprint
