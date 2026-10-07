"""Payment state and invariants, independent of persistence and transports."""

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from app.domain.enums import Currency, PaymentStatus, WebhookStatus


class InvalidPayment(ValueError):
    pass


class InvalidTransition(ValueError):
    pass


@dataclass(frozen=True, kw_only=True, init=False)
class PaymentRequest:
    amount: Decimal
    currency: Currency
    description: str
    webhook_url: str
    _metadata_json: str = field(repr=False)

    def __init__(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
    ) -> None:
        if not isinstance(metadata, dict):
            raise InvalidPayment("metadata must be an object")
        try:
            encoded = json.dumps(
                metadata, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False
            )
        except (TypeError, ValueError) as exc:
            raise InvalidPayment("metadata must contain JSON values") from exc
        if len(encoded.encode()) > 16384:
            raise InvalidPayment("metadata must be at most 16 KB")
        for name, value in (
            ("amount", amount),
            ("currency", currency),
            ("description", description),
            ("webhook_url", webhook_url),
            ("_metadata_json", encoded),
        ):
            object.__setattr__(self, name, value)
        self._validate()
        object.__setattr__(self, "amount", amount.quantize(Decimal("0.01")))

    @property
    def metadata(self) -> dict[str, Any]:
        # A fresh JSON value prevents callers from mutating the idempotent request.
        return json.loads(self._metadata_json)

    def _validate(self) -> None:
        if not self.amount.is_finite() or not 0 < self.amount < Decimal("10000000000"):
            raise InvalidPayment("amount must be positive and fit decimal(12, 2)")
        if self.amount != self.amount.quantize(Decimal("0.01")):
            raise InvalidPayment("amount must have at most 2 fractional digits")
        if not isinstance(self.currency, Currency):
            raise InvalidPayment("unsupported currency")
        if not 1 <= len(self.description) <= 500:
            raise InvalidPayment("description must contain 1 to 500 characters")
        parsed = urlparse(self.webhook_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or len(self.webhook_url) > 2048
        ):
            raise InvalidPayment("webhook_url must be a valid HTTP or HTTPS URL")

    def fingerprint(self) -> str:
        payload = dict(
            amount=format(self.amount, ".2f"),
            currency=self.currency.value,
            description=self.description,
            metadata=self.metadata,
            webhook_url=self.webhook_url,
        )
        value = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True)
class GatewayResult:
    succeeded: bool
    failure_reason: str | None = None


@dataclass(frozen=True)
class DeliveryResult:
    sent: bool
    attempts: int
    error: str | None = None
    sent_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.attempts < 1 or (self.sent and self.sent_at is None):
            raise ValueError("delivery result requires attempts and a timestamp on success")


@dataclass(frozen=True, kw_only=True)
class Payment:
    id: UUID
    request: PaymentRequest
    idempotency_key: str
    request_fingerprint: str
    created_at: datetime
    status: PaymentStatus = PaymentStatus.PENDING
    processed_at: datetime | None = None
    failure_reason: str | None = None
    webhook_status: WebhookStatus = WebhookStatus.PENDING
    webhook_attempts: int = 0
    webhook_last_error: str | None = None
    webhook_sent_at: datetime | None = None

    @classmethod
    def create(cls, request: PaymentRequest, key: str, now: datetime) -> "Payment":
        if not key or len(key) > 255:
            raise InvalidPayment("idempotency key must contain 1 to 255 characters")
        return cls(
            id=uuid4(),
            request=request,
            idempotency_key=key,
            request_fingerprint=request.fingerprint(),
            created_at=now,
        )

    def finalize(self, result: GatewayResult, now: datetime) -> "Payment":
        if self.status != PaymentStatus.PENDING:
            raise InvalidTransition("a final payment cannot be processed again")
        return replace(
            self,
            status=PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED,
            failure_reason=result.failure_reason,
            processed_at=now,
        )

    def record_delivery(self, result: DeliveryResult) -> "Payment":
        if self.status == PaymentStatus.PENDING or self.webhook_status != WebhookStatus.PENDING:
            raise InvalidTransition("delivery requires a final payment and a pending webhook")
        return replace(
            self,
            webhook_status=WebhookStatus.SENT if result.sent else WebhookStatus.FAILED,
            webhook_attempts=result.attempts,
            webhook_last_error=result.error,
            webhook_sent_at=result.sent_at,
        )

    def notification(self) -> "PaymentNotification":
        if self.status == PaymentStatus.PENDING:
            raise InvalidTransition("pending payments have no result notification")
        return PaymentNotification(
            event_id=uuid5(NAMESPACE_URL, f"payment:{self.id}:{self.status.value}"),
            payment_id=self.id,
            url=self.request.webhook_url,
            status=self.status,
            amount=self.request.amount,
            currency=self.request.currency,
            processed_at=self.processed_at,
            metadata=deepcopy(self.request.metadata),
        )


@dataclass(frozen=True, kw_only=True)
class PaymentNotification:
    event_id: UUID
    payment_id: UUID
    url: str
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None
    metadata: dict[str, Any] = field(default_factory=dict)
