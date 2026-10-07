from datetime import datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from app.domain.enums import Currency, PaymentStatus, WebhookStatus
from app.domain.payments import Payment, PaymentRequest


class PaymentCreate(BaseModel):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str

    def to_request(self) -> PaymentRequest:
        return PaymentRequest(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=self.webhook_url,
        )

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        self.to_request()
        return self


class PaymentCreatedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentCreatedResponse":
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentDetailResponse(BaseModel):
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    webhook_status: WebhookStatus
    webhook_attempts: int
    webhook_last_error: str | None
    webhook_sent_at: datetime | None
    created_at: datetime
    processed_at: datetime | None
    failure_reason: str | None

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentDetailResponse":
        return cls(
            id=payment.id,
            amount=payment.request.amount,
            currency=payment.request.currency,
            description=payment.request.description,
            metadata=payment.request.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.request.webhook_url,
            webhook_status=payment.webhook_status,
            webhook_attempts=payment.webhook_attempts,
            webhook_last_error=payment.webhook_last_error,
            webhook_sent_at=payment.webhook_sent_at,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            failure_reason=payment.failure_reason,
        )
