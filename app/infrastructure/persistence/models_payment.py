import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.domain.enums import Currency, PaymentStatus, WebhookStatus
from app.infrastructure.persistence.base import Base
from app.infrastructure.persistence.types import GUID


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("currency in ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint(
            "status in ('pending', 'succeeded', 'failed')",
            name="ck_payments_status",
        ),
        CheckConstraint(
            "webhook_status in ('pending', 'sent', 'failed')",
            name="ck_payments_webhook_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
        default=dict,
    )
    status: Mapped[PaymentStatus] = mapped_column(
        String(20),
        nullable=False,
        default=PaymentStatus.PENDING,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    webhook_status: Mapped[WebhookStatus] = mapped_column(
        String(20),
        nullable=False,
        default=WebhookStatus.PENDING,
    )
    webhook_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    webhook_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    webhook_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(1000), nullable=True)
