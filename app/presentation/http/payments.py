from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.application.payments import IdempotencyConflictError, Payments
from app.domain.payments import InvalidPayment
from app.presentation.http.dependencies import get_payments, require_api_key
from app.presentation.http.schemas import (
    PaymentCreate,
    PaymentCreatedResponse,
    PaymentDetailResponse,
)

router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)]
)


@router.post("", response_model=PaymentCreatedResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_payment_endpoint(
    payment_in: PaymentCreate,
    payments: Annotated[Payments, Depends(get_payments)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> PaymentCreatedResponse:
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")
    try:
        payment = await payments.create(payment_in.to_request(), idempotency_key)
    except IdempotencyConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except InvalidPayment as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return PaymentCreatedResponse.from_payment(payment)


@router.get("/{payment_id}", response_model=PaymentDetailResponse)
async def get_payment_endpoint(
    payment_id: UUID,
    payments: Annotated[Payments, Depends(get_payments)],
) -> PaymentDetailResponse:
    payment = await payments.get(payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetailResponse.from_payment(payment)
