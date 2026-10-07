from hmac import compare_digest

from fastapi import Header, HTTPException, Request, status

from app.application.payments import Payments


def validate_api_key(api_key: str | None, expected: str) -> None:
    if api_key is None or not compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


async def require_api_key(
    request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")
) -> None:
    validate_api_key(x_api_key, request.app.state.api_key)


def get_payments(request: Request) -> Payments:
    return request.app.state.payments
