import pytest
from fastapi import HTTPException

from app.presentation.http.dependencies import validate_api_key


@pytest.mark.parametrize("api_key", [None, "wrong"])
def test_validate_api_key_rejects_missing_or_invalid_key(api_key):
    with pytest.raises(HTTPException) as exc_info:
        validate_api_key(api_key, "dev-api-key")

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid API key"


def test_validate_api_key_accepts_configured_key():
    assert validate_api_key("dev-api-key", "dev-api-key") is None
