import pytest
from pydantic import ValidationError

from snowflake_utils.settings import SnowflakeSettings


@pytest.mark.parametrize(
    "authenticator",
    [
        "snowflake",
        "externalbrowser",
        "username_password_mfa",
        "https://valid.okta.com",
    ],
)
def test_authenticator(authenticator: str) -> None:
    settings = SnowflakeSettings(authenticator=authenticator)
    assert settings.authenticator == authenticator


def test_authenticator_invalid() -> None:
    with pytest.raises(ValidationError):
        SnowflakeSettings(authenticator="invalid")
