import pytest
from pydantic import ValidationError

from snowflake_utils.settings import GovernanceSettings, SnowflakeSettings


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


@pytest.mark.parametrize(
    "object_name",
    [
        "table",
        "governance.public.table",
    ],
)
def test_governance_settings(object_name: str) -> None:
    settings = GovernanceSettings()
    assert settings.fqn(object_name) == "governance.public.table"
