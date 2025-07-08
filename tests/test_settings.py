import os
from unittest.mock import patch

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


def test_schema_name() -> None:
    settings = SnowflakeSettings(schema_name="foo")
    assert settings.schema_name == "foo"


def test_schema_name_from_env() -> None:
    with patch.dict(os.environ, {"SNOWFLAKE_SCHEMA": "bar"}):
        settings = SnowflakeSettings()
        assert settings.schema_name == "bar"


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
