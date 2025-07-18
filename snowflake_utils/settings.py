import os
from enum import Enum
from logging import getLogger
from typing import Annotated

from pydantic import AliasChoices, Field, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from snowflake.connector import SnowflakeConnection
from snowflake.connector import connect as _connect

logger = getLogger(__name__)


class Authenticator(str, Enum):
    snowflake = "snowflake"
    externalbrowser = "externalbrowser"
    username_password_mfa = "username_password_mfa"

    def __str__(self) -> str:
        return self.value


OktaDomain = Annotated[
    str,
    StringConstraints(pattern=r"https://.*\.okta\.com"),
]


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_", populate_by_name=True)

    account: str = "snowflake-test"
    user: str = "snowlfake"
    password: str = "snowlfake"
    db: str = "snowlfake"
    role: str = "snowlfake"
    warehouse: str = "snowlfake"
    authenticator: Authenticator | OktaDomain = Authenticator.snowflake
    schema_name: str | None = Field(
        default=None, validation_alias=AliasChoices("SNOWFLAKE_SCHEMA")
    )
    private_key_file: str | None = None
    private_key_password: str | None = None
    application: str | None = None

    def creds(self) -> dict[str, str | None]:
        base_creds = {
            "account": self.account,
            "user": self.user,
            "database": self.db,
            "schema": self.schema_name,
            "role": self.role,
            "warehouse": self.warehouse,
            "authenticator": str(self.authenticator),
        }

        if self.application is not None:
            base_creds["application"] = self.application
        else:
            logger.warning("No Snowflake application name provided!")

        if self.authenticator in (Authenticator.externalbrowser):
            return base_creds
        if self.private_key_file is not None and os.path.exists(self.private_key_file):
            return base_creds | {
                "private_key_file": self.private_key_file,
                "private_key_file_pwd": self.private_key_password,
            }
        return base_creds | {"password": self.password}

    def connect(self) -> SnowflakeConnection:
        return _connect(**self.creds())


def connect() -> SnowflakeConnection:
    return SnowflakeSettings().connect()


class GovernanceSettings(BaseSettings):
    governance_database: str = "governance"
    governance_schema: str = "public"

    def fqn(self, object_name: str) -> str:
        if len(object_name.split(".")) == 3:
            return object_name
        return f"{self.governance_database}.{self.governance_schema}.{object_name}"


governance_settings = GovernanceSettings()
