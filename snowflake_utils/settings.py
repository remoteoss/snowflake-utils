import os
from enum import Enum
from typing import Annotated

from pydantic import StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from snowflake.connector import SnowflakeConnection
from snowflake.connector import connect as _connect


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
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_")

    account: str = "snowflake-test"
    user: str = "snowlfake"
    password: str = "snowlfake"
    db: str = "snowlfake"
    role: str = "snowlfake"
    warehouse: str = "snowlfake"
    authenticator: Authenticator | OktaDomain = Authenticator.snowflake
    _schema: str | None = None
    private_key_file: str | None = None
    private_key_password: str | None = None

    def creds(self) -> dict[str, str | None]:
        base_creds = {
            "account": self.account,
            "user": self.user,
            "database": self.db,
            "schema": self._schema,
            "role": self.role,
            "warehouse": self.warehouse,
            "authenticator": str(self.authenticator),
        }
        if self.authenticator in (Authenticator.externalbrowser):
            return base_creds
        if self.private_key_file is not None and os.path.exists(self.private_key_file):
            return base_creds | {
                "private_key_file": self.private_key_file,
                "private_key_password": self.private_key_password,
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
