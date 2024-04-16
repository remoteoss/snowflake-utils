from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_")

    account: str = "snowflake-test"
    user: str = "snowlfake"
    password: str = "snowlfake"
    db: str = "snowlfake"
    role: str = "snowlfake"
    warehouse: str = "snowlfake"
    _schema: str | None = None

    def creds(self) -> dict[str, str | None]:
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.db,
            "schema": self._schema,
            "role": self.role,
            "warehouse": self.warehouse,
        }
