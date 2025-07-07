from pydantic import BaseModel
from snowflake.connector.cursor import SnowflakeCursor

from .table import Table


class Schema(BaseModel):
    name: str
    database: str | None = None

    @property
    def fully_qualified_name(self):
        if self.database:
            return f"{self.database}.{self.name}"
        else:
            return self.name

    def get_tables(self, cursor: SnowflakeCursor):
        cursor.execute(f"show tables in schema {self.fully_qualified_name};")
        data = cursor.execute(
            'select "name", "database_name", "schema_name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));'
        ).fetchall()
        return [
            Table(name=name, schema_name=schema, database=database)
            for (name, database, schema, *_) in data
        ]
