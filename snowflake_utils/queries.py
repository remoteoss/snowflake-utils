import logging
from typing import no_type_check

from snowflake import connector


@no_type_check
def execute_statement(
    cursor: connector.cursor.SnowflakeCursor, statement: str
) -> list[tuple] | list[dict] | None:
    logging.debug("Statement to execute: ")
    logging.debug(statement)
    result = cursor.execute(statement).fetchall()
    logging.debug("Statement executed.")
    return result
