import typer
from typing_extensions import Annotated

from .models import FileFormat, InlineFileFormat, Table, Schema, Column
from .queries import connect
import logging
import os

app = typer.Typer()


@app.command()
def copy(
    schema: Annotated[str, typer.Argument()],
    name: Annotated[str, typer.Argument()],
    path: Annotated[str, typer.Argument()],
    file_format: Annotated[str, typer.Argument()],
    storage_integration: Annotated[str, typer.Argument()],
) -> None:
    ff = (
        InlineFileFormat(definition=file_format)
        if "=" in file_format
        else FileFormat.from_string(file_format)
    )
    table = Table(name=name, schema_=schema, infer_schema=True)
    table.copy_into(
        file_format=ff,
        path=path,
        storage_integration=storage_integration,
    )


@app.command()
def mass_single_column_update(
    schema: Annotated[str, typer.Argument()],
    target_column: Annotated[str, typer.Argument()],
    new_column: Annotated[str, typer.Argument()],
    data_type: Annotated[str, typer.Argument()],
) -> None:
    db_schema = Schema(name=schema)
    target_column = Column(name=target_column, data_type=data_type)
    new_column = Column(name=new_column, data_type=data_type)
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.getLogger("snowflake-utils").setLevel(log_level)
    with connect() as conn, conn.cursor() as cursor:
        tables = db_schema.get_tables(cursor=cursor)
        for table in tables:
            columns = table.get_columns(cursor=cursor)
            column_names = [str.upper(column.name) for column in columns]
            if (
                str.upper(target_column.name) in column_names
                and str.upper(new_column.name) in column_names
            ):
                table.single_column_update(
                    cursor=cursor, target_column=target_column, new_column=new_column
                )
            else:
                logging.debug("One or both of the columns don't exist in the table")


if __name__ == "__main__":
    app()
