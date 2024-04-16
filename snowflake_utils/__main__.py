import typer
from typing_extensions import Annotated

from .models import FileFormat, InlineFileFormat, Table

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


if __name__ == "__main__":
    app()
