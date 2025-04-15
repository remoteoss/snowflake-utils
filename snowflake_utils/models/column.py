from datetime import date, datetime

from pydantic import BaseModel, Field


class Column(BaseModel):
    name: str
    data_type: str
    tags: dict[str, str] = Field(default_factory=dict)


def _possibly_cast(s: str, old_column_type: str, new_column_type: str) -> str:
    if old_column_type == "VARIANT" and new_column_type != "VARIANT":
        return f"PARSE_JSON({s})"
    return s


def _matched(columns: list[Column], old_columns: dict[str, str]):
    def tmp(x: str) -> str:
        return f'tmp."{x}"'

    return ",".join(
        f'dest."{c.name}" = {_possibly_cast(tmp(c.name), old_columns.get(c.name), c.data_type)}'
        for c in columns
    )


def _inserts(columns: list[Column], old_columns: dict[str, str]) -> str:
    return ",".join(
        _possibly_cast(f'tmp."{c.name}"', old_columns.get(c.name), c.data_type)
        for c in columns
    )


def _type_cast(s: any) -> any:
    if isinstance(s, (int, float)):
        return str(s)
    elif isinstance(s, str):
        return f"'{s}'"
    elif isinstance(s, (datetime, date)):
        return f"'{s.isoformat()}'"
    else:
        return f"'{s}'"


class MetadataColumn(BaseModel):
    name: str
    data_type: str
    metadata: str = Field(
        description="The metadata to be added to the column, for example FILE_ROW_NUMBER"
    )
