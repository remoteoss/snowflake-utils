from pydantic import BaseModel
from typing_extensions import Self


class InlineFileFormat(BaseModel):
    definition: str


class FileFormat(BaseModel):
    database: str | None = None
    schema_name: str | None = None
    name: str

    def __str__(self) -> str:
        return ".".join(
            s for s in [self.database, self.schema_name, self.name] if s is not None
        )

    @classmethod
    def from_string(cls, s: str) -> Self:
        s = s.split(".")
        match s:
            case [database, schema, name]:
                return cls(database=database, schema_name=schema, name=name)
            case [schema, name]:
                return cls(schema_name=schema, name=name)
            case [name]:
                return cls(name=name)
            case _:
                raise ValueError("Cannot parse file format")
