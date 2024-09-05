from pydantic import BaseModel
from typing_extensions import Self


class InlineFileFormat(BaseModel):
    definition: str


class FileFormat(BaseModel):
    database: str | None = None
    schema_: str | None = None
    name: str

    def __str__(self) -> str:
        return ".".join(
            s for s in [self.database, self.schema_, self.name] if s is not None
        )

    @classmethod
    def from_string(cls, s: str) -> Self:
        s = s.split(".")
        match s:
            case [database, schema, name]:
                return cls(database=database, schema_=schema, name=name)
            case [schema, name]:
                return cls(schema_=schema, name=name)
            case [name]:
                return cls(name=name)
            case _:
                raise ValueError("Cannot parse file format")
