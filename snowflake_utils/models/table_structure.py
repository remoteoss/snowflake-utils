from pydantic import BaseModel, Field, field_validator

from .column import Column


class TableStructure(BaseModel):
    columns: dict = [str, Column]
    tags: dict[str, str] = Field(default_factory=dict)

    @property
    def parsed_columns(self, replace_chars: bool = False) -> str:
        if replace_chars:
            return ", ".join(
                f'"{str.upper(k).strip().replace("-", "_")}" {v.data_type}'
                for k, v in self.columns.items()
            )
        else:
            return ", ".join(
                f'"{str.upper(k).strip()}" {v.data_type}'
                for k, v in self.columns.items()
            )

    def parse_from_json(self):
        raise NotImplementedError("Not implemented yet")

    @field_validator("columns")
    @classmethod
    def force_columns_to_casefold(cls, value) -> dict:
        return {k.casefold(): v for k, v in value.items()}
