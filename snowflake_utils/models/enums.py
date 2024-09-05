from enum import Enum


class MatchByColumnName(Enum):
    CASE_SENSITIVE = "CASE_SENSITIVE"
    CASE_INSENSITIVE = "CASE_INSENSITIVE"
    NONE = "NONE"


class TagLevel(Enum):
    COLUMN = "column"
    TABLE = "table"
