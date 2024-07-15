import logging
import os
from datetime import datetime

import pytest

from snowflake_utils.models import (
    Column,
    InlineFileFormat,
    MatchByColumnName,
    Schema,
    Table,
    TableStructure,
)
from snowflake_utils.queries import connect

test_table_schema = TableStructure(
    columns={
        "id": Column(name="id", data_type="integer", tags={"pii": "personal"}),
        "name": Column(name="name", data_type="text"),
        "last_name": Column(name="last_name", data_type="text"),
    }
)
path = os.getenv("S3_TEST_PATH", "s3://example-bucket/example/path")
test_table = Table(name="PYTEST", schema_="PUBLIC", table_structure=test_table_schema)
test_table_json_blob = Table(
    name="PYTEST_JSON_BLOB",
    schema_="PUBLIC",
    table_structure=TableStructure(
        columns={"payload": Column(name="payload", data_type="variant")}
    ),
)
json_file_format = InlineFileFormat(definition="TYPE = JSON STRIP_OUTER_ARRAY = TRUE")
parquet_file_format = InlineFileFormat(definition="TYPE = PARQUET")
storage_integration = "DATA_STAGING"
test_schema = Schema(name="PUBLIC", database="SANDBOX")
logger = logging.getLogger(__name__)


@pytest.mark.snowflake_vcr
def test_create_schema() -> None:
    with connect() as conn, conn.cursor() as cursor:
        statement = test_table.get_create_schema_statement()
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert ("statement succeeded" in result and test_table.schema_ in result) or (
            result == f"Schema {test_table.schema_} successfully created."
        )


@pytest.mark.snowflake_vcr
def test_create_or_replace_table() -> None:
    with connect() as conn, conn.cursor() as cursor:
        statement = test_table.get_create_table_statement(full_refresh=True)
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert result == f"Table {test_table.name} successfully created."


@pytest.mark.snowflake_vcr
def test_create_table_if_not_exists() -> None:
    with connect() as conn, conn.cursor() as cursor:
        statement = test_table.get_create_table_statement()
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert ("statement succeeded" in result and test_table.name in result) or (
            f"Table {test_table.name} successfully created."
        )


@pytest.mark.snowflake_vcr
def test_temporary_external_stage_creation() -> None:
    with connect() as conn, conn.cursor() as cursor:
        temporary_external_stage = cursor.execute(
            test_table.get_create_temporary_external_stage(
                path=path, storage_integration=storage_integration
            )
        ).fetchall()[0][0]
        assert (
            temporary_external_stage
            == f"Stage area {test_table.temporary_stage} successfully created."
        )


@pytest.mark.snowflake_vcr
def test_infer_schema() -> None:
    with connect() as conn, conn.cursor() as cursor:
        temporary_external_stage = cursor.execute(
            test_table.get_create_temporary_external_stage(
                path=path, storage_integration=storage_integration
            )
        ).fetchall()[0][0]
        logger.info(temporary_external_stage)
        statement = test_table.get_create_table_statement()
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert ("statement succeeded" in result and test_table.name in result) or (
            f"Table {test_table.name} successfully created."
        )


@pytest.mark.snowflake_vcr
def test_infer_schema_full_refresh() -> None:
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f"USE SCHEMA {test_table.schema_};")
        temporary_external_stage = cursor.execute(
            test_table.get_create_temporary_external_stage(
                path=path, storage_integration=storage_integration
            )
        ).fetchall()[0][0]
        logger.info(temporary_external_stage)
        temporary_file_format = cursor.execute(
            test_table.get_create_temporary_file_format_statement(
                file_format=json_file_format.definition
            )
        ).fetchall()[0][0]
        logger.info(temporary_file_format)
        statement = test_table.get_create_table_statement(full_refresh=True)
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert result == f"Table {test_table.name} successfully created."


@pytest.mark.snowflake_vcr
def test_copy_full_refresh() -> None:
    result = test_table.copy_into(
        path=path,
        file_format=json_file_format,
        storage_integration=storage_integration,
        full_refresh=True,
    )
    assert result[0][1] == "LOADED"


@pytest.mark.snowflake_vcr
def test_copy_full_existing_table() -> None:
    result = test_table.copy_into(
        path=path,
        file_format=json_file_format,
        storage_integration=storage_integration,
        full_refresh=False,
    )
    assert result[0][0] == "Copy executed with 0 files processed."


@pytest.mark.snowflake_vcr
def test_copy_json_blob() -> None:
    result = test_table_json_blob.copy_into(
        path=path,
        file_format=json_file_format,
        storage_integration=storage_integration,
        full_refresh=False,
        match_by_column_name=MatchByColumnName.NONE,
        target_columns=["payload"],
    )
    assert result[0][1] == "LOADED"


@pytest.mark.snowflake_vcr
def test_merge() -> None:
    test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        full_refresh=False,
    )
    test_table.merge(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        primary_keys=["id"],
    )

    assert dict(test_table.current_tags()) == {"id": {"pii": "personal"}}


@pytest.mark.snowflake_vcr
def test_single_column_update() -> None:
    with connect() as conn, conn.cursor() as cursor:
        target_column = Column(name="name", data_type="text")
        new_column = Column(name="last_name", data_type="text")
        test_table.single_column_update(
            cursor=cursor, target_column=target_column, new_column=new_column
        )


@pytest.mark.snowflake_vcr
def test_schema_tables() -> None:
    with connect() as conn:
        cursor = conn.cursor()
        test_schema.get_tables(cursor=cursor)


@pytest.mark.snowflake_vcr
def test_bulk_insert() -> None:
    test_table_schema = TableStructure(
        columns={
            "id": Column(name="id", data_type="integer"),
            "name": Column(name="name", data_type="text"),
            "loaded_at": Column(name="loaded_at", data_type="timestamp"),
        }
    )
    test_table = Table(
        name="PYTEST", schema_="PUBLIC", table_structure=test_table_schema
    )
    records = {
        "1": {"id": 1, "name": "test", "loaded_at": datetime(2024, 7, 4)},
        "2": {"id": 2, "name": "test2", "loaded_at": datetime(2024, 7, 4)},
    }
    test_table.bulk_insert(records, full_refresh=True)
    with connect() as conn:
        cursor = conn.cursor()
        results = cursor.execute("select * from SANDBOX.PUBLIC.PYTEST")
        res = results.fetchall()
        assert res == [
            (1, "test", datetime(2024, 7, 4)),
            (2, "test2", datetime(2024, 7, 4)),
        ]
        test_table.drop(cursor)


@pytest.mark.snowflake_vcr
def test_copy_with_tags() -> None:
    result = test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        full_refresh=True,
        sync_tags=True,
    )

    assert result[0][1] == "LOADED"

    assert dict(test_table.current_tags()) == {"id": {"pii": "personal"}}

    test_table.drop()
