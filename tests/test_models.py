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
from snowflake_utils.models.column import MetadataColumn
from snowflake_utils.settings import connect

test_table_schema = TableStructure(
    columns={
        "id": Column(name="id", data_type="integer", tags={"pii": "personal"}),
        "name": Column(name="name", data_type="text"),
        "last_name": Column(name="last_name", data_type="text"),
    },
    tags={"pii": "foo"},
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
storage_integration = "DATA_PRODUCTION"
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
def test_infer_schema_with_parquet() -> None:
    """Test schema inference with Parquet file format."""
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f"USE SCHEMA {test_table.schema_};")
        # Create a table without table_structure to enable schema inference
        infer_table = Table(name="PYTEST_INFER_PARQUET", schema_="PUBLIC")

        # Create temporary stage
        infer_table.setup_stage(cursor.execute, storage_integration, path)

        # Create temporary file format for Parquet
        infer_table.setup_file_format(cursor.execute, parquet_file_format)

        # Create table with inferred schema
        statement = infer_table.get_create_table_statement(full_refresh=True)
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert result == f"Table {infer_table.name} successfully created."

        # Clean up
        infer_table.drop(cursor)


@pytest.mark.snowflake_vcr
def test_infer_schema_with_metadata() -> None:
    """Test schema inference with metadata columns."""

    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f"USE SCHEMA {test_table.schema_};")
        # Create a table with metadata columns
        infer_table = Table(
            name="PYTEST_INFER_METADATA",
            schema_="PUBLIC",
            include_metadata=[
                MetadataColumn(
                    name="file_name", data_type="string", metadata="FILE_NAME"
                ),
                MetadataColumn(
                    name="file_row_number",
                    data_type="number",
                    metadata="FILE_ROW_NUMBER",
                ),
            ],
        )

        # Create temporary stage
        infer_table.setup_stage(cursor.execute, storage_integration, path)

        # Create temporary file format for Parquet
        infer_table.setup_file_format(cursor.execute, parquet_file_format)

        # Create table with inferred schema and metadata
        statement = infer_table.get_create_table_statement(full_refresh=True)
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert result == f"Table {infer_table.name} successfully created."

        # Verify metadata columns were added
        columns = infer_table.get_columns(cursor)
        column_names = [col.name for col in columns]
        assert "file_name" in column_names
        assert "file_row_number" in column_names

        # Clean up
        infer_table.drop(cursor)


@pytest.mark.snowflake_vcr
def test_infer_schema_with_evolution() -> None:
    """Test schema inference with schema evolution enabled."""
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f"USE SCHEMA {test_table.schema_};")
        # Create a table with schema evolution enabled
        infer_table = Table(
            name="PYTEST_INFER_EVOLUTION",
            schema_="PUBLIC",
            enable_schema_evolution=True,
        )

        # Create temporary stage
        infer_table.setup_stage(cursor.execute, storage_integration, path)
        infer_table.setup_file_format(cursor.execute, parquet_file_format)

        # Create table with inferred schema and evolution enabled
        statement = infer_table.get_create_table_statement(full_refresh=True)
        result = cursor.execute(statement).fetchall()[0][0]
        logger.info(result)
        assert result == f"Table {infer_table.name} successfully created."

        # Verify schema evolution is enabled
        table_info = cursor.execute(f"SHOW TABLES LIKE '{infer_table.name}'").fetchall()
        for i, column in enumerate(cursor.description):
            if column.name == "enable_schema_evolution":
                assert table_info[0][i] == "Y"

        # Clean up
        infer_table.drop(cursor)


@pytest.mark.snowflake_vcr
def test_copy_full_refresh() -> None:
    result = test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        full_refresh=True,
    )
    assert result[0][1] == "LOADED"


@pytest.mark.snowflake_vcr
def test_copy_full_existing_table() -> None:
    result = test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
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

    with connect() as conn, conn.cursor() as cursor:
        assert dict(test_table.current_column_tags(cursor)) == {
            "id": {"pii": "personal"}
        }


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

    with connect() as conn, conn.cursor() as cursor:
        assert dict(test_table.current_column_tags(cursor)) == {
            "id": {"pii": "personal"}
        }
        assert dict(test_table.current_table_tags(cursor)) == {"pii": "foo"}

    test_table.drop()


@pytest.mark.snowflake_vcr
def test_use_existing_stage() -> None:
    """Test using an existing stage instead of a temporary stage."""
    # Create a table that uses an existing stage
    stage_table = Table(
        name="PYTEST_STAGE",
        schema_="PUBLIC",
        database="SANDBOX",
        use_temporary_stage=False,
        table_structure=test_table_schema,
    )
    stage_name = f"{stage_table.schema_}.MY_STAGE"

    # Create the stage first
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL='{path}'
            STORAGE_INTEGRATION = {storage_integration}
            """
        )

        stage_table.setup_file_format(cursor.execute, parquet_file_format)
        stage_table.setup_stage(cursor.execute, stage=stage_name)

        statement = stage_table.get_create_table_statement(full_refresh=True)
        result = cursor.execute(statement).fetchall()[0][0]
        assert result == f"Table {stage_table.name} successfully created."

    # Test copying data using the stage
    result = stage_table.copy_into(
        file_format=parquet_file_format,
        path=f"@{stage_table.schema_}.MY_STAGE",
        full_refresh=True,
    )
    assert result[0][1] == "LOADED"

    # Clean up
    with connect() as conn, conn.cursor() as cursor:
        stage_table.drop(cursor)
        cursor.execute(f"DROP STAGE {stage_table.schema_}.MY_STAGE")


@pytest.mark.snowflake_vcr
def test_copy_custom() -> None:
    with connect() as conn, conn.cursor() as cursor:
        result = test_table.copy_custom(
            column_definitions={
                "id": "$1:id",
                "name": "$1:name",
                "last_name": "$1:last_name",
            },
            path=path,
            file_format=parquet_file_format,
            storage_integration=storage_integration,
            full_refresh=True,
            sync_tags=True,
        )
        assert result[0][1] == "LOADED"
        assert dict(test_table.current_column_tags(cursor)) == {
            "id": {"pii": "personal"}
        }
        assert dict(test_table.current_table_tags(cursor)) == {"pii": "foo"}
    test_table.drop()


@pytest.mark.snowflake_vcr
def test_merge_custom() -> None:
    with connect() as conn, conn.cursor() as cursor:
        column_definitions = {
            "id": "$1:id",
            "name": "$1:name",
            "last_name": "$1:last_name",
        }
        test_table.copy_custom(
            column_definitions=column_definitions,
            path=path,
            file_format=parquet_file_format,
            storage_integration=storage_integration,
            full_refresh=True,
            sync_tags=True,
        )
        test_table.merge_custom(
            column_definitions=column_definitions,
            path=path,
            file_format=parquet_file_format,
            storage_integration=storage_integration,
            primary_keys=["id"],
        )

        assert dict(test_table.current_column_tags(cursor)) == {
            "id": {"pii": "personal"}
        }
        assert dict(test_table.current_table_tags(cursor)) == {"pii": "foo"}
    test_table.drop()
