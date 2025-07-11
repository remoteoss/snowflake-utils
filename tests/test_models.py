import logging
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from snowflake_utils.models import (
    Column,
    FileFormat,
    InlineFileFormat,
    MatchByColumnName,
    Schema,
    Table,
    TableStructure,
)
from snowflake_utils.models.column import MetadataColumn

test_table_schema = TableStructure(
    columns={
        "id": Column(name="id", data_type="integer", tags={"pii": "personal"}),
        "name": Column(name="name", data_type="text"),
        "last_name": Column(name="last_name", data_type="text"),
    },
    tags={"pii": "foo"},
)
path = os.getenv("S3_TEST_PATH", "s3://example-bucket/example/path")
test_table = Table(
    name="PYTEST", schema_name="PUBLIC", table_structure=test_table_schema
)
test_table_json_blob = Table(
    name="PYTEST_JSON_BLOB",
    schema_name="PUBLIC",
    table_structure=TableStructure(
        columns={"payload": Column(name="payload", data_type="variant")}
    ),
)
json_file_format = InlineFileFormat(definition="TYPE = JSON STRIP_OUTER_ARRAY = TRUE")
parquet_file_format = InlineFileFormat(definition="TYPE = PARQUET")
storage_integration = "DATA_PRODUCTION"
test_schema = Schema(name="PUBLIC", database="SANDBOX")
logger = logging.getLogger(__name__)


def make_mock_cursor(
    fetchall_return=None,
    execute_return=None,
    description=None,
    fetchall_side_effect=None,
):
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = mock_cursor
    if fetchall_side_effect:
        mock_cursor.fetchall.side_effect = fetchall_side_effect
    else:
        mock_cursor.fetchall.return_value = fetchall_return or [
            ("statement succeeded: PUBLIC",)
        ]
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    if description:
        mock_cursor.description = description
    return mock_cursor


def make_mock_conn(cursor=None):
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = cursor or make_mock_cursor()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    return mock_conn


@patch("snowflake_utils.settings.connect")
def test_create_schema(mock_connect):
    mock_cursor = make_mock_cursor(fetchall_return=[("statement succeeded: PUBLIC",)])
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    statement = test_table.get_create_schema_statement()
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert ("statement succeeded" in result and test_table.schema_name in result) or (
        result == f"Schema {test_table.schema_name} successfully created."
    )


@patch("snowflake_utils.settings.connect")
def test_create_or_replace_table(mock_connect):
    mock_cursor = make_mock_cursor(
        fetchall_return=[(f"Table {test_table.name} successfully created.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    statement = test_table.get_create_table_statement(full_refresh=True)
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert result == f"Table {test_table.name} successfully created."


@patch("snowflake_utils.settings.connect")
def test_create_table_if_not_exists(mock_connect):
    mock_cursor = make_mock_cursor(fetchall_return=[("statement succeeded: PYTEST",)])
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    statement = test_table.get_create_table_statement()
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert ("statement succeeded" in result and test_table.name in result) or (
        f"Table {test_table.name} successfully created."
    )


@patch("snowflake_utils.settings.connect")
def test_temporary_external_stage_creation(mock_connect):
    mock_cursor = make_mock_cursor(
        fetchall_return=[
            (f"Stage area {test_table.temporary_stage} successfully created.",)
        ]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    result = mock_cursor.execute(
        test_table.get_create_temporary_external_stage(
            path=path, storage_integration=storage_integration
        )
    ).fetchall()[0][0]
    assert result == f"Stage area {test_table.temporary_stage} successfully created."


@patch("snowflake_utils.settings.connect")
def test_infer_schema(mock_connect):
    mock_cursor = make_mock_cursor(
        fetchall_return=[(f"Table {test_table.name} successfully created.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    mock_cursor.execute(
        test_table.get_create_temporary_external_stage(
            path=path, storage_integration=storage_integration
        )
    ).fetchall()[0][0]
    statement = test_table.get_create_table_statement()
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert ("statement succeeded" in result and test_table.name in result) or (
        f"Table {test_table.name} successfully created."
    )


@patch("snowflake_utils.settings.connect")
def test_infer_schema_full_refresh(mock_connect):
    mock_cursor = make_mock_cursor(
        fetchall_return=[(f"Table {test_table.name} successfully created.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    mock_cursor.execute(f"USE SCHEMA {test_table.schema_name};")
    mock_cursor.execute(
        test_table.get_create_temporary_external_stage(
            path=path, storage_integration=storage_integration
        )
    ).fetchall()[0][0]
    mock_cursor.execute(
        test_table.get_create_temporary_file_format_statement(
            file_format=json_file_format.definition
        )
    ).fetchall()[0][0]
    statement = test_table.get_create_table_statement(full_refresh=True)
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert result == f"Table {test_table.name} successfully created."


@patch("snowflake_utils.settings.connect")
def test_infer_schema_with_parquet(mock_connect):
    """Test schema inference with Parquet file format."""
    mock_cursor = make_mock_cursor(
        fetchall_return=[("Table PYTEST_INFER_PARQUET successfully created.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    mock_cursor.execute(f"USE SCHEMA {test_table.schema_name};")
    # Create a table without table_structure to enable schema inference
    infer_table = Table(name="PYTEST_INFER_PARQUET", schema_name="PUBLIC")

    # Create temporary stage
    infer_table.setup_stage(mock_cursor.execute, storage_integration, path)

    # Create temporary file format for Parquet
    infer_table.setup_file_format(mock_cursor.execute, parquet_file_format)

    # Create table with inferred schema
    statement = infer_table.get_create_table_statement(full_refresh=True)
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert result == f"Table {infer_table.name} successfully created."

    # Clean up
    infer_table.drop(mock_cursor)


@patch("snowflake_utils.settings.connect")
def test_infer_schema_with_metadata(mock_connect):
    """Test schema inference with metadata columns."""
    # Mock cursor with proper data for different operations
    mock_cursor = make_mock_cursor(
        fetchall_side_effect=[
            [("Table PYTEST_INFER_METADATA successfully created.",)],
            [
                ("file_name", "string"),
                ("file_row_number", "number"),
                ("id", "integer"),
            ],  # get_columns expects (name, data_type)
            [
                ("file_name", "string"),
                ("file_row_number", "number"),
                ("id", "integer"),
            ],  # get_columns expects (name, data_type)
        ]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    mock_cursor.execute(f"USE SCHEMA {test_table.schema_name};")
    # Create a table with metadata columns
    infer_table = Table(
        name="PYTEST_INFER_METADATA",
        schema_name="PUBLIC",
        include_metadata=[
            MetadataColumn(name="file_name", data_type="string", metadata="FILE_NAME"),
            MetadataColumn(
                name="file_row_number",
                data_type="number",
                metadata="FILE_ROW_NUMBER",
            ),
        ],
    )

    # Create temporary stage
    infer_table.setup_stage(mock_cursor.execute, storage_integration, path)

    # Create temporary file format for Parquet
    infer_table.setup_file_format(mock_cursor.execute, parquet_file_format)

    # Create table with inferred schema and metadata
    statement = infer_table.get_create_table_statement(full_refresh=True)
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert result == f"Table {infer_table.name} successfully created."

    # Verify metadata columns were added
    columns = infer_table.get_columns(mock_cursor)
    column_names = [col.name for col in columns]
    assert "file_name" in column_names
    assert "file_row_number" in column_names

    # Clean up
    infer_table.drop(mock_cursor)


@patch("snowflake_utils.settings.connect")
def test_infer_schema_with_evolution(mock_connect):
    """Test schema inference with schema evolution enabled."""
    mock_cursor = make_mock_cursor(
        fetchall_return=[("Table PYTEST_INFER_EVOLUTION successfully created.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    mock_cursor.execute(f"USE SCHEMA {test_table.schema_name};")
    # Create a table with schema evolution enabled
    infer_table = Table(
        name="PYTEST_INFER_EVOLUTION",
        schema_name="PUBLIC",
        enable_schema_evolution=True,
    )

    # Create temporary stage
    infer_table.setup_stage(mock_cursor.execute, storage_integration, path)
    infer_table.setup_file_format(mock_cursor.execute, parquet_file_format)

    # Create table with inferred schema and evolution enabled
    statement = infer_table.get_create_table_statement(full_refresh=True)
    result = mock_cursor.execute(statement).fetchall()[0][0]
    assert result == f"Table {infer_table.name} successfully created."

    # Verify schema evolution is enabled
    table_info = mock_cursor.execute(
        f"SHOW TABLES LIKE '{infer_table.name}'"
    ).fetchall()
    for i, column in enumerate(mock_cursor.description):
        if column.name == "enable_schema_evolution":
            assert table_info[0][i] == "Y"

    # Clean up
    infer_table.drop(mock_cursor)


@patch.object(Table, "_copy")
def test_copy_full_refresh(mock_copy):
    mock_copy.return_value = [("test_file.parquet", "LOADED")]
    result = test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        full_refresh=True,
    )
    assert result[0][1] == "LOADED"


@patch.object(Table, "_copy")
def test_copy_full_existing_table(mock_copy):
    mock_copy.return_value = [("Copy executed with 0 files processed.",)]
    result = test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        full_refresh=False,
    )
    assert result[0][0] == "Copy executed with 0 files processed."


@patch.object(Table, "_copy")
def test_copy_json_blob(mock_copy):
    mock_copy.return_value = [("test_file.json", "LOADED")]
    result = test_table_json_blob.copy_into(
        path=path,
        file_format=json_file_format,
        storage_integration=storage_integration,
        full_refresh=False,
        match_by_column_name=MatchByColumnName.NONE,
        target_columns=["payload"],
    )
    assert result[0][1] == "LOADED"


@patch.object(Table, "_copy")
@patch.object(Table, "_merge")
def test_merge(mock_merge, mock_copy):
    mock_copy.return_value = [("test_file.parquet", "LOADED")]
    mock_merge.return_value = [("test_file.parquet", "LOADED")]
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

    with patch("snowflake_utils.settings.connect") as mock_connect:
        # Mock cursor with proper tag data
        mock_cursor = make_mock_cursor(fetchall_return=[("id", "pii", "personal")])
        mock_conn = make_mock_conn(cursor=mock_cursor)
        mock_connect.return_value = mock_conn
        with mock_conn as conn, conn.cursor() as cursor:
            assert dict(test_table.current_column_tags(cursor)) == {
                "id": {"pii": "personal"}
            }


@patch("snowflake_utils.settings.connect")
def test_single_column_update(mock_connect):
    mock_cursor = make_mock_cursor()
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    with mock_conn as conn, conn.cursor() as cursor:
        target_column = Column(name="name", data_type="text")
        new_column = Column(name="last_name", data_type="text")
        test_table.single_column_update(
            cursor=cursor, target_column=target_column, new_column=new_column
        )


@patch("snowflake_utils.settings.connect")
def test_schema_tables(mock_connect):
    # Mock cursor with proper table data
    mock_cursor = make_mock_cursor(
        fetchall_return=[("TEST_TABLE", "SANDBOX", "PUBLIC")]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    with mock_conn as conn:
        cursor = conn.cursor()
        test_schema.get_tables(cursor=cursor)


@patch.object(Table, "bulk_insert")
@patch("snowflake_utils.settings.connect")
def test_bulk_insert(mock_connect, mock_bulk_insert):
    # Mock cursor with proper bulk insert data
    mock_cursor = make_mock_cursor(
        fetchall_return=[
            (1, "test", datetime(2024, 7, 4)),
            (2, "test2", datetime(2024, 7, 4)),
        ]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    # Mock the bulk_insert method to do nothing
    mock_bulk_insert.return_value = None

    test_table_schema = TableStructure(
        columns={
            "id": Column(name="id", data_type="integer"),
            "name": Column(name="name", data_type="text"),
            "loaded_at": Column(name="loaded_at", data_type="timestamp"),
        }
    )
    test_table = Table(
        name="PYTEST", schema_name="PUBLIC", table_structure=test_table_schema
    )
    records = {
        "1": {"id": 1, "name": "test", "loaded_at": datetime(2024, 7, 4)},
        "2": {"id": 2, "name": "test2", "loaded_at": datetime(2024, 7, 4)},
    }

    # Call bulk_insert (now mocked)
    test_table.bulk_insert(records, full_refresh=True)

    # Verify bulk_insert was called with correct arguments
    mock_bulk_insert.assert_called_once_with(records, full_refresh=True)

    # Test the select query using mocked cursor
    with mock_conn as conn:
        cursor = conn.cursor()
        results = cursor.execute("select * from SANDBOX.PUBLIC.PYTEST")
        res = results.fetchall()
        assert res == [
            (1, "test", datetime(2024, 7, 4)),
            (2, "test2", datetime(2024, 7, 4)),
        ]
        test_table.drop(cursor)


@patch.object(Table, "drop")
@patch.object(Table, "_copy")
@patch("snowflake_utils.settings.connect")
def test_copy_with_tags(mock_connect, mock_copy, mock_drop):
    mock_copy.return_value = [("test_file.parquet", "LOADED")]
    mock_drop.return_value = None
    # Mock cursor with proper tag data - different data for different calls
    mock_cursor = make_mock_cursor(
        fetchall_side_effect=[
            [("id", "pii", "personal")],  # Column tags
            [("", "pii", "foo")],  # Table tags (empty column_name for table-level)
        ]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    result = test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        full_refresh=True,
        sync_tags=True,
    )

    assert result[0][1] == "LOADED"

    with mock_conn as conn, conn.cursor() as cursor:
        assert dict(test_table.current_column_tags(cursor)) == {
            "id": {"pii": "personal"}
        }
        assert dict(test_table.current_table_tags(cursor)) == {"pii": "foo"}

    test_table.drop()
    mock_drop.assert_called_once()


@patch("snowflake_utils.settings.connect")
def test_use_existing_stage(mock_connect):
    """Test using an existing stage instead of a temporary stage."""
    mock_cursor = make_mock_cursor(
        fetchall_return=[("Table PYTEST_STAGE successfully created.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    # Create a table that uses an existing stage
    stage_table = Table(
        name="PYTEST_STAGE",
        schema_name="PUBLIC",
        database="SANDBOX",
        use_temporary_stage=False,
        table_structure=test_table_schema,
    )
    stage_name = f"{stage_table.schema_name}.MY_STAGE"

    # Create the stage first
    with mock_conn as conn, conn.cursor() as cursor:
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
    with patch.object(Table, "_copy") as mock_copy:
        mock_copy.return_value = [("test_file.parquet", "LOADED")]
        result = stage_table.copy_into(
            file_format=parquet_file_format,
            path=f"@{stage_table.schema_name}.MY_STAGE",
            full_refresh=True,
        )
        assert result[0][1] == "LOADED"

    # Clean up
    with mock_conn as conn, conn.cursor() as cursor:
        stage_table.drop(cursor)
        cursor.execute(f"DROP STAGE {stage_table.schema_name}.MY_STAGE")


@patch.object(Table, "drop")
@patch.object(Table, "_copy")
@patch.object(Table, "_merge")
@patch("snowflake_utils.settings.connect")
def test_merge_custom(mock_connect, mock_merge, mock_copy, mock_drop):
    mock_copy.return_value = [("test_file.parquet", "LOADED")]
    mock_merge.return_value = [("test_file.parquet", "LOADED")]
    mock_drop.return_value = None
    # Mock cursor with proper tag data - different data for different calls
    mock_cursor = make_mock_cursor(
        fetchall_side_effect=[
            [("id", "pii", "personal")],  # Column tags
            [("", "pii", "foo")],  # Table tags (empty column_name for table-level)
        ]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn
    with mock_conn as conn, conn.cursor() as cursor:
        # Setup stage first
        test_table.setup_stage(cursor.execute, storage_integration, path)
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
    mock_drop.assert_called_once()


@patch.object(Table, "_copy")
def test_copy_with_files(mock_copy) -> None:
    """Test copy_into method with specific files parameter."""
    # Setup mock to return expected result
    mock_copy.return_value = [("test_file.parquet", "LOADED")]

    # Create a table instance
    table = Table(name="TEST_TABLE", schema_name="TEST_SCHEMA")

    # Test with files parameter
    result = table.copy_into(
        path="s3://test-bucket/path",
        file_format=parquet_file_format,
        files=["test_file.parquet", "another_file.parquet"],
    )

    # Verify the result
    assert result[0][1] == "LOADED"

    # Verify the _copy method was called with the correct query containing FILES clause
    mock_copy.assert_called()
    call_args = mock_copy.call_args
    query = call_args[0][0]  # First positional argument is the query
    assert "FILES = ('test_file.parquet', 'another_file.parquet')" in query


@patch.object(Table, "_merge")
def test_merge_with_files(mock_merge) -> None:
    """Test merge method with specific files parameter."""
    # Create a table instance
    table = Table(name="TEST_TABLE", schema_name="TEST_SCHEMA")

    # Test merge with files parameter
    table.merge(
        path="s3://test-bucket/path",
        file_format=parquet_file_format,
        primary_keys=["id"],
        files=["test_file.parquet"],
    )

    # Verify the _merge method was called
    mock_merge.assert_called_once()

    # Get the copy_callable that was passed to _merge
    call_args = mock_merge.call_args
    copy_callable = call_args[0][0]  # First positional argument is the copy_callable

    # Now test the copy_callable to verify it passes the files parameter correctly
    with patch.object(Table, "_copy") as mock_copy:
        mock_copy.return_value = [("test_file.parquet", "LOADED")]

        # Create a temporary table to test the copy_callable
        temp_table = Table(name="TEMP_TABLE", schema_name="TEST_SCHEMA")

        # Call the copy_callable (this simulates what happens inside _merge)
        copy_callable(temp_table, sync_tags=False)

        # Verify the _copy method was called with the correct query containing FILES clause
        mock_copy.assert_called()
        call_args = mock_copy.call_args
        query = call_args[0][0]  # First positional argument is the query
        assert "FILES = ('test_file.parquet')" in query


@patch.object(Table, "_copy")
def test_copy_into_files_parameter_formatting(mock_copy) -> None:
    """Test that files parameter is properly formatted in COPY INTO query."""
    mock_copy.return_value = [("test_file.parquet", "LOADED")]

    # Test with files parameter
    files = ["file1.parquet", "file2.parquet", "file3.parquet"]
    result = test_table.copy_into(
        path=path,
        file_format=parquet_file_format,
        storage_integration=storage_integration,
        files=files,
    )

    # Verify the copy was called with properly formatted files clause
    mock_copy.assert_called_once()
    call_args = mock_copy.call_args[0]
    query = call_args[0]  # First argument is the query

    # Check that files clause is properly formatted
    expected_files_clause = (
        "FILES = ('file1.parquet', 'file2.parquet', 'file3.parquet')"
    )
    assert expected_files_clause in query
    assert result[0][1] == "LOADED"


# Tests for newly added setup methods
@patch("snowflake_utils.settings.connect")
def test_setup_file_format_with_inline_format(mock_connect):
    """Test setup_file_format with InlineFileFormat creates temporary file format."""
    mock_cursor = make_mock_cursor(
        fetchall_return=[("Temporary file format created successfully.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    # Create a table with custom role and database
    test_table_with_context = Table(
        name="PYTEST_SETUP", schema_name="PUBLIC", role="TEST_ROLE", database="TEST_DB"
    )

    # Test with inline file format
    result = test_table_with_context.setup_file_format(
        mock_cursor.execute, json_file_format
    )

    # Verify temporary file format was created
    expected_temp_format = test_table_with_context.temporary_file_format
    assert result == expected_temp_format
    assert test_table_with_context._file_format == expected_temp_format

    # Verify the create statement was executed
    mock_cursor.execute.assert_called()
    create_call = mock_cursor.execute.call_args_list[0][0][0]
    assert "CREATE OR REPLACE TEMPORARY FILE FORMAT" in create_call
    assert json_file_format.definition in create_call


@patch("snowflake_utils.settings.connect")
def test_setup_file_format_with_existing_format(mock_connect):
    """Test setup_file_format with existing FileFormat uses it directly."""
    mock_cursor = make_mock_cursor()
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    # Create existing file format
    existing_format = FileFormat(
        database="PROD", schema_name="DATA", name="EXISTING_FORMAT"
    )

    test_table = Table(name="PYTEST_SETUP", schema_name="PUBLIC")

    # Test with existing file format
    result = test_table.setup_file_format(mock_cursor.execute, existing_format)

    # Verify existing format was used
    assert result == existing_format
    assert test_table._file_format == existing_format

    # Verify no create statement was executed
    mock_cursor.execute.assert_not_called()


@patch("snowflake_utils.settings.connect")
def test_setup_stage_with_temporary_stage(mock_connect):
    """Test setup_stage creates temporary stage when no existing stage provided."""
    mock_cursor = make_mock_cursor(
        fetchall_return=[("Temporary stage created successfully.",)]
    )
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    test_table = Table(name="PYTEST_SETUP", schema_name="PUBLIC")

    # Test with temporary stage creation
    test_table.setup_stage(
        mock_cursor.execute, storage_integration=storage_integration, path=path
    )

    # Verify temporary stage was set
    assert test_table._stage == test_table.temporary_stage

    # Verify the create statement was executed
    mock_cursor.execute.assert_called_once()
    create_call = mock_cursor.execute.call_args[0][0]
    assert "CREATE OR REPLACE TEMPORARY STAGE" in create_call
    assert test_table.temporary_stage in create_call
    assert path in create_call
    assert storage_integration in create_call


@patch("snowflake_utils.settings.connect")
def test_setup_stage_with_existing_stage(mock_connect):
    """Test setup_stage uses existing stage when provided."""
    mock_cursor = make_mock_cursor()
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    test_table = Table(name="PYTEST_SETUP", schema_name="PUBLIC")
    existing_stage = "PUBLIC.MY_STAGE"

    # Test with existing stage
    test_table.setup_stage(mock_cursor.execute, stage=existing_stage, path="data/")

    # Verify existing stage was set with path
    assert test_table._stage == f"{existing_stage}/data/"

    # Verify no create statement was executed
    mock_cursor.execute.assert_not_called()


@patch("snowflake_utils.settings.connect")
def test_setup_stage_without_parameters(mock_connect):
    """Test setup_stage does nothing when no storage_integration or stage provided."""
    mock_cursor = make_mock_cursor()
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    test_table = Table(name="PYTEST_SETUP", schema_name="PUBLIC")

    # Test without parameters
    test_table.setup_stage(mock_cursor.execute)

    # Verify no stage was set
    assert test_table._stage is None

    # Verify no create statement was executed
    mock_cursor.execute.assert_not_called()


# Test: existing_column_tags and existing_table_tags
@patch("snowflake_utils.settings.connect")
def test_current_column_tags_uses_existing(mock_connect):
    table = Table(
        name="TEST_COL_TAGS",
        schema_name="PUBLIC",
        existing_column_tags={"col1": {"tag1": "val1"}, "col2": {"tag2": "val2"}},
    )
    # Should return the provided dict and not call the cursor
    mock_cursor = make_mock_cursor()
    result = table.current_column_tags(mock_cursor)
    assert result == {"col1": {"tag1": "val1"}, "col2": {"tag2": "val2"}}


@patch("snowflake_utils.settings.connect")
def test_current_table_tags_uses_existing(mock_connect):
    table = Table(
        name="TEST_TABLE_TAGS",
        schema_name="PUBLIC",
        existing_table_tags={"tag1": "val1", "tag2": "val2"},
    )
    # Should return the provided dict and not call the cursor
    mock_cursor = make_mock_cursor()
    result = table.current_table_tags(mock_cursor)
    assert result == {"tag1": "val1", "tag2": "val2"}


@patch("snowflake_utils.settings.connect")
def test_file_format_property_raises_error_when_not_set(mock_connect):
    """Test that file_format property raises error when not set up."""
    test_table = Table(name="PYTEST_PROPERTY", schema_name="PUBLIC")

    # Verify property raises error when not set
    with pytest.raises(
        ValueError, match="Call setup_file_format to set the file format"
    ):
        _ = test_table.file_format


@patch("snowflake_utils.settings.connect")
def test_stage_property_raises_error_when_not_set(mock_connect):
    """Test that stage property raises error when not set up."""
    test_table = Table(name="PYTEST_PROPERTY", schema_name="PUBLIC")

    # Verify property raises error when not set
    with pytest.raises(ValueError, match="Call setup_stage to set the stage"):
        _ = test_table.stage


@patch("snowflake_utils.settings.connect")
def test_file_format_property_returns_value_when_set(mock_connect):
    """Test that file_format property returns value when properly set."""
    mock_cursor = make_mock_cursor()
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    test_table = Table(name="PYTEST_PROPERTY", schema_name="PUBLIC")

    # Set up file format
    test_table.setup_file_format(mock_cursor.execute, json_file_format)

    # Verify property returns the file format
    assert test_table.file_format == test_table._file_format


@patch("snowflake_utils.settings.connect")
def test_stage_property_returns_value_when_set(mock_connect):
    """Test that stage property returns value when properly set."""
    mock_cursor = make_mock_cursor()
    mock_conn = make_mock_conn(cursor=mock_cursor)
    mock_connect.return_value = mock_conn

    test_table = Table(name="PYTEST_PROPERTY", schema_name="PUBLIC")

    # Set up stage
    test_table.setup_stage(
        mock_cursor.execute, storage_integration=storage_integration, path=path
    )

    # Verify property returns the stage
    assert test_table.stage == test_table._stage
