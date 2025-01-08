import logging
from collections import defaultdict
from functools import partial

from pydantic import BaseModel, Field
from snowflake.connector.cursor import SnowflakeCursor

from ..queries import execute_statement
from ..settings import connect, governance_settings
from .column import Column, _inserts, _matched, _type_cast
from .enums import MatchByColumnName, TagLevel
from .file_format import FileFormat, InlineFileFormat
from .table_structure import TableStructure


class Table(BaseModel):
    name: str
    schema_: str
    table_structure: TableStructure | None = None
    role: str | None = None
    database: str | None = None
    include_metadata: dict[str, str] = Field(default_factory=dict)

    def _include_metadata(self) -> str:
        if not self.include_metadata:
            return ""
        else:
            metadata = ", ".join(
                f"{k}=METADATA${v}" for k, v in self.include_metadata.items()
            )
            return f"INCLUDE_METADATA = ({metadata})"

    @property
    def fqn(self) -> str:
        if database := self.database:
            return f"{database}.{self.schema_}.{self.name}"
        return f"{self.schema_}.{self.name}"

    @property
    def temporary_stage(self) -> str:
        return f"tmp_external_stage_{self.schema_}_{self.name}".upper()

    @property
    def temporary_file_format(self) -> FileFormat:
        return FileFormat(
            schema=self.schema_,
            name=f"tmp_file_format_{self.schema_}_{self.name}".upper(),
        )

    def get_create_temporary_file_format_statement(self, file_format: str) -> str:
        """
        Creates a temporary file format with the inline arguments
        """
        file_format_statement = f"""
        CREATE OR REPLACE TEMPORARY FILE FORMAT {self.schema_}.{self.temporary_file_format}
        {file_format}
        """
        return file_format_statement

    def get_create_schema_statement(self) -> str:
        """ """
        logging.debug(f"Creating schema if it not exsists: {self.schema_}")
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_}"

    def get_create_temporary_external_stage(
        self, path: str, storage_integration: str
    ) -> str:
        logging.debug(f"Creating temporate stage at path: {path}")
        return f"""
        CREATE OR REPLACE TEMPORARY STAGE {self.schema_}.{self.temporary_stage}
        URL='{path}'
        STORAGE_INTEGRATION = {storage_integration}    
        """

    def get_create_table_statement(
        self,
        full_refresh: bool = False,
    ) -> str:
        logging.debug(f"Creating table: {self.fqn}")
        if self.table_structure:
            return f"{'CREATE OR REPLACE TABLE' if full_refresh else 'CREATE TABLE IF NOT EXISTS'} {self.fqn} ({self.table_structure.parsed_columns})"
        else:
            return f"""
            {'CREATE OR REPLACE TABLE' if full_refresh else 'CREATE TABLE IF NOT EXISTS'} {self.fqn}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                    LOCATION=>'@{self.temporary_stage}',
                    FILE_FORMAT=>'{self.temporary_file_format}',
                    IGNORE_CASE => TRUE
                    )
                ));
            
            """

    def bulk_insert(
        self,
        records,
        full_refresh: bool = False,
    ) -> None:
        with connect() as connection:
            cursor = connection.cursor()
            _execute_statement = partial(execute_statement, cursor)
            _execute_statement(self.get_create_schema_statement())
            _execute_statement(self.get_create_table_statement(full_refresh))
            for k in records:
                cols = ", ".join([k for k in records[k].keys()])
                vals = ", ".join([_type_cast(v) for v in records[k].values()])
                _execute_statement(
                    f"""
                    INSERT INTO {self.fqn}({cols})
                    VALUES ({vals})
                    """
                )
        return None

    def _copy(
        self,
        query: str,
        path: str,
        file_format: InlineFileFormat | FileFormat,
        storage_integration: str | None = None,
        full_refresh: bool = False,
        sync_tags: bool = False,
    ) -> None:
        with connect() as connection:
            cursor = connection.cursor()
            execute = self.setup_connection(path, storage_integration, cursor)
            file_format = self.setup_file_format(file_format, execute)
            self.create_table(full_refresh, execute)

            if sync_tags and self.table_structure:
                self.sync_tags(cursor)
            logging.info(f"Starting copy into `{self.fqn}` from path '{path}'")
            return execute(query.format(file_format=file_format))

    def copy_into(
        self,
        path: str,
        file_format: InlineFileFormat | FileFormat,
        storage_integration: str | None = None,
        match_by_column_name: MatchByColumnName = MatchByColumnName.CASE_INSENSITIVE,
        full_refresh: bool = False,
        target_columns: list[str] | None = None,
        sync_tags: bool = False,
        primary_keys: list[str] = ["id"],
        replication_keys: list[str] | None = None,
        qualify: bool = False,
    ) -> None:
        col_str = f"({', '.join(target_columns)})" if target_columns else ""
        copy_query = f"""
                COPY INTO {self.fqn} {col_str}
                FROM {path}
                {f"STORAGE_INTEGRATION = {storage_integration}" if storage_integration else ''}
                FILE_FORMAT = ( FORMAT_NAME ='{{file_format}}')
                MATCH_BY_COLUMN_NAME={match_by_column_name.value}
                {self._include_metadata()}
                """
        if qualify:
            self._copy(
                copy_query,
                path,
                file_format,
                storage_integration,
                full_refresh,
                sync_tags,
            )
            with connect() as connection:
                cursor = connection.cursor()
                self.qualify(
                    cursor=cursor,
                    primary_keys=primary_keys,
                    replication_keys=replication_keys,
                )
                if sync_tags and self.table_structure:
                    self.sync_tags(cursor)
        else:
            return self._copy(
                copy_query,
                path,
                file_format,
                storage_integration,
                full_refresh,
                sync_tags,
            )

    def create_table(self, full_refresh: bool, execute_statement: callable) -> None:
        execute_statement(self.get_create_table_statement(full_refresh))

    def setup_file_format(
        self, file_format: FileFormat | InlineFileFormat, execute_statement: callable
    ) -> FileFormat:
        if isinstance(file_format, InlineFileFormat):
            execute_statement(
                self.get_create_temporary_file_format_statement(
                    file_format=file_format.definition
                )
            )
            file_format = self.temporary_file_format
        return file_format

    def get_columns(self, cursor: SnowflakeCursor) -> list[Column]:
        data = cursor.execute(f"desc table {self.fqn}").fetchall()
        return [
            Column(name=name, data_type=data_type) for (name, data_type, *_) in data
        ]

    def add_column(self, cursor: SnowflakeCursor, column: Column) -> None:
        cursor.execute(
            f"alter table {self.fqn} add column {column.name} {column.data_type}"
        )

    def exists(self, cursor: SnowflakeCursor) -> bool:
        return bool(
            cursor.execute(
                f"select table_name from information_schema.tables where table_name ilike '{self.name}' and table_schema = '{self.schema_}'"
            ).fetchall()
        )

    def _merge(
        self,
        copy_callable: callable,
        primary_keys: list[str] = ["id"],
        replication_keys: list[str] | None = None,
        qualify: bool = False,
    ) -> None:
        with connect() as connection:
            cursor = connection.cursor()
            if not self.exists(cursor):
                copy_callable(self, sync_tags=True)
                if qualify:
                    self.qualify(cursor, primary_keys, replication_keys)
                return None

        temp_table = self.model_copy(update={"name": f"{self.name}_temp"})
        copy_callable(temp_table, sync_tags=False)
        if qualify:
            with connect() as connection:
                cursor = connection.cursor()
                temp_table.qualify(cursor, primary_keys, replication_keys)

        with connect() as connection:
            cursor = connection.cursor()
            cursor.execute(self.get_create_table_statement(full_refresh=False))
            old_columns = {x.name: x.data_type for x in self.get_columns(cursor)}
            new_columns = temp_table.get_columns(cursor)

            for column in new_columns:
                if column.name not in old_columns:
                    self.add_column(cursor, column)

            cursor.execute(
                self._merge_statement(
                    temp_table, new_columns, old_columns, primary_keys
                )
            )
            if self.table_structure:
                self.sync_tags(cursor)
            temp_table.drop(cursor)

    def merge(
        self,
        path: str,
        file_format: InlineFileFormat | FileFormat,
        primary_keys: list[str] = ["id"],
        replication_keys: list[str] | None = None,
        storage_integration: str | None = None,
        match_by_column_name: MatchByColumnName = MatchByColumnName.CASE_INSENSITIVE,
        qualify: bool = False,
    ) -> None:
        def copy_callable(table: Table, sync_tags: bool) -> None:
            return table.copy_into(
                path=path,
                storage_integration=storage_integration,
                file_format=file_format,
                match_by_column_name=match_by_column_name,
                sync_tags=sync_tags,
            )

        return self._merge(copy_callable, primary_keys, replication_keys, qualify)

    def qualify(
        self,
        cursor: SnowflakeCursor,
        primary_keys: list[str],
        replication_keys: list[str] | None,
    ) -> None:
        qualify_partition = ",".join(f'"{c.upper()}"' for c in primary_keys)
        qualify_order = ",".join(
            f'"{c.upper()}" desc' for c in (replication_keys or primary_keys)
        )
        logging.debug(
            f"Adding QUALIFY to table {self.fqn} on PARTITION {qualify_partition} ORDERED BY {qualify_order}"
        )
        return cursor.execute(
            f"""
        create or replace table {self.fqn} as (
            select * from {self.fqn}
            qualify row_number() over (partition by {qualify_partition} order by {qualify_order}) = 1
            )
        """
        )

    def _merge_statement(
        self,
        temp_table: "Table",
        columns: list[Column],
        old_columns: dict[str, str],
        primary_keys: list[str],
    ) -> str:
        pkes = " and ".join(
            f'dest."{c.upper()}" = tmp."{c.upper()}"' for c in primary_keys
        )
        matched = _matched(columns, old_columns)
        column_names = ",".join(f'"{c.name}"' for c in columns)
        inserts = _inserts(columns, old_columns)

        logging.info(
            f"Running merge statement on table: {self.fqn} using {temp_table.schema_}.{temp_table.name}"
        )
        logging.debug(f"Primary keys: {pkes}")
        return f"""
            merge into {self.fqn} as dest 
            using {temp_table.schema_}.{temp_table.name} tmp
            ON {pkes}
            when matched then update set {matched}
            when not matched then insert ({column_names}) VALUES ({inserts})
        """

    def drop(self, cursor: SnowflakeCursor | None = None) -> None:
        if cursor is None:
            cursor = connect().cursor()
        logging.debug(f"Dropping table:{self.fqn}")
        cursor.execute(f"drop table {self.fqn}")

    def single_column_update(
        self, cursor: SnowflakeCursor, target_column: Column, new_column: Column
    ) -> None:
        """Updates the value of one column with the value of another column in the same table."""
        logging.debug(
            f"Updating the value of {target_column.name} with {new_column.name} in the table {self.name}"
        )
        cursor.execute(
            f"UPDATE {self.fqn} SET {target_column.name} = {new_column.name};"
        )

    def _current_tags(
        self, level: TagLevel, cursor: SnowflakeCursor
    ) -> list[tuple[str, str, str]]:
        cursor.execute(
            f"""select lower(column_name) as column_name, lower(tag_name) as tag_name, tag_value
                from table(information_schema.tag_references_all_columns('{self.fqn}', 'table'))
                where lower(level) = '{level.value}'
                """
        )
        return cursor.fetchall()

    def current_column_tags(self, cursor: SnowflakeCursor) -> dict[str, dict[str, str]]:
        tags = defaultdict(dict)

        for column_name, tag_name, tag_value in self._current_tags(
            TagLevel.COLUMN, cursor
        ):
            tags[column_name][tag_name] = tag_value
        return tags

    def current_table_tags(self, cursor: SnowflakeCursor) -> dict[str, str]:
        return {
            tag_name.casefold(): tag_value
            for _, tag_name, tag_value in self._current_tags(TagLevel.TABLE, cursor)
        }

    def sync_tags_table(self, cursor: SnowflakeCursor) -> None:
        tags = self.current_table_tags(cursor=cursor)
        desired_tags = {k.casefold(): v for k, v in self.table_structure.tags.items()}
        for tag_name in desired_tags:
            if tag_name not in tags:
                self._set_table_tag(cursor, desired_tags, tag_name)
        for tag_name in tags:
            if tag_name not in desired_tags:
                self._unset_table_tag(cursor, tag_name)

    def _unset_table_tag(self, cursor, tag_name):
        cursor.execute(
            f"ALTER TABLE {self.fqn} UNSET TAG {governance_settings.fqn(tag_name)}"
        )

    def _set_table_tag(self, cursor, desired_tags, tag_name):
        cursor.execute(
            f"ALTER TABLE {self.fqn} SET TAG {governance_settings.fqn(tag_name)} = '{desired_tags[tag_name]}'"
        )

    def sync_tags(self, cursor: SnowflakeCursor) -> None:
        self.sync_tags_table(cursor)
        self.sync_tags_columns(cursor)

    def sync_tags_columns(self, cursor: SnowflakeCursor) -> None:
        tags = self.current_column_tags(cursor)
        existing_tags = {
            f"{column}.{tag_name}.{tags[column][tag_name]}".casefold(): (
                column,
                tag_name,
                tags[column][tag_name],
            )
            for column in tags
            for tag_name in tags[column]
        }

        desired_tags = {
            f"{column}.{tag_name}.{tag_value}".casefold(): (column, tag_name, tag_value)
            for column in self.table_structure.columns
            for tag_name, tag_value in self.table_structure.columns[
                column.casefold()
            ].tags.items()
        }

        for tag in existing_tags:
            if tag not in desired_tags:
                column, tag_name, _value = existing_tags[tag]
                self._unset_column_tag(cursor, column, tag_name)

        for tag in desired_tags:
            if tag not in existing_tags:
                self._set_column_tag(cursor, *desired_tags[tag])

    def _set_column_tag(
        self, cursor: SnowflakeCursor, column: str, tag_name: str, tag_value: str
    ) -> None:
        cursor.execute(
            f"""ALTER TABLE {self.fqn} MODIFY COLUMN "{column.upper()}" SET TAG {governance_settings.fqn(tag_name)} = '{tag_value}'"""
        )

    def _unset_column_tag(self, cursor: SnowflakeCursor, column: str, tag: str) -> None:
        cursor.execute(
            f'ALTER TABLE {self.fqn} MODIFY COLUMN "{column.upper()}" UNSET TAG {governance_settings.fqn(tag)}'
        )

    def copy_custom(
        self,
        column_definitions: dict[str, str],
        path: str,
        file_format: InlineFileFormat | FileFormat,
        storage_integration: str | None = None,
        full_refresh: bool = False,
        sync_tags: bool = False,
    ) -> None:
        column_names = ", ".join(column_definitions.keys())
        definitions = ", ".join(column_definitions.values())

        query = f"""
                COPY INTO {self.fqn} ({column_names})
                FROM 
                (select {definitions} from @{self.temporary_stage}/)
                FILE_FORMAT = ( FORMAT_NAME ='{{file_format}}')
                """
        return self._copy(
            query, path, file_format, storage_integration, full_refresh, sync_tags
        )

    def merge_custom(
        self,
        column_definitions: dict[str, str],
        path: str,
        file_format: InlineFileFormat | FileFormat,
        primary_keys: list[str] = ["id"],
        replication_keys: list[str] | None = None,
        storage_integration: str | None = None,
        qualify: bool = False,
    ) -> None:
        def copy_callable(table: Table, sync_tags: bool) -> None:
            return table.copy_custom(
                column_definitions,
                path=path,
                storage_integration=storage_integration,
                file_format=file_format,
                full_refresh=True,
                sync_tags=sync_tags,
            )

        return self._merge(copy_callable, primary_keys, replication_keys, qualify)

    def setup_connection(
        self, path: str, storage_integration: str, cursor: SnowflakeCursor
    ) -> callable:
        """Setup the connection including custom role, database, schema, and temporary stage"""
        _execute_statement = partial(execute_statement, cursor)
        if self.role is not None:
            logging.debug(f"Using role: {self.role}")
            _execute_statement(f"USE ROLE {self.role}")
        if self.database is not None:
            logging.debug(f"Using database: {self.database}")
            _execute_statement(f"USE DATABASE {self.database}")

        _execute_statement(self.get_create_schema_statement())
        logging.debug(f"Using schema: {self.schema_}")
        _execute_statement(f"USE SCHEMA {self.schema_}")
        _execute_statement(
            self.get_create_temporary_external_stage(
                path=path, storage_integration=storage_integration
            )
        )

        return _execute_statement
