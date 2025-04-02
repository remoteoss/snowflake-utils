# Snowflake Utils

This repo is used to store packages helpful in LOADING data into Snowflake.

At the moment, both COPY and MERGE are implemented.

## Adding this package to your project

Add the package with the following command:

```bash
poetry add git+https://github.com/remoteoss/snowflake-utils.git
```

## Usage

### Connection settings

In order to use this package, you can use environment variables to pass the configuraiton to be used by Snowflake.
These environment variables will be automatically be picked up by the SnowflakeSettings class.

| Variable | Description |
----------- | --------------
| SNOWFLAKE_ACCOUNT | The account identifier |
| SNOWFLAKE_USER | The user that should log in |
| SNOWFLAKE_PASSWORD | The password, if using password authentication method. Will be ignored if authenticator is set to `externalbrowser` |
| SNOWFLAKE_DB | The database to be set as context for the connection |
| SNOWFLAKE_ROLE | The role to be used as primary role in the connection. Can be left empty to use the default one for your user. Remember that since early 2025 all secondary roles are also enabled by default |
| SNOWFLAKE_WAREHOUSE | The warehouse to be used during the connection |
| SNOWFLAKE_AUTHENTICATOR | The authenticator to be used, can be `snowflake`, `externalbrowser` or `username_password_mfa`. Will be ignored if private key file is specified and exists. |
| SNOWFLAKE_PRIVATE_KEY_FILE | The file that contains the private key to be used for authentication. This is *the path* to the file, not the content. |
| SNOWFLAKE_PRIVATE_KEY_PASSWORD | The optional password for the above file. |

When manually initialising the SnowflakeSettings class you can override any of these attributes (and the `_schema` attribute to set a schema in your context), depending on your needs.

### Governance settings

The library also implemented some governance QOL methods, for example to include tags on tables/columns to be used for masking policies.
The environment variables `GOVERNANCE_{DATABASE|SCHEMA}` will be used to determine the default location of PII tags, so that they can be specified using only their name and not the fully qualified one. If they are specified using the fqn, that will be parsed correctly.

### The Table object

The Table object has the following attributes: 

- name: the name of the object
- schema_: the schema in which the table is located
- database: the database in which the schema is located. Can be used to override the database specified in the settings
- role: the role to be used to perform operations on the table. If set, it takes precedence over the one set in the connection
- include_metadata: whether any [metadata](https://docs.snowflake.com/en/user-guide/querying-metadata#metadata-columns) should be included when COPYing files to the table. The corresponding columns need to exist.
- table_structure: optional specification of the table structure, that will otherwise will be auto-inferred from the files 
Available (public) methods:

- *fqn*: returns the `database.schema.name`
- *temporary_stage*: returns the name for a temporary stage based on the table name and schema
- *temporary_file_format*: returns the name for a temporary file format based on table name and schema
- *get_create_temporary_file_format_statement*: given the string definition of a file format, creates the corresponding temporary file format
- *get_create_schema_statement*: generate the query to create the schema for the table (if not existing)
- *get_create_table_statement*: generate the query to create the table, using the structure provided or auto-inferring (which can yield some incorret data types). Currently the auto-infer does not support metadata columns. This will use both 
- *get_create_temporary_external_stage*: generate the query for a temporary stage for the table, based on the storage integration provided
- *bulk_insert*: given a get of records, will insert them in a table, optionally fully refreshing it. This is a plain insert and not a COPY, don't use it for large data.
- *copy_into*: takes, as input: 
  - a path (on S3)
  - a file format (either inline or already existing)
  - a match by column name parameter, defaulting to insensitive match between the file and the table
  - whether the table should be fully refreshed
  - a list of target columns, if not all the columns are present in the file to be loaded (or not all need to be writte)
  - whether to sync tags (provided in the table structure) to the table/columns
  - whether to perform a qualify on the table after loading the data. If this is used, a list of primary keys (and optionally of replication keys) should also be provided. If only the primary keys are supplied, those will also be used to determine which records are kept (non deterministic).
- *create_table*: runs the create table statement, with optional full refresh to recreate an existing table. 
- *setup_file_format*: given a file format object, creates the corresponding resource in Snowflake
- *get_columns*: returns the Columns of the table
- *add_column*: adds a new column to the table
- *exists*: boolean check if the table already exists
- *merge*: similar to copy and accepts the same options (except full refresh), but the data is first copied to a temporary table and then merged on primary keys. If the destination table does not exists, performs a copy. Provided table/column tags are always applied.
- *drop*: drops the table
- *single_column_update*: shortand for running an UPDATE statement to update the values of one column with those of another
- *current_column_tags*: extracts the tags currently applied to the columns
- *current_table_tags*: extracts the tags currentlya applied on the table
- *sync_tags*: syncs the tags specified in table structure to the table. There are also a `_columns` and a `_table` version of this method to only sync one of the two kinds of tags, but the more generic should be preferred.
- *copy_custom*: same as `copy_into`, but allows custom transformation of the data that is being loaded. Useful if you need to extract nested json fields for optimizing queries, or if you need shallow transformations such as casting timestamps
- *merge_custom*: same as `copy_custom` but for `merge`.
- *setup_connection*: returns a cursor object with the context based on the table options 
  
You can pass a role, database and schema as an attribute of the Table class to override the corresponding env variables.

In order to use the copy method of the Table class, you need a s3 bucket with the desired files to load into Snowflake, a Storage Integration with access to that bucket and a role that has access to this Storage Integration.

Additionally, you can either use an existing file format or pass the options for an temporary file format to be created. See [this](https://docs.snowflake.com/en/sql-reference/sql/create-file-format#format-type-options-formattypeoptions) for all the available options.

You don't need to know the schema of the file, Snowflake can infer the schema, but you have the option to input the schema as an attribute of the Table class.
You can also use the merge method to include changes and optionally qualify the results to prevent duplicates.

Example:

```python
#Given a table TEST in the schema PUBLIC
test_table = Table(name="TEST", schema_="PUBLIC")
#JSON temporary file format with outer array
json_file_format = InlineFileFormat(definition="TYPE = JSON STRIP_OUTER_ARRAY = TRUE")
#Existing storage integration
storage_integration = "DATA_STAGING"
#S3 Path with the JSON file(s)
path = "s3://example-bucket/example/path"
#Run the copy cmd with full refresh
test_table.copy(
        path=path,
        file_format=json_file_format,
        storage_integration=storage_integration,
        full_refresh=True,
    )
```

## Table structure

When initialising the table object you can pass a table structure that contains a dictionary of name: column, where Column is an object that contains the column data type and eventual tags to be applied to the column.

