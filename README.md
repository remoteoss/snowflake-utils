# Snowflake Utils

This repo is used to store packages helpful in LOADING data into Snowflake.

At the moment, both COPY and MERGE are implemented.

## Adding this package to your project

Add the package with the following command:

```bash
poetry add git+https://github.com/remoteoss/snowflake-utils.git
```

## Usage

In order to use this package, you have to define 3 required env vars:

- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD

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

## Authentication methods

You can use multiple authentication methods by either passing `authenticator` to the SnowflakeSettings object or by setting the env variable `SNOWFLAKE_AUTHENTICATOR`.
Available authenticators include:

- `snowflake`: default, username + password
- `username_password_mfa` which also caches the MFA token
- `externalbrowser` to login through the browser
- suppling an Okta domain -> currently untested
