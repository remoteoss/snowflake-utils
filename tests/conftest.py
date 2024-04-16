def pytest_configure(config):
    config.addinivalue_line(
        "markers", "snowflake_vcr: Mark the test as using Snowflake VCR."
    )
