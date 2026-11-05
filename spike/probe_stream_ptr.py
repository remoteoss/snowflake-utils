"""Probe: can Python fabricate the Arrow stream pointer the core normally returns?

If it could, replay could rebuild result rows from recorded bytes.  Run with
connector 5.2.0 + pyarrow; it is expected to crash (SIGBUS, exit 138).
"""

import ctypes

import pyarrow as pa
import snowflake.connector  # noqa: F401
from snowflake.connector._internal.arrow_context import ArrowConverterContext
from snowflake.connector._internal.arrow_stream_utils import (
    CythonArrowStreamTableIterator,
)

schema = pa.schema([pa.field("1", pa.int64())])
table = pa.table({"1": pa.array([1], type=pa.int64())})
reader = pa.RecordBatchReader.from_batches(schema, table.to_batches())
out = ctypes.c_void_p()
reader._export_to_c(ctypes.addressof(out))
print("exported", hex(out.value or 0), flush=True)

iterator = CythonArrowStreamTableIterator(out.value, ArrowConverterContext())
print("iterator", list(iterator), flush=True)
