import io
import gzip

import pyarrow as pa
from pyarrow.json import ReadOptions, ParseOptions, read_json
import pyarrow.parquet as pq


def stream_json(fn, parquet_fn, schema=None, chunk_size=10000000):
    if isinstance(fn, str):
        fn = [fn]

    if schema is None:
        schema = read_json(fn[0]).schema

    writer = pq.ParquetWriter(parquet_fn, schema)

    for _f in fn:
        check_gz = _f.endswith('.gz')

        if check_gz:
            f = gzip.open(_f, 'r')
        else:
            f = open(_f, 'r')

        while True:
            chunk = f.readlines(chunk_size)
            if not chunk:
                break

            tbl = read_json(io.BytesIO(''.join(chunk).encode()))
            assert tbl.schema == schema # make sure the read table schema is the same as the parsed schema
            writer.write_table(tbl)

        f.close()
    
    writer.close()
