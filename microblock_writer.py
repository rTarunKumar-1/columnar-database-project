import pyarrow as pa
import pyarrow.parquet as pq

class MicroBlockWriter:
    def __init__(self, block_size=16384, compression="snappy"):
        self.block_size = block_size
        self.compression = compression

    def write(self, df, out_path):
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            out_path,
            row_group_size=self.block_size,
            compression=self.compression
        )
        return out_path
